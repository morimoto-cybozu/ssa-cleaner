package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/coreos/etcd/clientv3"
	"github.com/cybozu-go/cke"
	"github.com/cybozu-go/cke/static"
	"github.com/cybozu-go/etcdutil"
	"github.com/cybozu-go/log"
	"github.com/cybozu-go/well"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	serializerYaml "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"sigs.k8s.io/yaml"
)

const (
	managerCKE              = "cke"
	managerBeforeFirstApply = "before-first-apply"
	managerCleaner          = "ssa-cleaner"
)

var (
	cfgFile        string
	kubeconfigFile string

	etcdClient      *clientv3.Client
	storage         cke.Storage
	dynClient       dynamic.Interface
	mapper          *restmapper.DeferredDiscoveryRESTMapper
	decUnstructured = serializerYaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
)

type object struct {
	Definition *unstructured.Unstructured
	Current    *unstructured.Unstructured
	Resource   dynamic.ResourceInterface
}

func loadConfig(p string) (*etcdutil.Config, error) {
	b, err := ioutil.ReadFile(p)
	if err != nil {
		return nil, err
	}

	cfg := cke.NewEtcdConfig()
	err = yaml.Unmarshal(b, cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

var rootCmd = &cobra.Command{
	Use:   "ssa-cleaner",
	Short: "clean 'before-first-apply' managers from CKE-managed k8s resources",
	Long:  `Clean 'before-first-apply' managers from CKE-managed k8s resources.`,

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// without this, each subcommand's RunE would display usage text.
		cmd.SilenceUsage = true

		err := well.LogConfig{}.Apply()
		if err != nil {
			return err
		}

		cfg, err := loadConfig(cfgFile)
		if err != nil {
			return err
		}

		etcd, err := etcdutil.NewClient(cfg)
		if err != nil {
			return err
		}
		etcdClient = etcd

		storage = cke.Storage{Client: etcd}

		kubeconfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigFile)
		if err != nil {
			return err
		}

		dyn, err := dynamic.NewForConfig(kubeconfig)
		if err != nil {
			return err
		}
		dynClient = dyn

		dc, err := discovery.NewDiscoveryClientForConfig(kubeconfig)
		if err != nil {
			return err
		}
		mapper = restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))

		return nil
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		if etcdClient != nil {
			etcdClient.Close()
		}
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		well.Go(func(ctx context.Context) error {
			resources, err := storage.GetAllResources(ctx)
			if err != nil {
				return err
			}
			resources = append(resources, static.Resources...)

			for _, res := range resources {
				err = forceFirstSSA(ctx, res)
				if err != nil {
					return err
				}

				err = removeBeforeFirstApply(ctx, res)
				if err != nil {
					return err
				}

				err = manageAllFields(ctx, res)
				if err != nil {
					return err
				}

				err = releaseAllFields(ctx, res)
				if err != nil {
					return err
				}
			}

			return nil
		})
		well.Stop()
		return well.Wait()
	},
}

func getObject(ctx context.Context, resource cke.ResourceDefinition) (*object, error) {
	// cf. cybozu-go/cke/resource.go, cybozu-go/cke/op/status.go
	obj := &object{}
	obj.Definition = &unstructured.Unstructured{}
	_, gvk, err := decUnstructured.Decode(resource.Definition, nil, obj.Definition)
	if err != nil {
		return nil, fmt.Errorf("failed to decode data into *Unstructured: %w", err)
	}

	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to find rest mapping for %s: %w", gvk.String(), err)
	}

	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := obj.Definition.GetNamespace()
		if ns == "" {
			return nil, fmt.Errorf("no namespace for %s: name=%s", gvk.String(), obj.Definition.GetName())
		}
		obj.Resource = dynClient.Resource(mapping.Resource).Namespace(ns)
	} else {
		obj.Resource = dynClient.Resource(mapping.Resource)
	}

	current, err := obj.Resource.Get(ctx, obj.Definition.GetName(), metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get resource: %w", err)
	}
	obj.Current = current

	return obj, nil
}

func (obj object) Apply(ctx context.Context, new *unstructured.Unstructured, managerName string, force bool) error {
	buf := &bytes.Buffer{}
	if err := unstructured.UnstructuredJSONScheme.Encode(new, buf); err != nil {
		return fmt.Errorf("failed to encode: %w", err)
	}

	_, err := obj.Resource.Patch(ctx, new.GetName(), types.ApplyPatchType, buf.Bytes(), metav1.PatchOptions{
		FieldManager: managerName,
		Force:        &force,
	})
	if err != nil {
		return fmt.Errorf("failed to patch: %w", err)
	}

	return nil
}

func forceFirstSSA(ctx context.Context, res cke.ResourceDefinition) error {
	obj, err := getObject(ctx, res)
	if err != nil {
		return fmt.Errorf("failed to get object for %s: %w", res.Key, err)
	}

	for _, f := range obj.Current.GetManagedFields() {
		if f.Manager == managerCKE {
			// already managed by CKE
			return nil
		}
	}

	log.Info("force first SSA", map[string]interface{}{
		"key": res.Key,
	})
	// apply resource definition as CKE
	if err := obj.Apply(ctx, obj.Definition, managerCKE, true); err != nil {
		log.Error("failed to apply", map[string]interface{}{
			"key":       res.Key,
			log.FnError: err,
		})
		return err
	}

	return nil
}

func removeBeforeFirstApply(ctx context.Context, res cke.ResourceDefinition) error {
	obj, err := getObject(ctx, res)
	if err != nil {
		return fmt.Errorf("failed to get object for %s: %w", res.Key, err)
	}

	index := -1
	for i, f := range obj.Current.GetManagedFields() {
		if f.Manager == managerBeforeFirstApply && f.Operation == metav1.ManagedFieldsOperationUpdate {
			index = i
			break
		}
	}
	if index == -1 {
		return nil
	}

	log.Info("remove before-first-apply manager", map[string]interface{}{
		"key": res.Key,
	})
	patch := fmt.Sprintf(`[{"op": "remove", "path": "/metadata/managedFields/%d"}]`, index)
	if _, err := obj.Resource.Patch(ctx, obj.Current.GetName(), types.JSONPatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		log.Error("failed to patch", map[string]interface{}{
			"key":       res.Key,
			log.FnError: err,
		})
		return err
	}

	return nil
}

func manageAllFields(ctx context.Context, res cke.ResourceDefinition) error {
	obj, err := getObject(ctx, res)
	if err != nil {
		return fmt.Errorf("failed to get object for %s: %w", res.Key, err)
	}

	new := obj.Current.DeepCopy()
	unstructured.RemoveNestedField(new.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(new.Object, "status")

	log.Info("manage all fields", map[string]interface{}{
		"key": res.Key,
	})
	// apply current resource without change as Cleaner
	if err := obj.Apply(ctx, new, managerCleaner, false); err != nil {
		log.Error("failed to apply", map[string]interface{}{
			"key":       res.Key,
			log.FnError: err,
		})
		return err
	}

	return nil
}

func releaseAllFields(ctx context.Context, res cke.ResourceDefinition) error {
	obj, err := getObject(ctx, res)
	if err != nil {
		return fmt.Errorf("failed to get object for %s: %w", res.Key, err)
	}

	new := &unstructured.Unstructured{}
	new.SetAPIVersion(obj.Current.GetAPIVersion())
	new.SetKind(obj.Current.GetKind())
	new.SetNamespace(obj.Current.GetNamespace())
	new.SetName(obj.Current.GetName())

	log.Info("release all fields", map[string]interface{}{
		"key": res.Key,
	})
	// apply empty resource as Cleaner
	if err := obj.Apply(ctx, new, managerCleaner, false); err != nil {
		log.Error("failed to apply", map[string]interface{}{
			"key":       res.Key,
			log.FnError: err,
		})
		return err
	}

	return nil
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.ErrorExit(err)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "/etc/cke/config.yml", "config file")
	if home := homedir.HomeDir(); home != "" {
		rootCmd.PersistentFlags().StringVar(&kubeconfigFile, "kubeconfig", filepath.Join(home, ".kube", "config"), "absolute path to the kubeconfig file")
	} else {
		rootCmd.PersistentFlags().StringVar(&kubeconfigFile, "kubeconfig", "", "absolute path to the kubeconfig file")
	}
}
