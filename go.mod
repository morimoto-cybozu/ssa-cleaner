module github.com/morimoto-cybozu/ssa-cleaner

replace google.golang.org/grpc => google.golang.org/grpc v1.26.0

go 1.15

require (
	github.com/coreos/etcd v3.3.25+incompatible
	github.com/cybozu-go/cke v1.19.4
	github.com/cybozu-go/etcdutil v1.3.5
	github.com/cybozu-go/log v1.6.0
	github.com/cybozu-go/well v1.10.0
	github.com/spf13/cobra v1.1.3
	k8s.io/apimachinery v0.19.7
	k8s.io/client-go v0.19.7
	sigs.k8s.io/yaml v1.2.0
)
