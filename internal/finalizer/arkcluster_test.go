package finalizer

import (
	"context"
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newClient(t *testing.T, init ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = arkv1.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(init...).WithStatusSubresource(&arkv1.ArkCluster{}).Build()
}

func TestEnsureAdds(t *testing.T) {
	cluster := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"}}
	c := newClient(t, cluster)
	added, err := Ensure(context.Background(), c, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !added {
		t.Error("expected added=true")
	}
	if !containsString(cluster.Finalizers, Name) {
		t.Error("finalizer not on the in-memory object")
	}
}

func TestEnsureIdempotent(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n", Finalizers: []string{Name}},
	}
	c := newClient(t, cluster)
	added, err := Ensure(context.Background(), c, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if added {
		t.Error("expected added=false when finalizer already present")
	}
}

func TestRunFinalizeRemovesFinalizer(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n", Finalizers: []string{Name}},
	}
	c := newClient(t, cluster)
	done, err := RunFinalize(context.Background(), c, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Error("expected done=true")
	}
	if containsString(cluster.Finalizers, Name) {
		t.Errorf("finalizer still present: %+v", cluster.Finalizers)
	}
}

func TestRunFinalizeNoFinalizerIsNoOp(t *testing.T) {
	cluster := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"}}
	c := newClient(t, cluster)
	done, err := RunFinalize(context.Background(), c, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Error("expected done=true even with no finalizer present")
	}
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
