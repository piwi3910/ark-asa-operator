package reconcile

import (
	"context"
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestPVCNamesUseMapSlug(t *testing.T) {
	if got := PVCNameSaves("piwis-place", "TheIsland_WP"); got != "piwis-place-island-saves" {
		t.Errorf("PVCNameSaves = %q", got)
	}
	if got := PVCNameServer("piwis-place", "TheIsland_WP", "a"); got != "piwis-place-island-server-a" {
		t.Errorf("PVCNameServer = %q", got)
	}
	if got := PVCNameCluster("piwis-place"); got != "piwis-place-cluster" {
		t.Errorf("PVCNameCluster = %q", got)
	}
}

func TestEnsureSavesPVCCreatesWhenMissing(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: arkv1.ArkClusterSpec{
			Storage: arkv1.StorageSpec{SavesPVCSize: "10Gi"},
		},
	}
	c := newFake(t).Build()
	if err := EnsureSavesPVC(context.Background(), c, cluster, "TheIsland_WP"); err != nil {
		t.Fatal(err)
	}
	// Re-call must be idempotent (no error)
	if err := EnsureSavesPVC(context.Background(), c, cluster, "TheIsland_WP"); err != nil {
		t.Errorf("second call must be no-op, got %v", err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "c-island-saves", Namespace: "ns"}, pvc); err != nil {
		t.Fatal(err)
	}
	if pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Errorf("expected RWO, got %s", pvc.Spec.AccessModes[0])
	}
}

func TestEnsureClusterPVCUsesRWXAndCustomClass(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: arkv1.ArkClusterSpec{
			Storage: arkv1.StorageSpec{ClusterPVCSize: "5Gi", ClusterStorageClass: "nfs-csi"},
		},
	}
	c := newFake(t).Build()
	if err := EnsureClusterPVC(context.Background(), c, cluster); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "c-cluster", Namespace: "ns"}, pvc); err != nil {
		t.Fatal(err)
	}
	if pvc.Spec.AccessModes[0] != corev1.ReadWriteMany {
		t.Errorf("expected RWX, got %s", pvc.Spec.AccessModes[0])
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "nfs-csi" {
		t.Errorf("storageClassName not nfs-csi: %v", pvc.Spec.StorageClassName)
	}
}

func TestEnsureServerPVCBothSides(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec:       arkv1.ArkClusterSpec{Storage: arkv1.StorageSpec{ServerPVCSize: "50Gi"}},
	}
	c := newFake(t).Build()
	if err := EnsureServerPVC(context.Background(), c, cluster, "TheIsland_WP", "a"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureServerPVC(context.Background(), c, cluster, "TheIsland_WP", "b"); err != nil {
		t.Fatal(err)
	}
	for _, side := range []string{"a", "b"} {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: "c-island-server-" + side, Namespace: "ns"}, pvc); err != nil {
			t.Errorf("server-%s missing: %v", side, err)
		}
	}
}
