package reconcile

import (
	"context"
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestInitJobNameStableAcrossGenerations(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n", Generation: 1},
		Spec:       arkv1.ArkClusterSpec{Image: "img:v1"},
	}
	first := InitJobName(cluster, "TheIsland_WP", "b")
	cluster.Generation = 99
	if InitJobName(cluster, "TheIsland_WP", "b") != first {
		t.Error("InitJobName must not depend on Generation")
	}
}

func TestInitJobNameChangesOnImageChange(t *testing.T) {
	c1 := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"}, Spec: arkv1.ArkClusterSpec{Image: "img:v1"}}
	c2 := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"}, Spec: arkv1.ArkClusterSpec{Image: "img:v2"}}
	if InitJobName(c1, "TheIsland_WP", "a") == InitJobName(c2, "TheIsland_WP", "a") {
		t.Error("InitJobName must change when image changes")
	}
}

func TestInitJobNameUsesMapSlug(t *testing.T) {
	c := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "piwis-place", Namespace: "n"}, Spec: arkv1.ArkClusterSpec{Image: "img"}}
	name := InitJobName(c, "TheIsland_WP", "b")
	// Should contain "piwis-place-island-init-b-"
	if name == "" || len(name) < len("piwis-place-island-init-b-") {
		t.Errorf("unexpected: %q", name)
	}
}

func TestEnsureInitJobCreatesOnInactive(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"},
		Spec:       arkv1.ArkClusterSpec{Image: "docker.io/sknnr/ark-ascended-server:latest"},
	}
	c := newFake(t).Build()
	created, err := EnsureInitJob(context.Background(), c, cluster, "TheIsland_WP", "b")
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Error("expected created=true")
	}
	job := &batchv1.Job{}
	name := InitJobName(cluster, "TheIsland_WP", "b")
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "n"}, job); err != nil {
		t.Fatal(err)
	}
	// Verify the inactive PVC is the one mounted
	vol := job.Spec.Template.Spec.Volumes[0]
	if vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != "c-island-server-b" {
		t.Errorf("wrong PVC mounted: %+v", vol)
	}
	// Second call must NOT create another Job (idempotent — same install identity → same name)
	created2, err := EnsureInitJob(context.Background(), c, cluster, "TheIsland_WP", "b")
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Error("expected second call to return created=false (idempotent)")
	}
}

func TestInitJobStatusReturnsNotFoundWhenMissing(t *testing.T) {
	cluster := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"}, Spec: arkv1.ArkClusterSpec{Image: "img"}}
	c := newFake(t).Build()
	got := InitJobStatus(context.Background(), c, cluster, "TheIsland_WP", "b")
	if got != "NotFound" {
		t.Errorf("got %q want NotFound", got)
	}
}

func TestInitJobStatusReturnsSucceededOnComplete(t *testing.T) {
	cluster := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"}, Spec: arkv1.ArkClusterSpec{Image: "img"}}
	c := newFake(t).Build()
	_, _ = EnsureInitJob(context.Background(), c, cluster, "TheIsland_WP", "b")
	// Patch the Job to Complete
	job := &batchv1.Job{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: InitJobName(cluster, "TheIsland_WP", "b"), Namespace: "n"}, job)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	_ = c.Status().Update(context.Background(), job)
	got := InitJobStatus(context.Background(), c, cluster, "TheIsland_WP", "b")
	if got != "Succeeded" {
		t.Errorf("got %q want Succeeded", got)
	}
}
