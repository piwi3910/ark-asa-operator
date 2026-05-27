//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"os/exec"
	"testing"
	"time"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// TestDrainDeadlinePersistsAcrossOperatorRestart is the AngellusMortis regression test.
// Triggers an update on a 1-map cluster, waits for DrainingActive, kills the operator
// pod, and verifies the drainDeadline is unchanged on restart.
//
// Requires: KUBECONFIG pointing at a cluster with the operator installed
// (e.g. `make e2e` from CI). Skipped unless E2E=1.
func TestDrainDeadlinePersistsAcrossOperatorRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	cfg, err := config.GetConfig()
	if err != nil {
		t.Skip("no kubeconfig:", err)
	}
	_ = ctrl.SetupSignalHandler()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = arkv1.AddToScheme(scheme)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	ac := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "restart-drain", Namespace: "ark-operator"},
		Spec: arkv1.ArkClusterSpec{
			ClusterID: "rd",
			Image:     "fake-ark-server:dev",
			Maps:      []arkv1.MapSpec{{ID: "TheIsland_WP"}},
			Service:   arkv1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, GamePortStart: 7777, RconPortStart: 27020, ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyCluster},
			Storage:   arkv1.StorageSpec{ServerPVCSize: "100Mi", SavesPVCSize: "100Mi", ClusterPVCSize: "100Mi", ClusterStorageClass: "nfs-csi"},
			UpdateStrategy: arkv1.UpdateStrategy{
				Type:             arkv1.UpdateStrategyBlueGreen,
				GracefulShutdown: metav1.Duration{Duration: 5 * time.Minute},
			},
		},
	}
	if err := c.Create(ctx, ac); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), ac)
	})

	// Wait for Running.
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		got := &arkv1.ArkCluster{}
		_ = c.Get(ctx, types.NamespacedName{Name: "restart-drain", Namespace: "ark-operator"}, got)
		if got.Status.Phase == arkv1.ClusterPhaseRunning {
			break
		}
		time.Sleep(5 * time.Second)
	}

	// Trigger update.
	got := &arkv1.ArkCluster{}
	if err := c.Get(ctx, types.NamespacedName{Name: "restart-drain", Namespace: "ark-operator"}, got); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(got.DeepCopy())
	got.Spec.Image = "fake-ark-server:dev-v2"
	if err := c.Patch(ctx, got, patch); err != nil {
		t.Fatal(err)
	}

	// Wait for DrainingActive with non-nil drainDeadline.
	var firstDeadline metav1.Time
	deadline = time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		got := &arkv1.ArkCluster{}
		_ = c.Get(ctx, types.NamespacedName{Name: "restart-drain", Namespace: "ark-operator"}, got)
		if len(got.Status.Maps) > 0 && got.Status.Maps[0].Phase == arkv1.MapPhaseDrainingActive && got.Status.Maps[0].DrainDeadline != nil {
			firstDeadline = *got.Status.Maps[0].DrainDeadline
			break
		}
		time.Sleep(2 * time.Second)
	}
	if firstDeadline.IsZero() {
		t.Fatal("never observed DrainingActive with non-nil drainDeadline")
	}

	// Kill the operator pod.
	if out, err := exec.Command("kubectl", "-n", "ark-operator", "rollout", "restart", "deployment/op-ark-asa-operator").CombinedOutput(); err != nil {
		t.Fatalf("rollout restart: %v %s", err, out)
	}
	time.Sleep(25 * time.Second) // wait for new operator pod

	// drainDeadline must be unchanged (this is the AngellusMortis bug we fixed).
	got2 := &arkv1.ArkCluster{}
	if err := c.Get(ctx, types.NamespacedName{Name: "restart-drain", Namespace: "ark-operator"}, got2); err != nil {
		t.Fatal(err)
	}
	if got2.Status.Maps[0].DrainDeadline == nil {
		t.Fatal("drainDeadline got cleared by operator restart")
	}
	if !got2.Status.Maps[0].DrainDeadline.Time.Equal(firstDeadline.Time) {
		t.Errorf("drainDeadline changed across operator restart: was %v, now %v — AngellusMortis regression",
			firstDeadline, got2.Status.Maps[0].DrainDeadline)
	}
}
