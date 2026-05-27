package reconcile

import (
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSetMapConditionAddsAndUpdates(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		Status: arkv1.ArkClusterStatus{Maps: []arkv1.MapStatus{{ID: "island"}}},
	}
	SetMapCondition(cluster, "island", metav1.Condition{
		Type: "PodReady", Status: metav1.ConditionTrue, Reason: "RCONReachable", Message: "ok",
	})
	c := cluster.Status.Maps[0].Conditions
	if len(c) != 1 || c[0].Type != "PodReady" || c[0].Status != metav1.ConditionTrue {
		t.Errorf("unexpected: %+v", c)
	}
	// Idempotent update (same status keeps a single entry, refreshes reason)
	SetMapCondition(cluster, "island", metav1.Condition{
		Type: "PodReady", Status: metav1.ConditionTrue, Reason: "RCONReachable", Message: "still ok",
	})
	if len(cluster.Status.Maps[0].Conditions) != 1 {
		t.Error("must not duplicate")
	}
	if cluster.Status.Maps[0].Conditions[0].Message != "still ok" {
		t.Error("message should refresh")
	}
	// Status change updates
	SetMapCondition(cluster, "island", metav1.Condition{
		Type: "PodReady", Status: metav1.ConditionFalse, Reason: "RCONTimeout", Message: "ouch",
	})
	if cluster.Status.Maps[0].Conditions[0].Status != metav1.ConditionFalse {
		t.Error("status should update")
	}
}

func TestSetMapConditionUnknownMapNoPanic(t *testing.T) {
	cluster := &arkv1.ArkCluster{}
	// Must not panic; just no-op when the map isn't in status yet.
	SetMapCondition(cluster, "missing", metav1.Condition{Type: "X", Status: metav1.ConditionTrue})
}

func TestEnsureMapStatusInsertsOnce(t *testing.T) {
	cluster := &arkv1.ArkCluster{}
	a := EnsureMapStatus(cluster, "island")
	a.Pod = "p1"
	b := EnsureMapStatus(cluster, "island")
	if b.Pod != "p1" {
		t.Error("EnsureMapStatus should return existing entry, not insert a new one")
	}
	if len(cluster.Status.Maps) != 1 {
		t.Errorf("expected 1 map status, got %d", len(cluster.Status.Maps))
	}
}

func TestAggregatePhase(t *testing.T) {
	tests := []struct {
		name string
		maps []arkv1.MapStatus
		want arkv1.ClusterPhase
	}{
		{"no maps", nil, arkv1.ClusterPhasePending},
		{"all running", []arkv1.MapStatus{{Phase: arkv1.MapPhaseRunning}, {Phase: arkv1.MapPhaseRunning}}, arkv1.ClusterPhaseRunning},
		{"one updating", []arkv1.MapStatus{{Phase: arkv1.MapPhaseRunning}, {Phase: arkv1.MapPhaseDrainingActive}}, arkv1.ClusterPhaseUpdating},
		{"one failed", []arkv1.MapStatus{{Phase: arkv1.MapPhaseRunning}, {Phase: arkv1.MapPhaseFailed}}, arkv1.ClusterPhaseDegraded},
		{"one provisioning", []arkv1.MapStatus{{Phase: arkv1.MapPhaseProvisioning}}, arkv1.ClusterPhaseInitializing},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := AggregatePhase(tc.maps); got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestSetClusterCondition(t *testing.T) {
	cluster := &arkv1.ArkCluster{}
	SetClusterCondition(cluster, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "OK"})
	if len(cluster.Status.Conditions) != 1 || cluster.Status.Conditions[0].Type != "Ready" {
		t.Errorf("expected one Ready condition; got %+v", cluster.Status.Conditions)
	}
	SetClusterCondition(cluster, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Broken"})
	if cluster.Status.Conditions[0].Status != metav1.ConditionFalse {
		t.Error("Ready should update to False")
	}
}
