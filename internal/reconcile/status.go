package reconcile

import (
	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SetMapCondition is the per-map equivalent of meta.SetStatusCondition.
// Adds the condition if not present, updates Status (and LastTransitionTime)
// when changed, refreshes Reason/Message otherwise. No-op if mapID has no
// status entry yet.
func SetMapCondition(cluster *arkv1.ArkCluster, mapID string, cond metav1.Condition) {
	for i := range cluster.Status.Maps {
		if cluster.Status.Maps[i].ID != mapID {
			continue
		}
		conds := cluster.Status.Maps[i].Conditions
		for j := range conds {
			if conds[j].Type == cond.Type {
				if conds[j].Status != cond.Status {
					cond.LastTransitionTime = metav1.Now()
					conds[j] = cond
				} else {
					// Same status — refresh reason/message only.
					conds[j].Reason = cond.Reason
					conds[j].Message = cond.Message
				}
				cluster.Status.Maps[i].Conditions = conds
				return
			}
		}
		cond.LastTransitionTime = metav1.Now()
		cluster.Status.Maps[i].Conditions = append(conds, cond)
		return
	}
}

// EnsureMapStatus returns a pointer to the map's status entry, creating an
// empty one if it doesn't exist yet. Safe to call multiple times.
func EnsureMapStatus(cluster *arkv1.ArkCluster, mapID string) *arkv1.MapStatus {
	for i := range cluster.Status.Maps {
		if cluster.Status.Maps[i].ID == mapID {
			return &cluster.Status.Maps[i]
		}
	}
	cluster.Status.Maps = append(cluster.Status.Maps, arkv1.MapStatus{ID: mapID})
	return &cluster.Status.Maps[len(cluster.Status.Maps)-1]
}

// AggregatePhase rolls per-map phases up into a cluster-level phase.
// Priority: any Failed → Degraded; any Updating-class → Updating; any
// Pending/Provisioning → Initializing; all Running → Running; otherwise Pending.
func AggregatePhase(maps []arkv1.MapStatus) arkv1.ClusterPhase {
	if len(maps) == 0 {
		return arkv1.ClusterPhasePending
	}
	anyFailed, anyUpdating, anyPending, allRunning := false, false, false, true
	for _, m := range maps {
		switch m.Phase {
		case arkv1.MapPhaseFailed:
			anyFailed = true
			allRunning = false
		case arkv1.MapPhaseInstallingActive, arkv1.MapPhaseInstallingInactive,
			arkv1.MapPhaseDrainingActive, arkv1.MapPhaseSwapping:
			anyUpdating = true
			allRunning = false
		case arkv1.MapPhasePending, arkv1.MapPhaseProvisioning:
			anyPending = true
			allRunning = false
		case arkv1.MapPhaseRunning:
			// no-op
		default:
			allRunning = false
		}
	}
	switch {
	case anyFailed:
		return arkv1.ClusterPhaseDegraded
	case anyUpdating:
		return arkv1.ClusterPhaseUpdating
	case anyPending:
		return arkv1.ClusterPhaseInitializing
	case allRunning:
		return arkv1.ClusterPhaseRunning
	default:
		return arkv1.ClusterPhasePending
	}
}

// SetClusterCondition mirrors SetMapCondition for cluster-level conditions.
func SetClusterCondition(cluster *arkv1.ArkCluster, cond metav1.Condition) {
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == cond.Type {
			if cluster.Status.Conditions[i].Status != cond.Status {
				cond.LastTransitionTime = metav1.Now()
				cluster.Status.Conditions[i] = cond
			} else {
				cluster.Status.Conditions[i].Reason = cond.Reason
				cluster.Status.Conditions[i].Message = cond.Message
			}
			return
		}
	}
	cond.LastTransitionTime = metav1.Now()
	cluster.Status.Conditions = append(cluster.Status.Conditions, cond)
}
