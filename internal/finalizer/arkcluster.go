// Package finalizer holds the ArkCluster graceful-shutdown finalizer.
//
// Phase 1: just adds/removes the finalizer string. The intent is that on
// cluster delete the controller runs `RunFinalize` once, which removes the
// finalizer so GC can cascade through owner refs. Phase 2 will add explicit
// RCON SaveAndExit on every running pod before the finalizer is removed.
package finalizer

import (
	"context"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Name is the finalizer string stamped on ArkCluster resources.
const Name = "ark.watteel.com/graceful-shutdown"

// Ensure adds the finalizer to the cluster if not already present.
// Returns (added, error). When added=true, a Status().Update or Update on the
// cluster has already happened; the controller should requeue to pick up the
// new resourceVersion.
func Ensure(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster) (bool, error) {
	if controllerutil.ContainsFinalizer(cluster, Name) {
		return false, nil
	}
	controllerutil.AddFinalizer(cluster, Name)
	return true, c.Update(ctx, cluster)
}

// RunFinalize performs final cleanup on cluster delete and removes the
// finalizer. Returns (done, error). When done=true, the API server's garbage
// collector cascades through owner refs and reclaims the cluster's resources
// (modulo spec.storage.persistOnDelete, which Phase 1 doesn't implement yet).
//
// Phase 1: removal only. Container preStop hook + Pod restartPolicy give us
// reasonable graceful shutdown per pod; Phase 2 will add explicit RCON
// SaveAndExit on every running pod here before removal.
func RunFinalize(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster) (bool, error) {
	if !controllerutil.ContainsFinalizer(cluster, Name) {
		return true, nil
	}
	controllerutil.RemoveFinalizer(cluster, Name)
	return true, c.Update(ctx, cluster)
}
