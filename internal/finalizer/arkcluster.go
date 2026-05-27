// Package finalizer holds the ArkCluster graceful-shutdown finalizer.
//
// On cluster delete, the controller invokes RunFinalize which:
//  1. Issues RCON SaveWorld + DoExit on every running pod (best-effort, bounded timeout).
//  2. Removes the finalizer so GC can cascade through owner refs.
package finalizer

import (
	"context"
	"time"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/rcon"
	"github.com/piwi3910/ark-asa-operator/internal/reconcile"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Name is the finalizer string stamped on ArkCluster resources.
const Name = "ark.watteel.com/graceful-shutdown"

// drainTimeoutPerMap caps each per-map RCON drain call. The finalizer is
// best-effort; we'd rather move on than stall deletion forever.
const drainTimeoutPerMap = 30 * time.Second

// Ensure adds the finalizer to the cluster if not already present.
func Ensure(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster) (bool, error) {
	if controllerutil.ContainsFinalizer(cluster, Name) {
		return false, nil
	}
	controllerutil.AddFinalizer(cluster, Name)
	return true, c.Update(ctx, cluster)
}

// RunFinalize issues RCON SaveAndExit on each map's running pod, then removes
// the finalizer so GC can cascade. Returns (done, error).
func RunFinalize(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster) (bool, error) {
	if !controllerutil.ContainsFinalizer(cluster, Name) {
		return true, nil
	}
	pw, err := readAdminPassword(ctx, c, cluster)
	if err == nil {
		for _, m := range cluster.Status.Maps {
			if m.RconAddress == "" {
				continue
			}
			drainCtx, cancel := context.WithTimeout(ctx, drainTimeoutPerMap)
			_ = rcon.SaveAndExit(drainCtx, m.RconAddress, pw)
			cancel()
		}
	}
	controllerutil.RemoveFinalizer(cluster, Name)
	return true, c.Update(ctx, cluster)
}

func readAdminPassword(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster) (string, error) {
	sel := cluster.Spec.GlobalSettings.AdminPassword
	name := reconcile.SecretsName(cluster.Name)
	key := "adminPassword"
	if sel != nil {
		name, key = sel.Name, sel.Key
	}
	sec := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: name}, sec); err != nil {
		return "", err
	}
	return string(sec.Data[key]), nil
}
