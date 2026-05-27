// Package reconcile contains per-resource Ensure* helpers that the
// ArkClusterController orchestrates. Each function is idempotent: calling
// it multiple times with the same input produces the same end state.
package reconcile

import (
	"context"
	"fmt"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/ark"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// PVCNameSaves returns the per-map saves PVC name. mapID is the raw ARK map slug
// (e.g. "TheIsland_WP"); it's run through ark.MapSlug for DNS-1123 compliance.
func PVCNameSaves(cluster, mapID string) string {
	return fmt.Sprintf("%s-%s-saves", cluster, ark.MapSlug(mapID))
}

// PVCNameServer returns the server PVC name for a given side ("a" or "b").
func PVCNameServer(cluster, mapID, side string) string {
	return fmt.Sprintf("%s-%s-server-%s", cluster, ark.MapSlug(mapID), side)
}

// PVCNameCluster returns the shared cluster-transfer PVC name.
func PVCNameCluster(cluster string) string {
	return fmt.Sprintf("%s-cluster", cluster)
}

// EnsureSavesPVC creates the per-map saves PVC if missing. RWO.
func EnsureSavesPVC(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster, mapID string) error {
	return ensurePVC(ctx, c, cluster, PVCNameSaves(cluster.Name, mapID),
		cluster.Spec.Storage.SavesPVCSize, cluster.Spec.Storage.StorageClass,
		corev1.ReadWriteOnce)
}

// EnsureClusterPVC creates the shared RWX cluster-transfer PVC if missing.
// Uses cluster.Spec.Storage.ClusterStorageClass (default "nfs-csi" via CRD default).
func EnsureClusterPVC(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster) error {
	sc := cluster.Spec.Storage.ClusterStorageClass
	if sc == "" {
		sc = "nfs-csi"
	}
	return ensurePVC(ctx, c, cluster, PVCNameCluster(cluster.Name),
		cluster.Spec.Storage.ClusterPVCSize, sc, corev1.ReadWriteMany)
}

// EnsureServerPVC creates the server-a / server-b PVC for one map if missing. RWO.
func EnsureServerPVC(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster, mapID, side string) error {
	return ensurePVC(ctx, c, cluster, PVCNameServer(cluster.Name, mapID, side),
		cluster.Spec.Storage.ServerPVCSize, cluster.Spec.Storage.StorageClass,
		corev1.ReadWriteOnce)
}

func ensurePVC(ctx context.Context, c client.Client, owner *arkv1.ArkCluster,
	name, sizeStr, storageClass string, mode corev1.PersistentVolumeAccessMode) error {
	size, err := resource.ParseQuantity(sizeStr)
	if err != nil {
		return fmt.Errorf("ensurePVC %s: invalid size %q: %w", name, sizeStr, err)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: owner.Namespace},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, c, pvc, func() error {
		// PVC spec is immutable once created (except resources.requests for
		// expansion); set fields only on create.
		if pvc.CreationTimestamp.IsZero() {
			pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{mode}
			pvc.Spec.Resources = corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			}
			if storageClass != "" {
				pvc.Spec.StorageClassName = &storageClass
			}
		}
		if pvc.Labels == nil {
			pvc.Labels = map[string]string{}
		}
		pvc.Labels["ark.watteel.com/cluster"] = owner.Name
		return controllerutil.SetControllerReference(owner, pvc, c.Scheme())
	})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("ensurePVC %s: %w", name, err)
	}
	return nil
}
