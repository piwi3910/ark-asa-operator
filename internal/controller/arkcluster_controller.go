// Package controller hosts the ArkCluster reconciler.
//
// Phase 1: single-map, no blue/green. Provision PVCs+Service+ConfigMaps+Secret,
// ensure server pod with current hash, surface status. Phase 2 will add the
// blue/green rollUpdate path.
package controller

import (
	"context"
	"fmt"
	"time"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/ark"
	"github.com/piwi3910/ark-asa-operator/internal/finalizer"
	"github.com/piwi3910/ark-asa-operator/internal/reconcile"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	requeueSteady = 5 * time.Minute
	requeueBusy   = 15 * time.Second
)

// ArkClusterReconciler reconciles an ArkCluster object.
type ArkClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=ark.watteel.com,resources=arkclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ark.watteel.com,resources=arkclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ark.watteel.com,resources=arkclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods;services;persistentvolumeclaims;configmaps;secrets;events,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *ArkClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("arkcluster", req.NamespacedName)

	cluster := &arkv1.ArkCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Finalize on delete
	if !cluster.DeletionTimestamp.IsZero() {
		done, err := finalizer.RunFinalize(ctx, r.Client, cluster)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return ctrl.Result{RequeueAfter: requeueBusy}, nil
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer present (Amendment F: this Update happens before any
	// world action, so deletion can always cascade).
	if added, err := finalizer.Ensure(ctx, r.Client, cluster); err != nil {
		return ctrl.Result{}, err
	} else if added {
		// Re-queue; the next Reconcile will read the updated resourceVersion.
		return ctrl.Result{Requeue: true}, nil
	}

	// 1. Cluster-level resources
	if err := reconcile.EnsureAdminPasswordSecret(ctx, r.Client, cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := reconcile.EnsureClusterPVC(ctx, r.Client, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Per-map fan-out (Phase 1: validated to exactly 1 by webhook)
	busy := false
	for i, mapSpec := range cluster.Spec.Maps {
		if err := r.reconcileMap(ctx, cluster, mapSpec, i, &busy); err != nil {
			logger.Error(err, "reconcileMap failed", "map", mapSpec.ID)
			return ctrl.Result{}, err
		}
	}

	// 3. Aggregate + persist status
	cluster.Status.TotalMaps = int32(len(cluster.Spec.Maps))
	cluster.Status.ReadyMaps = countReadyMaps(cluster)
	cluster.Status.Phase = reconcile.AggregatePhase(cluster.Status.Maps)
	cluster.Status.ObservedGeneration = cluster.Generation
	if err := r.Status().Update(ctx, cluster); err != nil && !apierrors.IsConflict(err) {
		logger.Error(err, "status update")
		return ctrl.Result{}, err
	}

	if busy {
		return ctrl.Result{RequeueAfter: requeueBusy}, nil
	}
	return ctrl.Result{RequeueAfter: requeueSteady}, nil
}

func (r *ArkClusterReconciler) reconcileMap(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, i int, busy *bool) error {
	if err := reconcile.EnsureSavesPVC(ctx, r.Client, cluster, mapSpec.ID); err != nil {
		return err
	}
	if err := reconcile.EnsureServerPVC(ctx, r.Client, cluster, mapSpec.ID, "a"); err != nil {
		return err
	}
	if err := reconcile.EnsureServerPVC(ctx, r.Client, cluster, mapSpec.ID, "b"); err != nil {
		return err
	}
	if err := reconcile.EnsureMapINIConfigMaps(ctx, r.Client, cluster, mapSpec.ID); err != nil {
		return err
	}
	if err := reconcile.EnsureService(ctx, r.Client, cluster, mapSpec.ID, i); err != nil {
		return err
	}

	mapStatus := reconcile.EnsureMapStatus(cluster, mapSpec.ID)
	if mapStatus.ActiveVolume == "" {
		mapStatus.ActiveVolume = "server-a"
	}
	activeVolume := mapStatus.ActiveVolume

	hash := r.computePodHash(ctx, cluster, mapSpec, i, activeVolume)

	// EnsurePod: create if missing, recreate (delete stale + create new) on hash mismatch.
	created, err := r.ensurePod(ctx, cluster, mapSpec, i, activeVolume, hash)
	if err != nil {
		return err
	}

	// Re-resolve mapStatus pointer after any potential slice mutations elsewhere.
	mapStatus = reconcile.EnsureMapStatus(cluster, mapSpec.ID)
	if created {
		*busy = true
		mapStatus.Phase = arkv1.MapPhaseProvisioning
	}

	mapStatus.Pod = reconcile.PodName(cluster.Name, mapSpec.ID, hash)
	mapStatus.SessionName = ark.SessionName(cluster.Spec.GlobalSettings.SessionNameFormat,
		cluster.Name, mapSpec.ID, friendlyName(mapSpec.ID))

	// Surface LB IP if the Service has one.
	svc := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: reconcile.ServiceName(cluster.Name, mapSpec.ID)}, svc); err == nil {
		if len(svc.Status.LoadBalancer.Ingress) > 0 {
			ip := svc.Status.LoadBalancer.Ingress[0].IP
			gp := ark.GamePort(cluster.Spec.Service.GamePortStart, i)
			rp := ark.RconPort(cluster.Spec.Service.RconPortStart, i)
			mapStatus.Address = fmt.Sprintf("%s:%d", ip, gp)
			mapStatus.RconAddress = fmt.Sprintf("%s:%d", ip, rp)
		}
	}

	// Promote to Running if pod ready.
	pod := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mapStatus.Pod}, pod); err == nil {
		if podReady(pod) {
			mapStatus.Phase = arkv1.MapPhaseRunning
			reconcile.SetMapCondition(cluster, mapSpec.ID, metav1.Condition{
				Type: "PodReady", Status: metav1.ConditionTrue, Reason: "RCONReachable", Message: "pod ready",
			})
		} else {
			*busy = true
			reconcile.SetMapCondition(cluster, mapSpec.ID, metav1.Condition{
				Type: "PodReady", Status: metav1.ConditionFalse, Reason: "PodNotReady", Message: "warming up",
			})
		}
	}
	return nil
}

// computePodHash includes resourceVersions of referenced ConfigMaps and Secrets
// (Amendment B). Without this, ConfigMap/Secret edits never propagate.
func (r *ArkClusterReconciler) computePodHash(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, i int, activeVolume string) string {
	mods := cluster.Spec.GlobalSettings.Mods
	if len(mapSpec.Mods) > 0 {
		mods = mapSpec.Mods
	}
	iniRev := r.readResourceVersion(ctx, cluster.Namespace, "ConfigMap", reconcile.GUSConfigMapName(cluster.Name, mapSpec.ID)) +
		"|" + r.readResourceVersion(ctx, cluster.Namespace, "ConfigMap", reconcile.GameConfigMapName(cluster.Name, mapSpec.ID))
	secretsRev := r.readResourceVersion(ctx, cluster.Namespace, "Secret", reconcile.SecretsName(cluster.Name))
	if cluster.Spec.GlobalSettings.ServerPassword != nil {
		secretsRev += "|" + r.readResourceVersion(ctx, cluster.Namespace, "Secret", cluster.Spec.GlobalSettings.ServerPassword.Name)
	}
	return ark.PodTemplateHash(ark.PodTemplateHashInput{
		Image:        cluster.Spec.Image,
		Mods:         mods,
		GamePort:     ark.GamePort(cluster.Spec.Service.GamePortStart, i),
		RconPort:     ark.RconPort(cluster.Spec.Service.RconPortStart, i),
		ActiveVolume: activeVolume,
		IniRev:       iniRev,
		SecretsRev:   secretsRev,
	})
}

func (r *ArkClusterReconciler) readResourceVersion(ctx context.Context, ns, kind, name string) string {
	var obj client.Object
	switch kind {
	case "ConfigMap":
		obj = &corev1.ConfigMap{}
	case "Secret":
		obj = &corev1.Secret{}
	default:
		return ""
	}
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, obj); err != nil {
		// Missing → empty revision string; the hash will change once it appears.
		return ""
	}
	return obj.GetResourceVersion()
}

// ensurePod: create the server pod if missing; delete-and-recreate if hash drifts.
// Returns (created, error) where created=true means the world changed and we
// should requeue. Phase 1 is naive: any drift triggers a hard delete + create.
// Phase 2 will replace this with the blue/green rollUpdate path.
func (r *ArkClusterReconciler) ensurePod(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, i int, activeVolume, hash string) (bool, error) {
	existing := &corev1.PodList{}
	if err := r.List(ctx, existing, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"ark.watteel.com/cluster": cluster.Name,
		"ark.watteel.com/map":     ark.MapSlug(mapSpec.ID),
	}); err != nil {
		return false, err
	}
	for j := range existing.Items {
		p := &existing.Items[j]
		if p.DeletionTimestamp != nil {
			continue
		}
		if p.Labels["ark.watteel.com/pod-template-hash"] == hash {
			return false, nil // already current
		}
		// Stale pod — delete; preStop hook handles graceful shutdown.
		grace := int64(60)
		if err := r.Delete(ctx, p, &client.DeleteOptions{GracePeriodSeconds: &grace}); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete stale pod %s: %w", p.Name, err)
		}
	}

	pod := reconcile.BuildServerPod(reconcile.PodInput{
		Cluster:      cluster,
		MapID:        mapSpec.ID,
		MapIndex:     i,
		FriendlyMap:  friendlyName(mapSpec.ID),
		ActiveVolume: activeVolume,
		Hash:         hash,
	})
	if err := controllerutil.SetControllerReference(cluster, pod, r.Scheme); err != nil {
		return false, err
	}
	if err := r.Create(ctx, pod); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, fmt.Errorf("create pod: %w", err)
	}
	return true, nil
}

func countReadyMaps(cluster *arkv1.ArkCluster) int32 {
	var n int32
	for _, m := range cluster.Status.Maps {
		if m.Phase == arkv1.MapPhaseRunning {
			n++
		}
	}
	return n
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// friendlyName returns a human-readable map name for sessionNameFormat.
// Includes Amendment E's regex fallback for community/modded maps.
func friendlyName(mapID string) string {
	switch mapID {
	case "TheIsland_WP":
		return "The Island"
	case "ScorchedEarth_WP":
		return "Scorched Earth"
	case "Aberration_WP":
		return "Aberration"
	case "Extinction_WP":
		return "Extinction"
	case "TheCenter_WP":
		return "The Center"
	case "Astraeos_WP":
		return "Astraeos"
	case "BobsMissions_WP":
		return "Club Ark"
	}
	s := mapID
	if len(s) > 3 && s[len(s)-3:] == "_WP" {
		s = s[:len(s)-3]
	}
	return camelCaseSplit.ReplaceAllString(s, "$1 $2")
}

// SetupWithManager wires the controller into the manager.
func (r *ArkClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&arkv1.ArkCluster{}, builder.WithPredicates()).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
