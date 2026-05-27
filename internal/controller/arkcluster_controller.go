// Package controller hosts the ArkCluster reconciler.
//
// Phase 2: blue/green rollUpdate path with Amendment F's persist-before-act
// ordering. The per-map flow has three branches:
//   - firstStart: no live pod yet → run init Job on active volume, then create pod.
//   - steady: live pod matches desired hash → surface status; auto-rollback on crash loop.
//   - rollUpdate: drift detected → install inactive, drain active via RCON, swap, recreate.
package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/ark"
	"github.com/piwi3910/ark-asa-operator/internal/finalizer"
	"github.com/piwi3910/ark-asa-operator/internal/rcon"
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

	// 2. Per-map fan-out.
	// Phase 3: respect OneAtATime rollout — only one map allowed to be mid-update at a time.
	midFlightMap := ""
	rolloutPolicy := cluster.Spec.UpdateStrategy.Rollout
	if rolloutPolicy == "" {
		rolloutPolicy = arkv1.RolloutOneAtATime
	}
	if rolloutPolicy != arkv1.RolloutParallel {
		for _, m := range cluster.Status.Maps {
			if m.Phase == arkv1.MapPhaseDrainingActive || m.Phase == arkv1.MapPhaseSwapping || m.Phase == arkv1.MapPhaseInstallingInactive {
				midFlightMap = m.ID
				break
			}
		}
	}

	busy := false
	for i, mapSpec := range cluster.Spec.Maps {
		if err := r.reconcileMap(ctx, cluster, mapSpec, i, &busy, midFlightMap); err != nil {
			logger.Error(err, "reconcileMap failed", "map", mapSpec.ID)
			return ctrl.Result{}, err
		}
	}

	// Phase 3: GC orphaned per-map resources.
	keep := map[string]bool{}
	for _, m := range cluster.Spec.Maps {
		keep[ark.MapSlug(m.ID)] = true
	}
	if err := r.gcOrphanedMaps(ctx, cluster, keep); err != nil {
		logger.Error(err, "gc orphaned maps")
		return ctrl.Result{}, err
	}
	// Prune status.Maps to match.
	filtered := cluster.Status.Maps[:0]
	for _, m := range cluster.Status.Maps {
		if keep[ark.MapSlug(m.ID)] {
			filtered = append(filtered, m)
		}
	}
	cluster.Status.Maps = filtered

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

// reconcileMap dispatches into firstStart / steady / rollUpdate based on whether
// a pod exists and whether its hash matches the desired hash.
func (r *ArkClusterReconciler) reconcileMap(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, i int, busy *bool, midFlightMap string) error {
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

	desiredHash := r.computePodHash(ctx, cluster, mapSpec, i, mapStatus.ActiveVolume)

	pods, err := r.listMapPods(ctx, cluster, mapSpec.ID)
	if err != nil {
		return err
	}
	activePod := findActivePod(pods, desiredHash)

	// Surface address from Service (whether we're idle or busy).
	r.refreshAddress(ctx, cluster, mapSpec, i, mapStatus)

	// Case A — no pod yet OR active volume needs initial steamcmd install.
	if activePod == nil {
		return r.firstStart(ctx, cluster, mapSpec, i, mapStatus, desiredHash, busy)
	}

	// Case B — pod matches desired hash and is healthy. Steady state.
	if activePod.Labels["ark.watteel.com/pod-template-hash"] == desiredHash {
		return r.steady(ctx, cluster, mapSpec, mapStatus, activePod, busy)
	}

	// Case C — drift. OneAtATime hold-back: if another map is mid-update and
	// we're not it, leave current pod alone and let the next reconcile retry.
	if midFlightMap != "" && midFlightMap != mapSpec.ID {
		*busy = true
		return nil
	}
	// Blue/green rollUpdate.
	return r.rollUpdate(ctx, cluster, mapSpec, i, mapStatus, desiredHash, busy, activePod)
}

// gcOrphanedMaps deletes per-map Pods, Services, and (if not PersistOnDelete)
// PVCs whose map is no longer in spec. The shared cluster-transfer PVC is preserved.
func (r *ArkClusterReconciler) gcOrphanedMaps(ctx context.Context, cluster *arkv1.ArkCluster, keep map[string]bool) error {
	sel := client.MatchingLabels{"ark.watteel.com/cluster": cluster.Name}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), sel); err == nil {
		for i := range pods.Items {
			slug := pods.Items[i].Labels["ark.watteel.com/map"]
			if slug != "" && !keep[slug] {
				_ = r.Delete(ctx, &pods.Items[i])
			}
		}
	}
	var svcs corev1.ServiceList
	if err := r.List(ctx, &svcs, client.InNamespace(cluster.Namespace), sel); err == nil {
		for i := range svcs.Items {
			slug := svcs.Items[i].Labels["ark.watteel.com/map"]
			if slug != "" && !keep[slug] {
				_ = r.Delete(ctx, &svcs.Items[i])
			}
		}
	}
	if !cluster.Spec.Storage.PersistOnDelete {
		var pvcs corev1.PersistentVolumeClaimList
		if err := r.List(ctx, &pvcs, client.InNamespace(cluster.Namespace), sel); err == nil {
			for i := range pvcs.Items {
				name := pvcs.Items[i].Name
				// Cluster-shared PVC stays regardless of which maps are present.
				if name == reconcile.PVCNameCluster(cluster.Name) {
					continue
				}
				// Per-map PVCs follow naming <cluster>-<slug>-...
				belongs := false
				for slug := range keep {
					if strings.HasPrefix(name, cluster.Name+"-"+slug+"-") {
						belongs = true
						break
					}
				}
				if !belongs {
					_ = r.Delete(ctx, &pvcs.Items[i])
				}
			}
		}
	}
	// Same treatment for orphaned ConfigMaps.
	var cms corev1.ConfigMapList
	if err := r.List(ctx, &cms, client.InNamespace(cluster.Namespace), sel); err == nil {
		for i := range cms.Items {
			name := cms.Items[i].Name
			belongs := false
			for slug := range keep {
				if strings.HasPrefix(name, cluster.Name+"-"+slug+"-") {
					belongs = true
					break
				}
			}
			if !belongs && (strings.HasSuffix(name, "-gus") || strings.HasSuffix(name, "-game")) {
				_ = r.Delete(ctx, &cms.Items[i])
			}
		}
	}
	return nil
}

// firstStart: ensure init Job on active volume, then create initial pod.
func (r *ArkClusterReconciler) firstStart(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, i int, mapStatus *arkv1.MapStatus, hash string, busy *bool) error {
	side := volumeSide(mapStatus.ActiveVolume)
	switch reconcile.InitJobStatus(ctx, r.Client, cluster, mapSpec.ID, side) {
	case "NotFound":
		_, err := reconcile.EnsureInitJob(ctx, r.Client, cluster, mapSpec.ID, side)
		mapStatus.Phase = arkv1.MapPhaseInstallingActive
		*busy = true
		return err
	case "Running":
		mapStatus.Phase = arkv1.MapPhaseInstallingActive
		*busy = true
		return nil
	case "Failed":
		mapStatus.Phase = arkv1.MapPhaseFailed
		reconcile.SetMapCondition(cluster, mapSpec.ID, metav1.Condition{
			Type: "InstallSucceeded", Status: metav1.ConditionFalse, Reason: "InstallJobFailed", Message: "steamcmd job failed",
		})
		return nil
	}
	// Succeeded — create pod
	_, err := r.ensurePodForMap(ctx, cluster, mapSpec, i, mapStatus.ActiveVolume, hash)
	*busy = true
	mapStatus.Phase = arkv1.MapPhaseProvisioning
	return err
}

// steady: pod hash matches desired and pod exists. Promote to Running if ready,
// otherwise stay busy. Detect CrashLoopBackOff for auto-rollback.
func (r *ArkClusterReconciler) steady(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, mapStatus *arkv1.MapStatus, pod *corev1.Pod, busy *bool) error {
	mapStatus.Pod = pod.Name
	mapStatus.SessionName = ark.SessionName(cluster.Spec.GlobalSettings.SessionNameFormat,
		cluster.Name, mapSpec.ID, friendlyName(mapSpec.ID))

	// Auto-rollback (Amendment F bug 2): if pod is CrashLooping with R >= 3
	// within ~5 min, swap back. Only auto-rolls back when we previously did a
	// blue/green swap (active=server-b); when active=server-a it's a first-deploy
	// failure with no prior good install to revert to.
	if r.shouldRollback(pod) && mapStatus.ActiveVolume == "server-b" {
		return r.rollback(ctx, cluster, mapSpec, mapStatus, busy, pod)
	}
	if r.shouldRollback(pod) {
		restarts := int32(0)
		if len(pod.Status.ContainerStatuses) > 0 {
			restarts = pod.Status.ContainerStatuses[0].RestartCount
		}
		reconcile.SetMapCondition(cluster, mapSpec.ID, metav1.Condition{
			Type: "PodReady", Status: metav1.ConditionFalse, Reason: "PodCrashLooping",
			Message: fmt.Sprintf("pod has restarted %d times — no prior volume to roll back to", restarts),
		})
		*busy = true
		return nil
	}

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
	return nil
}

// rollUpdate: blue/green flow with Amendment F's persist-before-act ordering.
func (r *ArkClusterReconciler) rollUpdate(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, i int, mapStatus *arkv1.MapStatus, desiredHash string, busy *bool, currentPod *corev1.Pod) error {
	_ = desiredHash // desiredHash is recomputed after swap; current value not needed here
	*busy = true
	inactive := otherSide(mapStatus.ActiveVolume)
	inactiveSide := volumeSide(inactive)

	// Step A: install on inactive volume
	switch reconcile.InitJobStatus(ctx, r.Client, cluster, mapSpec.ID, inactiveSide) {
	case "NotFound":
		_, err := reconcile.EnsureInitJob(ctx, r.Client, cluster, mapSpec.ID, inactiveSide)
		mapStatus.Phase = arkv1.MapPhaseInstallingInactive
		return err
	case "Running":
		mapStatus.Phase = arkv1.MapPhaseInstallingInactive
		return nil
	case "Failed":
		reconcile.SetMapCondition(cluster, mapSpec.ID, metav1.Condition{
			Type: "InstallSucceeded", Status: metav1.ConditionFalse, Reason: "InstallJobFailed",
		})
		mapStatus.Phase = arkv1.MapPhaseFailed
		return nil
	}

	// Step B: drain active pod. drainDeadline is the source of truth for "we started drain".
	if mapStatus.DrainDeadline == nil {
		grace := cluster.Spec.UpdateStrategy.GracefulShutdown
		if grace.Duration < 0 {
			grace.Duration = 0
		}
		deadline := metav1.NewTime(time.Now().Add(grace.Duration))
		mapStatus.DrainDeadline = &deadline
		mapStatus.Phase = arkv1.MapPhaseDrainingActive
		// Amendment F bug 3: persist BEFORE issuing the RCON announce.
		if err := r.persistStatus(ctx, cluster); err != nil {
			// Roll back the in-memory mutation so a fresh reconcile retries from a clean state.
			mapStatus.DrainDeadline = nil
			mapStatus.Phase = arkv1.MapPhaseRunning
			return err
		}
		if grace.Duration > 0 && mapStatus.RconAddress != "" {
			pw, _ := r.readAdminPassword(ctx, cluster)
			_ = rcon.AnnounceShutdown(ctx, mapStatus.RconAddress, pw, grace.Duration, "cluster update")
		}
		return nil
	}
	if time.Now().Before(mapStatus.DrainDeadline.Time) {
		mapStatus.Phase = arkv1.MapPhaseDrainingActive
		return nil
	}

	// Step C/D: deadline reached. SaveAndExit, swap, create new pod.

	// SaveAndExit best-effort (idempotent at the world level).
	if mapStatus.RconAddress != "" {
		pw, _ := r.readAdminPassword(ctx, cluster)
		_ = rcon.SaveAndExit(ctx, mapStatus.RconAddress, pw)
	}

	// COMMIT THE SWAP IN STATUS BEFORE acting on the world (Amendment F bug 1).
	mapStatus.ActiveVolume = inactive
	mapStatus.PendingBuildID = ""
	mapStatus.DrainDeadline = nil
	mapStatus.Phase = arkv1.MapPhaseSwapping
	if err := r.persistStatus(ctx, cluster); err != nil {
		return err
	}

	// World action: delete the dying pod.
	if currentPod != nil && currentPod.DeletionTimestamp == nil {
		grace := int64(60)
		_ = r.Delete(ctx, currentPod, &client.DeleteOptions{GracePeriodSeconds: &grace})
	}

	// Create new pod with the (newly-active) inactive volume.
	newHash := r.computePodHash(ctx, cluster, mapSpec, i, mapStatus.ActiveVolume)
	_, err := r.ensurePodForMap(ctx, cluster, mapSpec, i, mapStatus.ActiveVolume, newHash)
	return err
}

// rollback: auto-rollback flow (Amendment F bug 2 — persist before delete).
func (r *ArkClusterReconciler) rollback(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, mapStatus *arkv1.MapStatus, busy *bool, badPod *corev1.Pod) error {
	*busy = true
	previous := otherSide(mapStatus.ActiveVolume)
	mapStatus.ActiveVolume = previous
	reconcile.SetMapCondition(cluster, mapSpec.ID, metav1.Condition{
		Type: "RollbackOccurred", Status: metav1.ConditionTrue, Reason: "PodCrashLooping",
		Message: fmt.Sprintf("auto-rolled back to %s after crash loop", previous),
	})
	if err := r.persistStatus(ctx, cluster); err != nil {
		return err
	}
	if badPod != nil && badPod.DeletionTimestamp == nil {
		grace := int64(0)
		_ = r.Delete(ctx, badPod, &client.DeleteOptions{GracePeriodSeconds: &grace})
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(cluster, corev1.EventTypeWarning, "AutoRollback",
			"map %s rolled back to %s due to crash loop", mapSpec.ID, previous)
	}
	return nil
}

// shouldRollback returns true if the pod has >=3 restarts caused by non-zero exit
// codes within roughly the last 5 minutes.
func (r *ArkClusterReconciler) shouldRollback(pod *corev1.Pod) bool {
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	cs := pod.Status.ContainerStatuses[0]
	if cs.RestartCount < 3 {
		return false
	}
	last := cs.LastTerminationState.Terminated
	if last == nil {
		return false
	}
	if last.ExitCode == 0 {
		return false
	}
	return time.Since(last.FinishedAt.Time) < 5*time.Minute
}

// persistStatus writes the cluster's status, used between in-memory state
// mutations and world-altering actions (Amendment F).
func (r *ArkClusterReconciler) persistStatus(ctx context.Context, cluster *arkv1.ArkCluster) error {
	if err := r.Status().Update(ctx, cluster); err != nil && !apierrors.IsConflict(err) {
		return fmt.Errorf("persist status: %w", err)
	}
	return nil
}

// readAdminPassword fetches the RCON admin password from the cluster's Secret.
func (r *ArkClusterReconciler) readAdminPassword(ctx context.Context, cluster *arkv1.ArkCluster) (string, error) {
	sel := cluster.Spec.GlobalSettings.AdminPassword
	name := reconcile.SecretsName(cluster.Name)
	key := "adminPassword"
	if sel != nil {
		name, key = sel.Name, sel.Key
	}
	sec := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: name}, sec); err != nil {
		return "", err
	}
	return string(sec.Data[key]), nil
}

// ensurePodForMap creates the server pod if missing; reports created=true.
// Stale pods are NOT deleted here — rollUpdate handles that explicitly.
func (r *ArkClusterReconciler) ensurePodForMap(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, i int, activeVolume, hash string) (bool, error) {
	pod := reconcile.BuildServerPod(reconcile.PodInput{
		Cluster: cluster, MapID: mapSpec.ID, MapIndex: i,
		FriendlyMap: friendlyName(mapSpec.ID), ActiveVolume: activeVolume, Hash: hash,
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

func (r *ArkClusterReconciler) listMapPods(ctx context.Context, cluster *arkv1.ArkCluster, mapID string) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"ark.watteel.com/cluster": cluster.Name,
		"ark.watteel.com/map":     ark.MapSlug(mapID),
	}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (r *ArkClusterReconciler) refreshAddress(ctx context.Context, cluster *arkv1.ArkCluster, mapSpec arkv1.MapSpec, i int, mapStatus *arkv1.MapStatus) {
	svc := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: reconcile.ServiceName(cluster.Name, mapSpec.ID)}, svc); err != nil {
		return
	}
	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return
	}
	ip := svc.Status.LoadBalancer.Ingress[0].IP
	gp := ark.GamePort(cluster.Spec.Service.GamePortStart, i)
	rp := ark.RconPort(cluster.Spec.Service.RconPortStart, i)
	mapStatus.Address = fmt.Sprintf("%s:%d", ip, gp)
	mapStatus.RconAddress = fmt.Sprintf("%s:%d", ip, rp)
}

func findActivePod(pods []corev1.Pod, desiredHash string) *corev1.Pod {
	// Prefer pod matching desired hash (ready or not).
	for i := range pods {
		if pods[i].DeletionTimestamp == nil && pods[i].Labels["ark.watteel.com/pod-template-hash"] == desiredHash {
			return &pods[i]
		}
	}
	// Otherwise return the first non-terminating pod (which would be stale).
	for i := range pods {
		if pods[i].DeletionTimestamp == nil {
			return &pods[i]
		}
	}
	return nil
}

func volumeSide(activeVolume string) string {
	if activeVolume == "server-b" {
		return "b"
	}
	return "a"
}

func otherSide(active string) string {
	if active == "server-a" {
		return "server-b"
	}
	return "server-a"
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
