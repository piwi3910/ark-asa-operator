// Package controller — ModUpdateController polls CurseForge for mod version
// changes and stamps an annotation on ArkClusters that have new versions
// available. The primary ArkClusterController watches that annotation as
// part of the pod-template hash and triggers a blue/green roll on change.
//
// Phase 4. Lives in the same controller-runtime manager as ArkClusterController.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/curseforge"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// AnnotationModsChanged is stamped by ModUpdateController when a tracked mod
// has a new latestFileID. The hash distinguishes one change from another so
// repeated reconciles know whether to re-roll.
const AnnotationModsChanged = "ark.watteel.com/mods-changed"

type ModUpdateReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	CurseForgeBaseURL string // empty = real CurseForge

	// for tests
	now func() time.Time
}

func (r *ModUpdateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.now == nil {
		r.now = time.Now
	}
	logger := log.FromContext(ctx).WithValues("arkcluster", req.NamespacedName)

	cluster := &arkv1.ArkCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if cluster.Spec.ModAutoUpdate == nil || !cluster.Spec.ModAutoUpdate.Enabled {
		return ctrl.Result{}, nil
	}
	interval := time.Duration(cluster.Spec.ModAutoUpdate.IntervalMinutes) * time.Minute
	if interval < 5*time.Minute {
		interval = 5 * time.Minute
	}
	if cluster.Status.Mods != nil && cluster.Status.Mods.LastCheckTime != nil {
		if elapsed := r.now().Sub(cluster.Status.Mods.LastCheckTime.Time); elapsed < interval {
			return ctrl.Result{RequeueAfter: interval - elapsed}, nil
		}
	}
	key, err := r.readAPIKey(ctx, cluster)
	if err != nil {
		setModFailedCondition(cluster, "APIKeyMissing", err.Error())
		_ = r.Status().Update(ctx, cluster)
		return ctrl.Result{RequeueAfter: interval}, nil
	}
	ids := collectModIDs(cluster)
	if len(ids) == 0 {
		return ctrl.Result{RequeueAfter: interval}, nil
	}
	base := r.CurseForgeBaseURL
	if base == "" {
		base = "https://api.curseforge.com"
	}
	cf := curseforge.NewClient(base, key, nil)
	info, err := cf.GetFiles(ctx, ids)
	now := metav1.NewTime(r.now())
	if err != nil {
		logger.Error(err, "curseforge fetch")
		setModFailedCondition(cluster, "ModAPIError", err.Error())
		if cluster.Status.Mods == nil {
			cluster.Status.Mods = &arkv1.ModStatus{}
		}
		cluster.Status.Mods.LastCheckTime = &now
		cluster.Status.Mods.LastError = err.Error()
		_ = r.Status().Update(ctx, cluster)
		return ctrl.Result{RequeueAfter: interval}, nil
	}
	tracked, changed := mergeModStatus(cluster.Status.Mods, info, now)
	if cluster.Status.Mods == nil {
		cluster.Status.Mods = &arkv1.ModStatus{}
	}
	next := metav1.NewTime(now.Add(interval))
	cluster.Status.Mods.LastCheckTime = &now
	cluster.Status.Mods.NextCheckTime = &next
	cluster.Status.Mods.LastError = ""
	cluster.Status.Mods.Tracked = tracked
	if err := r.Status().Update(ctx, cluster); err != nil && !apierrors.IsConflict(err) {
		return ctrl.Result{}, err
	}
	if changed {
		// Stamp annotation so the primary reconciler picks up the change.
		patch := client.MergeFrom(cluster.DeepCopy())
		if cluster.Annotations == nil {
			cluster.Annotations = map[string]string{}
		}
		cluster.Annotations[AnnotationModsChanged] = hashTracked(tracked)
		if err := r.Patch(ctx, cluster, patch); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: interval}, nil
}

func (r *ModUpdateReconciler) readAPIKey(ctx context.Context, cluster *arkv1.ArkCluster) (string, error) {
	sel := cluster.Spec.ModAutoUpdate.CurseForgeAPIKeyRef
	sec := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: sel.Name}, sec); err != nil {
		return "", fmt.Errorf("read api key secret: %w", err)
	}
	v := string(sec.Data[sel.Key])
	if v == "" {
		return "", fmt.Errorf("api key empty")
	}
	return v, nil
}

func collectModIDs(cluster *arkv1.ArkCluster) []int64 {
	set := map[int64]bool{}
	for _, m := range cluster.Spec.GlobalSettings.Mods {
		set[m] = true
	}
	for _, ms := range cluster.Spec.Maps {
		for _, m := range ms.Mods {
			set[m] = true
		}
	}
	out := make([]int64, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func mergeModStatus(existing *arkv1.ModStatus, info map[int64]curseforge.ModInfo, now metav1.Time) ([]arkv1.TrackedMod, bool) {
	prev := map[int64]arkv1.TrackedMod{}
	if existing != nil {
		for _, t := range existing.Tracked {
			prev[t.ID] = t
		}
	}
	changed := false
	tracked := make([]arkv1.TrackedMod, 0, len(info))
	for id, mi := range info {
		p, seen := prev[id]
		t := arkv1.TrackedMod{
			ID:               id,
			Slug:             mi.Slug,
			LatestVersion:    mi.LatestVersion,
			LatestFileID:     mi.LatestFileID,
			InstalledVersion: p.InstalledVersion,
			InstalledFileID:  p.InstalledFileID,
		}
		if !seen || t.InstalledFileID == 0 {
			// First time seeing this mod — treat installed = current latest.
			t.InstalledFileID = mi.LatestFileID
			t.InstalledVersion = mi.LatestVersion
		}
		if seen && p.LatestFileID != mi.LatestFileID {
			changed = true
			tt := now
			t.LastChanged = &tt
		} else if p.LastChanged != nil {
			t.LastChanged = p.LastChanged
		}
		tracked = append(tracked, t)
	}
	sort.Slice(tracked, func(i, j int) bool { return tracked[i].ID < tracked[j].ID })
	return tracked, changed
}

func hashTracked(t []arkv1.TrackedMod) string {
	var s string
	for _, m := range t {
		s += fmt.Sprintf("%d=%d;", m.ID, m.LatestFileID)
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}

func setModFailedCondition(cluster *arkv1.ArkCluster, reason, msg string) {
	cond := metav1.Condition{Type: "ModsHealthy", Status: metav1.ConditionFalse, Reason: reason, Message: msg, LastTransitionTime: metav1.Now()}
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == cond.Type {
			cluster.Status.Conditions[i] = cond
			return
		}
	}
	cluster.Status.Conditions = append(cluster.Status.Conditions, cond)
}

func (r *ModUpdateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&arkv1.ArkCluster{}).Named("modupdate").Complete(r)
}
