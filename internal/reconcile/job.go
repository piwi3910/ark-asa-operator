package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/ark"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// arkAppID is the Steam app ID of the ARK SA dedicated server.
const arkAppID = "2430930"

// installIdentity returns a short hex digest of inputs that determine what gets
// installed (image + ARK app id). Stable across unrelated spec changes —
// Amendment D's fix for duplicate init Jobs.
func installIdentity(cluster *arkv1.ArkCluster) string {
	image := cluster.Spec.Image
	if image == "" {
		image = "docker.io/sknnr/ark-ascended-server:latest"
	}
	sum := sha256.Sum256([]byte(image + "|" + arkAppID))
	return hex.EncodeToString(sum[:4])
}

// InitJobName returns the per-(cluster,map,side) init Job name.
// Stable across cluster.Generation changes (Amendment D).
func InitJobName(cluster *arkv1.ArkCluster, mapID, side string) string {
	return fmt.Sprintf("%s-%s-init-%s-%s", cluster.Name, ark.MapSlug(mapID), side, installIdentity(cluster))
}

// EnsureInitJob creates the steamcmd-validate Job for the given inactive
// volume side. Returns (created, error). If the Job already exists,
// returns (false, nil) — idempotent.
func EnsureInitJob(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster, mapID, side string) (bool, error) {
	name := InitJobName(cluster, mapID, side)
	existing := &batchv1.Job{}
	err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: cluster.Namespace}, existing)
	if err == nil {
		return false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, err
	}
	image := cluster.Spec.Image
	if image == "" {
		image = "docker.io/sknnr/ark-ascended-server:latest"
	}

	psc := cluster.Spec.PodSecurityContext
	if psc == nil {
		uid := int64(10000)
		gid := int64(10000)
		fsg := int64(10000)
		psc = &corev1.PodSecurityContext{RunAsUser: &uid, RunAsGroup: &gid, FSGroup: &fsg}
	}

	backoff := int32(3)
	ttl := int32(3600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"ark.watteel.com/cluster": cluster.Name,
				"ark.watteel.com/map":     ark.MapSlug(mapID),
				"ark.watteel.com/role":    "init",
				"ark.watteel.com/side":    side,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:   corev1.RestartPolicyOnFailure,
					SecurityContext: psc,
					Containers: []corev1.Container{
						{
							Name:    "install",
							Image:   image,
							Command: []string{"/bin/bash", "-c"},
							Args: []string{
								`set -euo pipefail
steamcmd \
  +@sSteamCmdForcePlatformType windows \
  +force_install_dir /home/steam/ark \
  +login anonymous \
  +app_update ` + arkAppID + ` validate \
  +quit`,
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "server", MountPath: "/home/steam/ark"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "server",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: PVCNameServer(cluster.Name, mapID, side),
								},
							},
						},
					},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(cluster, job, c.Scheme()); err != nil {
		return false, err
	}
	if err := c.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// InitJobStatus returns one of: "NotFound", "Running", "Succeeded", "Failed".
func InitJobStatus(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster, mapID, side string) string {
	job := &batchv1.Job{}
	err := c.Get(ctx, client.ObjectKey{Name: InitJobName(cluster, mapID, side), Namespace: cluster.Namespace}, job)
	if apierrors.IsNotFound(err) {
		return "NotFound"
	}
	if err != nil {
		return "Failed"
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			return "Succeeded"
		}
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return "Failed"
		}
	}
	return "Running"
}
