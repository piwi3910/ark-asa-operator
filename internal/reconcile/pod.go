package reconcile

import (
	"fmt"
	"strconv"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/ark"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// PodInput is everything the Pod builder needs that isn't already on the ArkCluster.
type PodInput struct {
	Cluster      *arkv1.ArkCluster
	MapID        string // raw ARK map ID, e.g. "TheIsland_WP"
	MapIndex     int    // zero-based index into Cluster.Spec.Maps
	FriendlyMap  string // human-readable name for sessionNameFormat substitution
	ActiveVolume string // "server-a" or "server-b"
	Hash         string // pod-template hash (drift-detection label)
}

// PodName returns the per-pod resource name: <cluster>-<mapSlug>-<hash>.
// hash is truncated to 8 chars; the controller produces it via ark.PodTemplateHash.
func PodName(cluster, mapID, hash string) string {
	if hash == "" {
		hash = "0"
	}
	if len(hash) > 8 {
		hash = hash[:8]
	}
	return fmt.Sprintf("%s-%s-%s", cluster, ark.MapSlug(mapID), hash)
}

// BuildServerPod constructs the server Pod for one map, ready for Create.
// Does not call CreateOrUpdate — that's EnsurePod's job (which lands in a
// later task with the controller wiring).
func BuildServerPod(in PodInput) *corev1.Pod {
	cluster := in.Cluster
	gs := cluster.Spec.GlobalSettings
	slug := ark.MapSlug(in.MapID)
	gamePort := ark.GamePort(cluster.Spec.Service.GamePortStart, in.MapIndex)
	rconPort := ark.RconPort(cluster.Spec.Service.RconPortStart, in.MapIndex)
	sessionName := ark.SessionName(gs.SessionNameFormat, cluster.Name, in.MapID, in.FriendlyMap)

	mods := gs.Mods
	if ms := findMap(cluster, in.MapID); ms != nil && len(ms.Mods) > 0 {
		mods = ms.Mods
	}
	extraFlags := ark.ExtraFlags(ark.ExtraFlagsInput{
		ClusterDir:       "/srv/ark/cluster",
		ClusterID:        cluster.Spec.ClusterID,
		Mods:             mods,
		BattlEyeEnabled:  gs.BattlEye,
		AllowedPlatforms: gs.AllowedPlatforms,
		ExtraOptions:     ark.ComposeExtraOptions(gs),
	})
	extraSettings := ark.ExtraSettings(gs, rconPort)

	image := cluster.Spec.Image
	if image == "" {
		image = "ghcr.io/sknnr/ark-ascended-server:latest"
	}

	tgps := int64(1800)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PodName(cluster.Name, in.MapID, in.Hash),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"ark.watteel.com/cluster":           cluster.Name,
				"ark.watteel.com/map":               slug,
				"ark.watteel.com/role":              "server",
				"ark.watteel.com/active-volume":     in.ActiveVolume,
				"ark.watteel.com/pod-template-hash": in.Hash,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyAlways,
			TerminationGracePeriodSeconds: &tgps,
			SecurityContext:               cluster.Spec.PodSecurityContext,
			NodeSelector:                  cluster.Spec.NodeSelector,
			Tolerations:                   cluster.Spec.Tolerations,
			Containers: []corev1.Container{
				{
					Name:            "ark",
					Image:           image,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Env: []corev1.EnvVar{
						{Name: "SESSION_NAME", Value: sessionName},
						{Name: "SERVER_MAP", Value: in.MapID},
						{Name: "GAME_PORT", Value: strconv.Itoa(int(gamePort))},
						{Name: "RCON_PORT", Value: strconv.Itoa(int(rconPort))},
						{Name: "EXTRA_SETTINGS", Value: extraSettings},
						{Name: "EXTRA_FLAGS", Value: extraFlags},
						{Name: "MODS", Value: modsCSV(mods)},
						{Name: "SERVER_PASSWORD", ValueFrom: serverPasswordSource(gs.ServerPassword)},
						{Name: "SERVER_ADMIN_PASSWORD", ValueFrom: adminPasswordSource(gs.AdminPassword, SecretsName(cluster.Name))},
					},
					Ports: []corev1.ContainerPort{
						{Name: "game", ContainerPort: gamePort, Protocol: corev1.ProtocolUDP},
						{Name: "rcon", ContainerPort: rconPort, Protocol: corev1.ProtocolTCP},
					},
					Resources: cluster.Spec.Resources,
					StartupProbe: &corev1.Probe{
						ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("rcon")}},
						InitialDelaySeconds: 60,
						PeriodSeconds:       10,
						FailureThreshold:    60,
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("rcon")}},
						PeriodSeconds:    15,
						TimeoutSeconds:   10,
						FailureThreshold: 4,
					},
					// LivenessProbe: gorcon `rcon` is bundled in the sknnr image at /usr/local/bin/rcon.
					// Gated by startupProbe so cold-start is safe. 30s × 10 failures = 5-min budget.
					LivenessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"/bin/sh", "-c",
									`rcon -a 127.0.0.1:${RCON_PORT} -p "${SERVER_ADMIN_PASSWORD}" ListPlayers`,
								},
							},
						},
						PeriodSeconds:    30,
						TimeoutSeconds:   10,
						FailureThreshold: 10,
					},
					Lifecycle: &corev1.Lifecycle{
						PreStop: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"/bin/sh", "-c",
									`rcon -a 127.0.0.1:${RCON_PORT} -p "${SERVER_ADMIN_PASSWORD}" SaveWorld; rcon -a 127.0.0.1:${RCON_PORT} -p "${SERVER_ADMIN_PASSWORD}" DoExit; sleep 1`,
								},
							},
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "server", MountPath: "/home/steam/ark"},
						{Name: "saves", MountPath: "/home/steam/ark/ShooterGame/Saved"},
						{Name: "cluster-xfer", MountPath: "/srv/ark/cluster"},
						{Name: "gus-ini", MountPath: "/home/steam/ark/ShooterGame/Saved/Config/WindowsServer/GameUserSettings.ini", SubPath: "GameUserSettings.ini", ReadOnly: true},
						{Name: "game-ini", MountPath: "/home/steam/ark/ShooterGame/Saved/Config/WindowsServer/Game.ini", SubPath: "Game.ini", ReadOnly: true},
					},
				},
			},
			Volumes: []corev1.Volume{
				{Name: "server", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: PVCNameServer(cluster.Name, in.MapID, volumeSide(in.ActiveVolume))}}},
				{Name: "saves", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: PVCNameSaves(cluster.Name, in.MapID)}}},
				{Name: "cluster-xfer", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: PVCNameCluster(cluster.Name)}}},
				{Name: "gus-ini", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: GUSConfigMapName(cluster.Name, in.MapID)}}}},
				{Name: "game-ini", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: GameConfigMapName(cluster.Name, in.MapID)}}}},
			},
		},
	}
	return pod
}

func volumeSide(activeVolume string) string {
	if activeVolume == "server-b" {
		return "b"
	}
	return "a"
}

func modsCSV(mods []int64) string {
	if len(mods) == 0 {
		return ""
	}
	out := strconv.FormatInt(mods[0], 10)
	for _, m := range mods[1:] {
		out += "," + strconv.FormatInt(m, 10)
	}
	return out
}

func serverPasswordSource(sel *corev1.SecretKeySelector) *corev1.EnvVarSource {
	if sel == nil {
		return nil
	}
	return &corev1.EnvVarSource{SecretKeyRef: sel}
}

func adminPasswordSource(sel *corev1.SecretKeySelector, fallbackSecretName string) *corev1.EnvVarSource {
	if sel != nil {
		return &corev1.EnvVarSource{SecretKeyRef: sel}
	}
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: fallbackSecretName},
		Key:                  "adminPassword",
	}}
}
