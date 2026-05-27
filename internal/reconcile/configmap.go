package reconcile

import (
	"context"
	"fmt"
	"strings"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/ark"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// GUSConfigMapName returns the per-map GameUserSettings.ini ConfigMap name.
func GUSConfigMapName(cluster, mapID string) string {
	return fmt.Sprintf("%s-%s-gus", cluster, ark.MapSlug(mapID))
}

// GameConfigMapName returns the per-map Game.ini ConfigMap name.
func GameConfigMapName(cluster, mapID string) string {
	return fmt.Sprintf("%s-%s-game", cluster, ark.MapSlug(mapID))
}

// EnsureMapINIConfigMaps materializes the per-map GUS + Game.ini ConfigMaps
// the pod mounts via subPath. Source priority:
//  1. spec.maps[i].{gameUserSettings,game}
//  2. spec.globalSettings.{gameUserSettings,game}
//  3. an empty default ("[ServerSettings]\n" for GUS, "" for Game).
func EnsureMapINIConfigMaps(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster, mapID string) error {
	mapSpec := findMap(cluster, mapID)
	gus := resolveINI(ctx, c, cluster, mapSpec, "GameUserSettings.ini")
	game := resolveINI(ctx, c, cluster, mapSpec, "Game.ini")

	// Always inject RCONEnabled + RCONPort so the ARK server's RCON listens.
	// Without these, the sknnr image's entrypoint shell script also writes
	// them, but only if the GUS file doesn't already exist — and our subPath
	// mount makes it exist. So we must take ownership of the bootstrap here.
	idx := findMapIndex(cluster, mapID)
	rconPort := cluster.Spec.Service.RconPortStart + int32(idx)
	gus = ensureGUSDefaults(gus, rconPort)

	if err := writeINIConfigMap(ctx, c, cluster, GUSConfigMapName(cluster.Name, mapID), "GameUserSettings.ini", gus); err != nil {
		return err
	}
	return writeINIConfigMap(ctx, c, cluster, GameConfigMapName(cluster.Name, mapID), "Game.ini", game)
}

// findMapIndex returns the spec.maps index for mapID, or 0 if not found.
func findMapIndex(cluster *arkv1.ArkCluster, mapID string) int {
	for i, m := range cluster.Spec.Maps {
		if m.ID == mapID {
			return i
		}
	}
	return 0
}

// ensureGUSDefaults takes any user-supplied GUS content and injects RCONEnabled
// + RCONPort under [ServerSettings] if not already present. Simple line-based
// algorithm:
//
//  1. Ensure the content has a [ServerSettings] section header. If absent,
//     prepend it.
//  2. Within [ServerSettings] (until the next [Section] header), if
//     RCONEnabled= or RCONPort= are missing, insert them right after the header.
//
// This is intentionally a simple textual merge — not a full INI parser. Users
// who want fancier behavior can supply their own GUS ConfigMap (we still merge
// RCON defaults on top, since that's a hard requirement for the controller's
// own readiness/liveness probes to function).
func ensureGUSDefaults(content string, rconPort int32) string {
	if content == "" {
		return fmt.Sprintf("[ServerSettings]\nRCONEnabled=True\nRCONPort=%d\n", rconPort)
	}
	lines := strings.Split(content, "\n")
	ssStart := -1
	for i, l := range lines {
		if strings.TrimSpace(l) == "[ServerSettings]" {
			ssStart = i
			break
		}
	}
	if ssStart == -1 {
		newHead := []string{"[ServerSettings]", "RCONEnabled=True", fmt.Sprintf("RCONPort=%d", rconPort), ""}
		return strings.Join(append(newHead, lines...), "\n")
	}
	hasEnabled, hasPort := false, false
	end := len(lines)
	for i := ssStart + 1; i < len(lines); i++ {
		t := strings.TrimSpace(lines[i])
		if strings.HasPrefix(t, "[") && strings.HasSuffix(t, "]") {
			end = i
			break
		}
		if strings.HasPrefix(t, "RCONEnabled=") {
			hasEnabled = true
		}
		if strings.HasPrefix(t, "RCONPort=") {
			hasPort = true
		}
	}
	if hasEnabled && hasPort {
		return content
	}
	inject := []string{}
	if !hasEnabled {
		inject = append(inject, "RCONEnabled=True")
	}
	if !hasPort {
		inject = append(inject, fmt.Sprintf("RCONPort=%d", rconPort))
	}
	out := make([]string, 0, len(lines)+len(inject))
	out = append(out, lines[:ssStart+1]...)
	out = append(out, inject...)
	out = append(out, lines[ssStart+1:end]...)
	out = append(out, lines[end:]...)
	return strings.Join(out, "\n")
}

func findMap(cluster *arkv1.ArkCluster, mapID string) *arkv1.MapSpec {
	for i := range cluster.Spec.Maps {
		if cluster.Spec.Maps[i].ID == mapID {
			return &cluster.Spec.Maps[i]
		}
	}
	return nil
}

func resolveINI(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster, mapSpec *arkv1.MapSpec, key string) string {
	var ref *arkv1.ConfigMapRef
	if mapSpec != nil {
		if key == "GameUserSettings.ini" {
			ref = mapSpec.GameUserSettings
		} else {
			ref = mapSpec.Game
		}
	}
	if ref == nil {
		if key == "GameUserSettings.ini" {
			ref = cluster.Spec.GlobalSettings.GameUserSettings
		} else {
			ref = cluster.Spec.GlobalSettings.Game
		}
	}
	if ref == nil {
		return ""
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: cluster.Namespace}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return ""
		}
		return ""
	}
	return cm.Data[key]
}

func writeINIConfigMap(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster, name, key, content string) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, cm, func() error {
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[key] = content
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels["ark.watteel.com/cluster"] = cluster.Name
		return controllerutil.SetControllerReference(cluster, cm, c.Scheme())
	})
	return err
}
