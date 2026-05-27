package reconcile

import (
	"context"
	"fmt"

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
	if gus == "" {
		gus = "[ServerSettings]\n"
	}
	game := resolveINI(ctx, c, cluster, mapSpec, "Game.ini")

	if err := writeINIConfigMap(ctx, c, cluster, GUSConfigMapName(cluster.Name, mapID), "GameUserSettings.ini", gus); err != nil {
		return err
	}
	return writeINIConfigMap(ctx, c, cluster, GameConfigMapName(cluster.Name, mapID), "Game.ini", game)
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
