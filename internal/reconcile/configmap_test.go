package reconcile

import (
	"context"
	"strings"
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestConfigMapNamesUseMapSlug(t *testing.T) {
	if got := GUSConfigMapName("c", "TheIsland_WP"); got != "c-island-gus" {
		t.Errorf("GUSConfigMapName = %q", got)
	}
	if got := GameConfigMapName("c", "TheIsland_WP"); got != "c-island-game" {
		t.Errorf("GameConfigMapName = %q", got)
	}
}

func TestEnsureMapINIConfigMapsDefaults(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec:       arkv1.ArkClusterSpec{Maps: []arkv1.MapSpec{{ID: "TheIsland_WP"}}},
	}
	c := newFake(t).Build()
	if err := EnsureMapINIConfigMaps(context.Background(), c, cluster, "TheIsland_WP"); err != nil {
		t.Fatal(err)
	}
	gus := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "c-island-gus", Namespace: "ns"}, gus); err != nil {
		t.Fatal(err)
	}
	if _, ok := gus.Data["GameUserSettings.ini"]; !ok {
		t.Error("GUS configmap missing GameUserSettings.ini key")
	}
	game := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "c-island-game", Namespace: "ns"}, game); err != nil {
		t.Fatal(err)
	}
	if _, ok := game.Data["Game.ini"]; !ok {
		t.Error("Game configmap missing Game.ini key")
	}
}

func TestEnsureMapINIConfigMapsFromMapSpec(t *testing.T) {
	src := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "src-gus", Namespace: "ns"},
		Data:       map[string]string{"GameUserSettings.ini": "[ServerSettings]\nDifficultyOffset=1.0\n"},
	}
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: arkv1.ArkClusterSpec{
			Maps:    []arkv1.MapSpec{{ID: "TheIsland_WP", GameUserSettings: &arkv1.ConfigMapRef{Name: "src-gus"}}},
			Service: arkv1.ServiceSpec{RconPortStart: 27020},
		},
	}
	c := newFake(t).WithObjects(src).Build()
	if err := EnsureMapINIConfigMaps(context.Background(), c, cluster, "TheIsland_WP"); err != nil {
		t.Fatal(err)
	}
	gus := &corev1.ConfigMap{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "c-island-gus", Namespace: "ns"}, gus)
	got := gus.Data["GameUserSettings.ini"]
	if !strings.Contains(got, "DifficultyOffset=1.0") {
		t.Errorf("user GUS content not preserved: %q", got)
	}
	if !strings.Contains(got, "RCONEnabled=True") || !strings.Contains(got, "RCONPort=27020") {
		t.Errorf("RCON defaults not injected: %q", got)
	}
}

func TestEnsureMapINIConfigMapsGlobalFallback(t *testing.T) {
	src := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "global-gus", Namespace: "ns"},
		Data:       map[string]string{"GameUserSettings.ini": "[ServerSettings]\nGlobal=1\n"},
	}
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: arkv1.ArkClusterSpec{
			Maps:           []arkv1.MapSpec{{ID: "TheIsland_WP"}},
			GlobalSettings: arkv1.GlobalSettings{GameUserSettings: &arkv1.ConfigMapRef{Name: "global-gus"}},
		},
	}
	c := newFake(t).WithObjects(src).Build()
	if err := EnsureMapINIConfigMaps(context.Background(), c, cluster, "TheIsland_WP"); err != nil {
		t.Fatal(err)
	}
	gus := &corev1.ConfigMap{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "c-island-gus", Namespace: "ns"}, gus)
	got := gus.Data["GameUserSettings.ini"]
	if !strings.Contains(got, "Global=1") {
		t.Errorf("global fallback not applied: %q", got)
	}
	if !strings.Contains(got, "RCONEnabled=True") {
		t.Errorf("RCON defaults not injected: %q", got)
	}
}

func TestEnsureGUSDefaultsAddsMissing(t *testing.T) {
	got := ensureGUSDefaults("", 27020)
	if !strings.Contains(got, "RCONEnabled=True") || !strings.Contains(got, "RCONPort=27020") {
		t.Errorf("missing defaults in: %q", got)
	}
	if !strings.Contains(got, "[ServerSettings]") {
		t.Errorf("missing section header in: %q", got)
	}
}

func TestEnsureGUSDefaultsRespectsUserKeys(t *testing.T) {
	user := "[ServerSettings]\nRCONEnabled=True\nRCONPort=99999\nDifficultyOffset=1.0\n"
	got := ensureGUSDefaults(user, 27020)
	if !strings.Contains(got, "RCONPort=99999") {
		t.Errorf("user RCONPort overridden: %q", got)
	}
	if strings.Count(got, "RCONPort=") != 1 {
		t.Errorf("duplicate RCONPort lines: %q", got)
	}
}

func TestEnsureGUSDefaultsInjectsSectionIfMissing(t *testing.T) {
	user := "[Some/Other.Section]\nKey=Val\n"
	got := ensureGUSDefaults(user, 27020)
	if !strings.Contains(got, "[ServerSettings]") {
		t.Error("section header not prepended")
	}
	if !strings.Contains(got, "RCONEnabled=True") {
		t.Error("RCONEnabled not injected")
	}
}
