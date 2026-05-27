package reconcile

import (
	"strings"
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodNameUsesMapSlug(t *testing.T) {
	if got := PodName("piwis-place", "TheIsland_WP", "abc12345"); got != "piwis-place-island-abc12345" {
		t.Errorf("PodName = %q", got)
	}
}

func TestBuildServerPodEnv(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "piwis-place", Namespace: "ark-operator"},
		Spec: arkv1.ArkClusterSpec{
			Image:     "ghcr.io/sknnr/ark-ascended-server:latest",
			ClusterID: "piwis-place",
			GlobalSettings: arkv1.GlobalSettings{
				SessionNameFormat: "piwi's place",
				MaxPlayers:        70,
				BattlEye:          false,
				AllowedPlatforms:  []string{"ALL"},
			},
			Service: arkv1.ServiceSpec{GamePortStart: 7777, RconPortStart: 27020},
		},
	}
	pod := BuildServerPod(PodInput{
		Cluster:      cluster,
		MapID:        "TheIsland_WP",
		MapIndex:     0,
		FriendlyMap:  "The Island",
		ActiveVolume: "server-a",
		Hash:         "abc123",
	})
	if pod.Name == "" {
		t.Fatal("pod name empty")
	}
	env := map[string]string{}
	for _, e := range pod.Spec.Containers[0].Env {
		if e.Value != "" {
			env[e.Name] = e.Value
		}
	}
	if env["SESSION_NAME"] != "piwi's place" {
		t.Errorf("SESSION_NAME wrong: %q", env["SESSION_NAME"])
	}
	if env["SERVER_MAP"] != "TheIsland_WP" {
		t.Errorf("SERVER_MAP must use raw map ID, got %q", env["SERVER_MAP"])
	}
	if env["GAME_PORT"] != "7777" {
		t.Errorf("GAME_PORT wrong: %q", env["GAME_PORT"])
	}
	if env["RCON_PORT"] != "27020" {
		t.Errorf("RCON_PORT wrong: %q", env["RCON_PORT"])
	}
	if pod.Labels["ark.watteel.com/pod-template-hash"] != "abc123" {
		t.Errorf("hash label missing")
	}
	if pod.Labels["ark.watteel.com/map"] != "island" {
		t.Errorf("map label should be slug 'island', got %q", pod.Labels["ark.watteel.com/map"])
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyAlways {
		t.Errorf("restart policy = %s", pod.Spec.RestartPolicy)
	}
}

func TestBuildServerPodHasGenerousLivenessProbe(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec:       arkv1.ArkClusterSpec{ClusterID: "c"},
	}
	pod := BuildServerPod(PodInput{Cluster: cluster, MapID: "TheIsland_WP", ActiveVolume: "server-a"})
	lp := pod.Spec.Containers[0].LivenessProbe
	if lp == nil {
		t.Fatal("expected a livenessProbe (RCON ListPlayers, 5-min budget)")
	}
	if lp.Exec == nil || len(lp.Exec.Command) == 0 {
		t.Fatal("livenessProbe must be exec-based")
	}
	cmd := strings.Join(lp.Exec.Command, " ")
	if !strings.Contains(cmd, "rcon") || !strings.Contains(cmd, "ListPlayers") {
		t.Errorf("livenessProbe should call rcon ListPlayers; got %q", cmd)
	}
	if lp.PeriodSeconds != 30 || lp.TimeoutSeconds != 10 || lp.FailureThreshold != 10 {
		t.Errorf("livenessProbe timing wrong; got period=%d timeout=%d threshold=%d",
			lp.PeriodSeconds, lp.TimeoutSeconds, lp.FailureThreshold)
	}
}

func TestBuildServerPodVolumeMountsUseSlug(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec:       arkv1.ArkClusterSpec{ClusterID: "c"},
	}
	pod := BuildServerPod(PodInput{Cluster: cluster, MapID: "TheIsland_WP", ActiveVolume: "server-a"})
	// All PVC claims should reference the slugged names
	gotClaims := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			gotClaims[v.PersistentVolumeClaim.ClaimName] = true
		}
	}
	for _, want := range []string{"c-island-server-a", "c-island-saves", "c-cluster"} {
		if !gotClaims[want] {
			t.Errorf("expected PVC claim %q in pod volumes; have %+v", want, gotClaims)
		}
	}
}
