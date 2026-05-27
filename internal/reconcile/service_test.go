package reconcile

import (
	"context"
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestServiceNameUsesMapSlug(t *testing.T) {
	if got := ServiceName("piwis-place", "TheIsland_WP"); got != "piwis-place-island" {
		t.Errorf("ServiceName = %q", got)
	}
}

func TestEnsureServiceCreatesLoadBalancer(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "piwis-place", Namespace: "ark-operator"},
		Spec: arkv1.ArkClusterSpec{
			Service: arkv1.ServiceSpec{
				Type:          corev1.ServiceTypeLoadBalancer,
				GamePortStart: 7777,
				RconPortStart: 27020,
			},
		},
	}
	c := newFake(t).Build()
	if err := EnsureService(context.Background(), c, cluster, "TheIsland_WP", 0); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Service{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "piwis-place-island", Namespace: "ark-operator"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Errorf("type = %s", got.Spec.Type)
	}
	// Amendment G: default is Cluster, NOT Local
	if got.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyCluster {
		t.Errorf("etp = %s, want Cluster (Amendment G default)", got.Spec.ExternalTrafficPolicy)
	}
	if got.Spec.Ports[0].Port != 7777 || got.Spec.Ports[0].Protocol != corev1.ProtocolUDP {
		t.Errorf("game port wrong: %+v", got.Spec.Ports[0])
	}
	if got.Spec.Ports[1].Port != 27020 || got.Spec.Ports[1].Protocol != corev1.ProtocolTCP {
		t.Errorf("rcon port wrong: %+v", got.Spec.Ports[1])
	}
	// Selector uses the slug
	if got.Spec.Selector["ark.watteel.com/map"] != "island" {
		t.Errorf("selector map should be slug 'island', got %q", got.Spec.Selector["ark.watteel.com/map"])
	}
}

func TestEnsureServiceHonorsExternalTrafficPolicyOptIn(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: arkv1.ArkClusterSpec{
			Service: arkv1.ServiceSpec{
				Type:                  corev1.ServiceTypeLoadBalancer,
				GamePortStart:         7777,
				RconPortStart:         27020,
				ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
			},
		},
	}
	c := newFake(t).Build()
	if err := EnsureService(context.Background(), c, cluster, "TheIsland_WP", 0); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Service{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "c-island", Namespace: "ns"}, got)
	if got.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal {
		t.Errorf("opt-in not honored: %s", got.Spec.ExternalTrafficPolicy)
	}
}

func TestEnsureServiceWithPinnedLBIP(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: arkv1.ArkClusterSpec{
			Service: arkv1.ServiceSpec{
				Type:            corev1.ServiceTypeLoadBalancer,
				GamePortStart:   7777,
				RconPortStart:   27020,
				LoadBalancerIPs: []string{"192.168.10.210"},
			},
		},
	}
	c := newFake(t).Build()
	if err := EnsureService(context.Background(), c, cluster, "TheIsland_WP", 0); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Service{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "c-island", Namespace: "ns"}, got)
	if got.Spec.LoadBalancerIP != "192.168.10.210" {
		t.Errorf("lbIP = %q", got.Spec.LoadBalancerIP)
	}
}

func TestEnsureServiceSecondMapPortsIncrement(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: arkv1.ArkClusterSpec{
			Service: arkv1.ServiceSpec{
				Type: corev1.ServiceTypeLoadBalancer, GamePortStart: 7777, RconPortStart: 27020,
			},
		},
	}
	c := newFake(t).Build()
	_ = EnsureService(context.Background(), c, cluster, "ScorchedEarth_WP", 1)
	got := &corev1.Service{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "c-scorched-earth", Namespace: "ns"}, got)
	if got.Spec.Ports[0].Port != 7778 {
		t.Errorf("expected game port 7778 for index 1, got %d", got.Spec.Ports[0].Port)
	}
}
