package reconcile

import (
	"context"
	"fmt"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/ark"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// ServiceName returns the per-map Service name (cluster-slug).
func ServiceName(cluster, mapID string) string {
	return fmt.Sprintf("%s-%s", cluster, ark.MapSlug(mapID))
}

// EnsureService creates the per-map Service. Each map's index in the cluster
// spec determines its port allocation (gamePortStart + i, rconPortStart + i).
// Amendment G: externalTrafficPolicy defaults to Cluster unless spec opts into Local.
func EnsureService(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster, mapID string, mapIndex int) error {
	slug := ark.MapSlug(mapID)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ServiceName(cluster.Name, mapID),
			Namespace: cluster.Namespace,
		},
	}
	gamePort := ark.GamePort(cluster.Spec.Service.GamePortStart, mapIndex)
	rconPort := ark.RconPort(cluster.Spec.Service.RconPortStart, mapIndex)

	_, err := controllerutil.CreateOrUpdate(ctx, c, svc, func() error {
		svcType := cluster.Spec.Service.Type
		if svcType == "" {
			svcType = corev1.ServiceTypeLoadBalancer
		}
		svc.Spec.Type = svcType
		svc.Spec.Selector = map[string]string{
			"ark.watteel.com/cluster": cluster.Name,
			"ark.watteel.com/map":     slug,
			"ark.watteel.com/role":    "server",
		}
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "game", Port: gamePort, TargetPort: intstr.FromInt32(gamePort), Protocol: corev1.ProtocolUDP},
			{Name: "rcon", Port: rconPort, TargetPort: intstr.FromInt32(rconPort), Protocol: corev1.ProtocolTCP},
		}
		if svcType == corev1.ServiceTypeLoadBalancer {
			// Amendment G: default to Cluster instead of Local.
			etp := cluster.Spec.Service.ExternalTrafficPolicy
			if etp == "" {
				etp = corev1.ServiceExternalTrafficPolicyCluster
			}
			svc.Spec.ExternalTrafficPolicy = etp
			if mapIndex < len(cluster.Spec.Service.LoadBalancerIPs) {
				svc.Spec.LoadBalancerIP = cluster.Spec.Service.LoadBalancerIPs[mapIndex]
			}
		}
		if svc.Labels == nil {
			svc.Labels = map[string]string{}
		}
		svc.Labels["ark.watteel.com/cluster"] = cluster.Name
		svc.Labels["ark.watteel.com/map"] = slug
		return controllerutil.SetControllerReference(cluster, svc, c.Scheme())
	})
	return err
}
