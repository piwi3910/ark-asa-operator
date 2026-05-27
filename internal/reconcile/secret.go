package reconcile

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// SecretsName returns the per-cluster Secret holding adminPassword and any
// other cluster-scoped credentials.
func SecretsName(cluster string) string { return cluster + "-secrets" }

// EnsureAdminPasswordSecret ensures the cluster-secrets Secret exists with
// an adminPassword key. If the key already has a value (user-supplied), it is
// NEVER overwritten. If absent, generates 18 random bytes encoded as URL-safe
// base64 (yielding a 24-char password).
func EnsureAdminPasswordSecret(ctx context.Context, c client.Client, cluster *arkv1.ArkCluster) error {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: SecretsName(cluster.Name), Namespace: cluster.Namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, sec, func() error {
		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		if _, ok := sec.Data["adminPassword"]; !ok {
			buf := make([]byte, 18)
			if _, err := rand.Read(buf); err != nil {
				return fmt.Errorf("rand: %w", err)
			}
			sec.Data["adminPassword"] = []byte(base64.URLEncoding.EncodeToString(buf))
		}
		if sec.Labels == nil {
			sec.Labels = map[string]string{}
		}
		sec.Labels["ark.watteel.com/cluster"] = cluster.Name
		sec.Type = corev1.SecretTypeOpaque
		return controllerutil.SetControllerReference(cluster, sec, c.Scheme())
	})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
