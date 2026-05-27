package reconcile

import (
	"context"
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSecretsName(t *testing.T) {
	if got := SecretsName("piwis-place"); got != "piwis-place-secrets" {
		t.Errorf("SecretsName = %q", got)
	}
}

func TestEnsureAdminPasswordSecretGenerates(t *testing.T) {
	cluster := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}}
	c := newFake(t).Build()
	if err := EnsureAdminPasswordSecret(context.Background(), c, cluster); err != nil {
		t.Fatal(err)
	}
	sec := &corev1.Secret{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "c-secrets", Namespace: "ns"}, sec); err != nil {
		t.Fatal(err)
	}
	if len(sec.Data["adminPassword"]) < 16 {
		t.Errorf("generated adminPassword too short: %d", len(sec.Data["adminPassword"]))
	}
}

func TestEnsureAdminPasswordSecretIdempotent(t *testing.T) {
	cluster := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}}
	c := newFake(t).Build()
	if err := EnsureAdminPasswordSecret(context.Background(), c, cluster); err != nil {
		t.Fatal(err)
	}
	first := &corev1.Secret{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "c-secrets", Namespace: "ns"}, first)

	if err := EnsureAdminPasswordSecret(context.Background(), c, cluster); err != nil {
		t.Fatal(err)
	}
	second := &corev1.Secret{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "c-secrets", Namespace: "ns"}, second)

	if string(first.Data["adminPassword"]) != string(second.Data["adminPassword"]) {
		t.Error("admin password must not regenerate on subsequent reconciles")
	}
}

func TestEnsureAdminPasswordSecretPreservesUserSetValue(t *testing.T) {
	cluster := &arkv1.ArkCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}}
	preExisting := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "c-secrets", Namespace: "ns"},
		Data:       map[string][]byte{"adminPassword": []byte("user-supplied")},
		Type:       corev1.SecretTypeOpaque,
	}
	c := newFake(t).WithObjects(preExisting).Build()
	if err := EnsureAdminPasswordSecret(context.Background(), c, cluster); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Secret{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "c-secrets", Namespace: "ns"}, got)
	if string(got.Data["adminPassword"]) != "user-supplied" {
		t.Errorf("user adminPassword overwritten: got %q", got.Data["adminPassword"])
	}
}
