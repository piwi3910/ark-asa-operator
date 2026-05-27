/*
Copyright 2026 Pascal Watteel.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	arkv1alpha1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
)

func TestValidateAcceptsMultiMap(t *testing.T) {
	c := &arkv1alpha1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"},
		Spec: arkv1alpha1.ArkClusterSpec{
			ClusterID: "c",
			Maps:      []arkv1alpha1.MapSpec{{ID: "TheIsland_WP"}, {ID: "ScorchedEarth_WP"}},
			Service:   arkv1alpha1.ServiceSpec{GamePortStart: 7777, RconPortStart: 27020, Type: corev1.ServiceTypeLoadBalancer},
		},
	}
	v := &ArkClusterCustomValidator{}
	if _, err := v.ValidateCreate(context.Background(), c); err != nil {
		t.Errorf("multi-map should now be allowed in phase 3; got: %v", err)
	}
}

func TestValidateRejectsPortOverlap(t *testing.T) {
	c := &arkv1alpha1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"},
		Spec: arkv1alpha1.ArkClusterSpec{
			ClusterID: "c",
			Maps:      []arkv1alpha1.MapSpec{{ID: "TheIsland_WP"}},
			Service:   arkv1alpha1.ServiceSpec{GamePortStart: 27020, RconPortStart: 27020, Type: corev1.ServiceTypeLoadBalancer},
		},
	}
	v := &ArkClusterCustomValidator{}
	if _, err := v.ValidateCreate(context.Background(), c); err == nil {
		t.Error("expected port-overlap to be rejected")
	}
}

func TestValidateAcceptsValid(t *testing.T) {
	c := &arkv1alpha1.ArkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"},
		Spec: arkv1alpha1.ArkClusterSpec{
			ClusterID: "c",
			Maps:      []arkv1alpha1.MapSpec{{ID: "TheIsland_WP"}},
			Service:   arkv1alpha1.ServiceSpec{GamePortStart: 7777, RconPortStart: 27020, Type: corev1.ServiceTypeLoadBalancer},
		},
	}
	v := &ArkClusterCustomValidator{}
	if _, err := v.ValidateCreate(context.Background(), c); err != nil {
		t.Errorf("unexpected: %v", err)
	}
}

// Keep a minimal Ginkgo Describe so the kubebuilder-generated suite
// (webhook_suite_test.go) has something to attach to.
var _ = Describe("ArkCluster Webhook", func() {
	BeforeEach(func() {
		Expect(true).To(BeTrue())
	})
})
