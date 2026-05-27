/*
Copyright 2026 Pascal Watteel.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	arkv1alpha1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
)

var _ = Describe("ArkClusterReconciler", func() {
	const ns = "default"

	It("creates PVCs and a Service for a 1-map cluster", func() {
		ac := &arkv1alpha1.ArkCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test-1", Namespace: ns},
			Spec: arkv1alpha1.ArkClusterSpec{
				ClusterID: "test-1",
				Maps:      []arkv1alpha1.MapSpec{{ID: "TheIsland_WP"}},
				Service: arkv1alpha1.ServiceSpec{
					GamePortStart:         7777,
					RconPortStart:         27020,
					Type:                  corev1.ServiceTypeClusterIP,
					ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyCluster,
				},
				Storage: arkv1alpha1.StorageSpec{
					ServerPVCSize:       "1Gi",
					SavesPVCSize:        "1Gi",
					ClusterPVCSize:      "1Gi",
					ClusterStorageClass: "standard",
				},
			},
		}
		Expect(k8sClient.Create(ctx, ac)).To(Succeed())

		Eventually(func() error {
			svc := &corev1.Service{}
			return k8sClient.Get(ctx, types.NamespacedName{Name: "test-1-island", Namespace: ns}, svc)
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		Eventually(func() error {
			pvc := &corev1.PersistentVolumeClaim{}
			return k8sClient.Get(ctx, types.NamespacedName{Name: "test-1-island-saves", Namespace: ns}, pvc)
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	})

	It("creates resources for two maps simultaneously", func() {
		ac := &arkv1alpha1.ArkCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "twomap", Namespace: ns},
			Spec: arkv1alpha1.ArkClusterSpec{
				ClusterID: "twomap",
				Image:     "img:dev",
				Maps:      []arkv1alpha1.MapSpec{{ID: "TheIsland_WP"}, {ID: "ScorchedEarth_WP"}},
				Service:   arkv1alpha1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, GamePortStart: 7777, RconPortStart: 27020, ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyCluster},
				Storage:   arkv1alpha1.StorageSpec{ServerPVCSize: "1Gi", SavesPVCSize: "1Gi", ClusterPVCSize: "1Gi", ClusterStorageClass: "standard"},
			},
		}
		Expect(k8sClient.Create(ctx, ac)).To(Succeed())
		Eventually(func() error {
			s := &corev1.Service{}
			return k8sClient.Get(ctx, types.NamespacedName{Name: "twomap-scorched-earth", Namespace: ns}, s)
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
		Eventually(func() error {
			p := &corev1.PersistentVolumeClaim{}
			return k8sClient.Get(ctx, types.NamespacedName{Name: "twomap-scorched-earth-saves", Namespace: ns}, p)
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	})
})
