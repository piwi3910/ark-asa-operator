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
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	arkv1alpha1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/ark"
)

// nolint:unused
// log is for logging in this package.
var arkclusterlog = logf.Log.WithName("arkcluster-resource")

// phase1SingleMapEnforced gates the temporary "exactly one map" restriction.
// Flip to false in Phase 3 to allow multi-map clusters.
const phase1SingleMapEnforced = true

// SetupArkClusterWebhookWithManager registers the webhook for ArkCluster in the manager.
func SetupArkClusterWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &arkv1alpha1.ArkCluster{}).
		WithValidator(&ArkClusterCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-ark-watteel-com-v1alpha1-arkcluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=ark.watteel.com,resources=arkclusters,verbs=create;update,versions=v1alpha1,name=varkcluster-v1alpha1.kb.io,admissionReviewVersions=v1

// ArkClusterCustomValidator validates an ArkCluster on create/update.
type ArkClusterCustomValidator struct{}

// ValidateCreate implements webhook.CustomValidator.
func (v *ArkClusterCustomValidator) ValidateCreate(_ context.Context, obj *arkv1alpha1.ArkCluster) (admission.Warnings, error) {
	arkclusterlog.Info("validate create", "name", obj.GetName())
	return validate(obj)
}

// ValidateUpdate implements webhook.CustomValidator.
func (v *ArkClusterCustomValidator) ValidateUpdate(_ context.Context, _ *arkv1alpha1.ArkCluster, newObj *arkv1alpha1.ArkCluster) (admission.Warnings, error) {
	arkclusterlog.Info("validate update", "name", newObj.GetName())
	return validate(newObj)
}

// ValidateDelete implements webhook.CustomValidator.
func (v *ArkClusterCustomValidator) ValidateDelete(_ context.Context, _ *arkv1alpha1.ArkCluster) (admission.Warnings, error) {
	return nil, nil
}

func validate(c *arkv1alpha1.ArkCluster) (admission.Warnings, error) {
	if phase1SingleMapEnforced && len(c.Spec.Maps) != 1 {
		return nil, fmt.Errorf("phase 1 supports exactly one map; got %d", len(c.Spec.Maps))
	}
	if ark.PortConflict(c.Spec.Service.GamePortStart, c.Spec.Service.RconPortStart, len(c.Spec.Maps)) {
		return nil, fmt.Errorf("game port range overlaps RCON port (gamePortStart=%d, rconPortStart=%d, maps=%d)",
			c.Spec.Service.GamePortStart, c.Spec.Service.RconPortStart, len(c.Spec.Maps))
	}
	return nil, nil
}
