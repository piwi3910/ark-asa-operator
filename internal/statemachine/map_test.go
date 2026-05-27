package statemachine

import (
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
)

func TestAllowedTransitions(t *testing.T) {
	tests := []struct {
		from, to arkv1.MapPhase
		ok       bool
	}{
		// Identity always allowed.
		{arkv1.MapPhaseRunning, arkv1.MapPhaseRunning, true},

		// Forward path.
		{arkv1.MapPhasePending, arkv1.MapPhaseProvisioning, true},
		{arkv1.MapPhaseProvisioning, arkv1.MapPhaseInstallingActive, true},
		{arkv1.MapPhaseInstallingActive, arkv1.MapPhaseRunning, true},
		{arkv1.MapPhaseRunning, arkv1.MapPhaseInstallingInactive, true},
		{arkv1.MapPhaseInstallingInactive, arkv1.MapPhaseDrainingActive, true},
		{arkv1.MapPhaseDrainingActive, arkv1.MapPhaseSwapping, true},
		{arkv1.MapPhaseSwapping, arkv1.MapPhaseRunning, true},

		// Failure transitions allowed from each non-terminal state.
		{arkv1.MapPhaseInstallingActive, arkv1.MapPhaseFailed, true},
		{arkv1.MapPhaseRunning, arkv1.MapPhaseFailed, true},

		// Disallowed.
		{arkv1.MapPhasePending, arkv1.MapPhaseRunning, false},
		{arkv1.MapPhaseRunning, arkv1.MapPhasePending, false},
		{arkv1.MapPhaseFailed, arkv1.MapPhaseRunning, false},
	}
	for _, tc := range tests {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			if got := AllowedTransition(tc.from, tc.to); got != tc.ok {
				t.Errorf("AllowedTransition(%s,%s)=%v want %v", tc.from, tc.to, got, tc.ok)
			}
		})
	}
}

func TestTransitionAppliesAndErrors(t *testing.T) {
	st := &arkv1.MapStatus{Phase: arkv1.MapPhasePending}
	if err := Transition(st, arkv1.MapPhaseProvisioning); err != nil {
		t.Fatalf("legal transition errored: %v", err)
	}
	if st.Phase != arkv1.MapPhaseProvisioning {
		t.Errorf("phase not updated: %s", st.Phase)
	}
	if err := Transition(st, arkv1.MapPhaseFailed); err != nil {
		t.Errorf("provisioning→failed should be allowed: %v", err)
	}
	if err := Transition(st, arkv1.MapPhaseRunning); err == nil {
		t.Error("Failed→Running must be rejected")
	}
}
