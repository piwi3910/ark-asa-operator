// Package statemachine declares the legal phase transitions for per-map
// reconciliation. Centralizing them here keeps reconciler control flow
// honest: anywhere we change MapStatus.Phase we go through Transition(),
// and any disallowed transition is rejected loudly rather than silently
// applied (which would mask a controller bug).
package statemachine

import arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"

// allowed lists explicit (from → to) transitions per map. Identity transitions
// (from == to) are always permitted; AllowedTransition handles that.
// Reset transitions (e.g., manual recovery from Failed) intentionally NOT here;
// they happen via explicit admin status surgery, not normal reconciliation.
var allowed = map[arkv1.MapPhase]map[arkv1.MapPhase]bool{
	"": {arkv1.MapPhasePending: true}, // fresh map status enters Pending first
	arkv1.MapPhasePending: {
		arkv1.MapPhaseProvisioning: true,
		arkv1.MapPhaseFailed:       true,
	},
	arkv1.MapPhaseProvisioning: {
		arkv1.MapPhaseInstallingActive: true,
		arkv1.MapPhaseFailed:           true,
	},
	arkv1.MapPhaseInstallingActive: {
		arkv1.MapPhaseRunning: true,
		arkv1.MapPhaseFailed:  true,
	},
	arkv1.MapPhaseRunning: {
		arkv1.MapPhaseInstallingInactive: true, // start of blue/green
		arkv1.MapPhaseDrainingActive:     true, // Recreate strategy goes here directly
		arkv1.MapPhaseFailed:             true,
	},
	arkv1.MapPhaseInstallingInactive: {
		arkv1.MapPhaseDrainingActive: true,
		arkv1.MapPhaseRunning:        true, // abort path: revert before draining
		arkv1.MapPhaseFailed:         true,
	},
	arkv1.MapPhaseDrainingActive: {
		arkv1.MapPhaseSwapping: true,
		arkv1.MapPhaseRunning:  true, // abort path: keep current active
		arkv1.MapPhaseFailed:   true,
	},
	arkv1.MapPhaseSwapping: {
		arkv1.MapPhaseRunning: true,
		arkv1.MapPhaseFailed:  true,
	},
	arkv1.MapPhaseFailed: {}, // terminal (user must manually clear)
}

// AllowedTransition reports whether moving from `from` to `to` is legal.
// Identity transitions are always allowed.
func AllowedTransition(from, to arkv1.MapPhase) bool {
	if from == to {
		return true
	}
	return allowed[from][to]
}
