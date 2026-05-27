package statemachine

import (
	"fmt"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
)

// Transition updates a MapStatus's phase, returning an error if the
// transition is not in the allowed table. Identity transitions are no-ops.
func Transition(status *arkv1.MapStatus, to arkv1.MapPhase) error {
	if !AllowedTransition(status.Phase, to) {
		return fmt.Errorf("disallowed map transition: %s -> %s", status.Phase, to)
	}
	status.Phase = to
	return nil
}
