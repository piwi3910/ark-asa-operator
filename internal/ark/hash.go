package ark

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// PodTemplateHashInput is the set of inputs that, when changed, must trigger
// a pod recreate (blue/green roll in Phase 2+). SecretsRev and IniRev are the
// resourceVersions of referenced Secrets and ConfigMaps so that edits to
// those propagate via the hash (per Amendment B).
type PodTemplateHashInput struct {
	Image        string
	Mods         []int64
	GamePort     int32
	RconPort     int32
	SecretsRev   string
	IniRev       string
	ActiveVolume string
}

// PodTemplateHash returns a stable hex digest used as a label on server pods.
// Mismatch between desired and observed hash drives reconciliation: the
// controller deletes pods whose label doesn't match the desired hash and
// creates a fresh pod with the new hash.
func PodTemplateHash(in PodTemplateHashInput) string {
	s := fmt.Sprintf("%s|%s|%d|%d|%s|%s|%s",
		in.Image, ModSetHash(in.Mods), in.GamePort, in.RconPort,
		in.SecretsRev, in.IniRev, in.ActiveVolume)
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}
