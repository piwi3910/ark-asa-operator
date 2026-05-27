package ark

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
)

// ModSetHash returns a stable, order-independent hex digest of a mod set.
// Used in pod-template hashing so reordering a mod list in the spec doesn't
// trigger a roll. Returns the same digest for nil and an empty slice.
func ModSetHash(mods []int64) string {
	cp := append([]int64(nil), mods...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	parts := make([]string, len(cp))
	for i, m := range cp {
		parts[i] = strconv.FormatInt(m, 10)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, ",")))
	return hex.EncodeToString(sum[:8]) // 16-char prefix, fits in a label value
}
