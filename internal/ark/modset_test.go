package ark

import "testing"

func TestModSetHashStable(t *testing.T) {
	a := ModSetHash([]int64{927090, 1056780})
	b := ModSetHash([]int64{1056780, 927090}) // order doesn't matter
	if a != b {
		t.Errorf("ModSetHash not order-independent: %s vs %s", a, b)
	}
}

func TestModSetHashEmpty(t *testing.T) {
	if ModSetHash(nil) != ModSetHash([]int64{}) {
		t.Error("nil and empty should hash the same")
	}
}

func TestModSetHashDifferent(t *testing.T) {
	if ModSetHash([]int64{1}) == ModSetHash([]int64{2}) {
		t.Error("different mod sets should hash differently")
	}
}
