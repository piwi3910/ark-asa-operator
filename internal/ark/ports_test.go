package ark

import "testing"

func TestGamePort(t *testing.T) {
	tests := []struct {
		name      string
		startPort int32
		index     int
		want      int32
	}{
		{"first map", 7777, 0, 7777},
		{"second map", 7777, 1, 7778},
		{"third map", 7777, 2, 7779},
		{"custom start", 8000, 5, 8005},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GamePort(tc.startPort, tc.index)
			if got != tc.want {
				t.Errorf("GamePort(%d, %d) = %d, want %d", tc.startPort, tc.index, got, tc.want)
			}
		})
	}
}

func TestRconPort(t *testing.T) {
	if got := RconPort(27020, 3); got != 27023 {
		t.Errorf("RconPort = %d, want 27023", got)
	}
}

func TestPortConflict(t *testing.T) {
	// 19244 maps starting at 7777 use ports 7777..27020 inclusive, hitting rconStart=27020.
	if !PortConflict(7777, 27020, 19244) {
		t.Error("expected conflict when game range hits rcon start")
	}
	// 19243 maps use 7777..27019, one short of rconStart — no conflict (half-open semantics).
	if PortConflict(7777, 27020, 19243) {
		t.Error("did not expect conflict when game range stops one short of rcon start")
	}
	if PortConflict(7777, 27020, 1) {
		t.Error("did not expect conflict for 1-map cluster")
	}
}
