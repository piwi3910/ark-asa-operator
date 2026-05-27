package ark

import "testing"

func TestPodTemplateHashStable(t *testing.T) {
	in := PodTemplateHashInput{
		Image:        "ghcr.io/sknnr/ark-ascended-server:1.2.3",
		Mods:         []int64{927090},
		GamePort:     7777,
		RconPort:     27020,
		SecretsRev:   "rv-7",
		IniRev:       "rv-3",
		ActiveVolume: "server-a",
	}
	a := PodTemplateHash(in)
	b := PodTemplateHash(in)
	if a != b {
		t.Errorf("hash not stable: %s vs %s", a, b)
	}
	in.Image = "ghcr.io/sknnr/ark-ascended-server:1.2.4"
	c := PodTemplateHash(in)
	if a == c {
		t.Error("changing image must change hash")
	}
}

func TestPodTemplateHashSensitivity(t *testing.T) {
	base := PodTemplateHashInput{
		Image: "img", Mods: []int64{1}, GamePort: 7777, RconPort: 27020,
		SecretsRev: "s1", IniRev: "i1", ActiveVolume: "server-a",
	}
	baseHash := PodTemplateHash(base)
	tests := []struct {
		name   string
		mutate func(*PodTemplateHashInput)
	}{
		{"mods", func(p *PodTemplateHashInput) { p.Mods = []int64{2} }},
		{"gamePort", func(p *PodTemplateHashInput) { p.GamePort = 7778 }},
		{"rconPort", func(p *PodTemplateHashInput) { p.RconPort = 27021 }},
		{"secretsRev", func(p *PodTemplateHashInput) { p.SecretsRev = "s2" }},
		{"iniRev", func(p *PodTemplateHashInput) { p.IniRev = "i2" }},
		{"activeVolume", func(p *PodTemplateHashInput) { p.ActiveVolume = "server-b" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			x := base
			tc.mutate(&x)
			if PodTemplateHash(x) == baseHash {
				t.Errorf("hash should change when %s changes", tc.name)
			}
		})
	}
}
