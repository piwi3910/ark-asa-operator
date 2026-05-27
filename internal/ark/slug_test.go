package ark

import "testing"

func TestMapSlug(t *testing.T) {
	tests := map[string]string{
		"TheIsland_WP":     "island",
		"ScorchedEarth_WP": "scorched-earth",
		"Aberration_WP":    "aberration",
		"Extinction_WP":    "extinction",
		"TheCenter_WP":     "the-center",
		"Astraeos_WP":      "astraeos",
		"BobsMissions_WP":  "club-ark",
		"My_Custom_Map_WP": "my-custom-map",
		"weirdMod_v2":      "weirdmod-v2",
	}
	for in, want := range tests {
		t.Run(in, func(t *testing.T) {
			if got := MapSlug(in); got != want {
				t.Errorf("MapSlug(%q) = %q, want %q", in, got, want)
			}
		})
	}
}
