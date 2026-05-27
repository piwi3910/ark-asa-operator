package ark

import (
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
)

func TestSessionName(t *testing.T) {
	tests := []struct {
		name, format, cluster, mapID, friendlyMap string
		want                                       string
	}{
		{"plain", "piwi’s place", "piwis-place", "TheIsland_WP", "The Island", "piwi’s place"},
		{"with cluster", "{cluster}", "piwis-place", "TheIsland_WP", "The Island", "piwis-place"},
		{"with map friendly", "{cluster} - {map}", "piwis-place", "TheIsland_WP", "The Island", "piwis-place - The Island"},
		{"with map raw fallback", "{map}", "x", "UnknownMap_WP", "", "UnknownMap_WP"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SessionName(tc.format, tc.cluster, tc.mapID, tc.friendlyMap)
			if got != tc.want {
				t.Errorf("SessionName = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestComposeExtraParams(t *testing.T) {
	gs := arkv1.GlobalSettings{
		ExtraParams: []string{"AdminLogging", "AllowFlyerCarryPvE"},
	}
	got := ComposeExtraParams(gs)
	want := "?AdminLogging?AllowFlyerCarryPvE"
	if got != want {
		t.Errorf("ComposeExtraParams = %q, want %q", got, want)
	}
}

func TestComposeExtraParamsEmpty(t *testing.T) {
	if got := ComposeExtraParams(arkv1.GlobalSettings{}); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestComposeExtraOptions(t *testing.T) {
	gs := arkv1.GlobalSettings{
		ExtraOptions: []string{"ForceAllowCaveFlyers", "ServerUseEventColors"},
	}
	got := ComposeExtraOptions(gs)
	want := "-ForceAllowCaveFlyers -ServerUseEventColors"
	if got != want {
		t.Errorf("ComposeExtraOptions = %q, want %q", got, want)
	}
}

func TestExtraFlagsBuilder(t *testing.T) {
	got := ExtraFlags(ExtraFlagsInput{
		ClusterDir:       "/srv/ark/cluster",
		ClusterID:        "piwis-place",
		Mods:             []int64{927090, 1056780},
		BattlEyeEnabled:  false,
		AllowedPlatforms: []string{"ALL"},
		ExtraOptions:     "-ForceAllowCaveFlyers",
	})
	want := "-ClusterDirOverride=/srv/ark/cluster -clusterid=piwis-place -mods=927090,1056780 -ServerPlatform=ALL -ForceAllowCaveFlyers -NoBattlEye -NoTransferFromFiltering"
	if got != want {
		t.Errorf("ExtraFlags = %q, want %q", got, want)
	}
}

func TestExtraFlagsMinimal(t *testing.T) {
	got := ExtraFlags(ExtraFlagsInput{
		ClusterDir: "/srv/ark/cluster",
		ClusterID:  "x",
	})
	// No mods, no platforms, no extras, BattlEye disabled → still emits the two essentials + -NoBattlEye + -NoTransferFromFiltering
	want := "-ClusterDirOverride=/srv/ark/cluster -clusterid=x -NoBattlEye -NoTransferFromFiltering"
	if got != want {
		t.Errorf("ExtraFlags minimal = %q, want %q", got, want)
	}
}

func TestExtraSettings(t *testing.T) {
	got := ExtraSettings(arkv1.GlobalSettings{
		MaxPlayers:  70,
		ExtraParams: []string{"AdminLogging"},
	}, 27020)
	want := "?MaxPlayers=70?RCONEnabled=True?RCONPort=27020?AdminLogging"
	if got != want {
		t.Errorf("ExtraSettings = %q, want %q", got, want)
	}
}
