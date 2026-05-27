package ark

import (
	"fmt"
	"strconv"
	"strings"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
)

// SessionName resolves the sessionNameFormat template with {cluster} and
// {map} substitutions. If friendlyMap is empty, falls back to the raw mapID.
func SessionName(format, cluster, mapID, friendlyMap string) string {
	if friendlyMap == "" {
		friendlyMap = mapID
	}
	out := strings.ReplaceAll(format, "{cluster}", cluster)
	out = strings.ReplaceAll(out, "{map}", friendlyMap)
	return out
}

// ComposeExtraParams returns the ?-prefixed launch params string ARK appends
// to the map URL (e.g. "?AdminLogging?serverPVE"). Empty slice yields "".
func ComposeExtraParams(gs arkv1.GlobalSettings) string {
	if len(gs.ExtraParams) == 0 {
		return ""
	}
	return "?" + strings.Join(gs.ExtraParams, "?")
}

// ComposeExtraOptions returns the space-separated -OptionName flags string
// (e.g. "-ForceAllowCaveFlyers -ServerUseEventColors").
func ComposeExtraOptions(gs arkv1.GlobalSettings) string {
	if len(gs.ExtraOptions) == 0 {
		return ""
	}
	out := make([]string, len(gs.ExtraOptions))
	for i, o := range gs.ExtraOptions {
		out[i] = "-" + o
	}
	return strings.Join(out, " ")
}

// ExtraFlagsInput aggregates everything needed to build the EXTRA_FLAGS env
// var passed to the sknnr/ark-ascended-server image. ExtraOptions is the
// pre-formatted "-Foo -Bar" string from ComposeExtraOptions.
type ExtraFlagsInput struct {
	ClusterDir       string
	ClusterID        string
	Mods             []int64
	BattlEyeEnabled  bool
	AllowedPlatforms []string
	ExtraOptions     string
}

// ExtraFlags builds the EXTRA_FLAGS env var value (space-separated -Flag style).
// Always emits -ClusterDirOverride, -clusterid, -NoTransferFromFiltering, and
// -NoBattlEye (unless BattlEyeEnabled is true). Mods, platforms, and extra
// options are appended only when present.
func ExtraFlags(in ExtraFlagsInput) string {
	parts := []string{
		fmt.Sprintf("-ClusterDirOverride=%s", in.ClusterDir),
		fmt.Sprintf("-clusterid=%s", in.ClusterID),
	}
	if len(in.Mods) > 0 {
		ids := make([]string, len(in.Mods))
		for i, m := range in.Mods {
			ids[i] = strconv.FormatInt(m, 10)
		}
		parts = append(parts, "-mods="+strings.Join(ids, ","))
	}
	if len(in.AllowedPlatforms) > 0 {
		parts = append(parts, "-ServerPlatform="+strings.Join(in.AllowedPlatforms, "+"))
	}
	if in.ExtraOptions != "" {
		parts = append(parts, in.ExtraOptions)
	}
	if !in.BattlEyeEnabled {
		parts = append(parts, "-NoBattlEye")
	}
	parts = append(parts, "-NoTransferFromFiltering")
	return strings.Join(parts, " ")
}

// ExtraSettings builds the ?-prefixed EXTRA_SETTINGS env var passed via the
// map URL. MaxPlayers + RCON config + any per-cluster extra params.
func ExtraSettings(gs arkv1.GlobalSettings, rconPort int32) string {
	parts := []string{
		fmt.Sprintf("?MaxPlayers=%d", gs.MaxPlayers),
		"?RCONEnabled=True",
		fmt.Sprintf("?RCONPort=%d", rconPort),
	}
	parts = append(parts, ComposeExtraParams(gs))
	return strings.Join(parts, "")
}
