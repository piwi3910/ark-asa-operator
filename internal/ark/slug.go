package ark

import (
	"regexp"
	"strings"
)

// MapSlug returns a DNS-1123-compatible slug for a given ARK map ID.
// Known canonical maps are mapped to friendly short slugs; everything else
// falls back to lowercase + strip _WP suffix + underscore-to-dash + collapse
// non-alphanumeric runs.
//
// Used wherever a map ID appears in a Kubernetes resource name (Pod, Service,
// PVC, ConfigMap, Job, etc.). Resource names must be DNS-1123 ([a-z0-9-]),
// but raw ARK map IDs like "TheIsland_WP" contain uppercase letters and
// underscores. Pass the raw map ID to env vars and ARK flags; pass MapSlug
// output to anything K8s API.
func MapSlug(mapID string) string {
	switch mapID {
	case "TheIsland_WP":
		return "island"
	case "ScorchedEarth_WP":
		return "scorched-earth"
	case "Aberration_WP":
		return "aberration"
	case "Extinction_WP":
		return "extinction"
	case "TheCenter_WP":
		return "the-center"
	case "Astraeos_WP":
		return "astraeos"
	case "BobsMissions_WP":
		return "club-ark"
	}
	s := strings.ToLower(mapID)
	s = strings.TrimSuffix(s, "_wp")
	s = strings.ReplaceAll(s, "_", "-")
	s = nonAlphanumDashRun.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	return s
}

var nonAlphanumDashRun = regexp.MustCompile(`[^a-z0-9-]+`)
