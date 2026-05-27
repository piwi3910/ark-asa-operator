package controller

import (
	"testing"

	arkv1 "github.com/piwi3910/ark-asa-operator/api/v1alpha1"
	"github.com/piwi3910/ark-asa-operator/internal/curseforge"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCollectModIDsDedupesAndSorts(t *testing.T) {
	c := &arkv1.ArkCluster{
		Spec: arkv1.ArkClusterSpec{
			GlobalSettings: arkv1.GlobalSettings{Mods: []int64{3, 1}},
			Maps: []arkv1.MapSpec{
				{ID: "A", Mods: []int64{2, 1}},
				{ID: "B", Mods: []int64{4}},
			},
		},
	}
	got := collectModIDs(c)
	want := []int64{1, 2, 3, 4}
	if len(got) != 4 {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d]=%d want %d", i, got[i], want[i])
		}
	}
}

func TestMergeModStatusDetectsChange(t *testing.T) {
	now := metav1.Now()
	cluster := &arkv1.ArkCluster{
		Spec: arkv1.ArkClusterSpec{},
		Status: arkv1.ArkClusterStatus{
			Mods: &arkv1.ModStatus{Tracked: []arkv1.TrackedMod{
				{ID: 1, InstalledFileID: 100, LatestFileID: 100},
			}},
		},
	}
	info := map[int64]curseforge.ModInfo{
		1: {ID: 1, Slug: "x", LatestFileID: 200, LatestVersion: "v2"},
	}
	got, changed := mergeModStatus(cluster, info, now)
	if !changed {
		t.Error("expected changed=true when latestFileID differs")
	}
	if got[0].LatestFileID != 200 {
		t.Errorf("latestFileID not updated: %+v", got[0])
	}
	if got[0].InstalledFileID != 100 {
		t.Errorf("installedFileID should remain 100 until reconciler applies the roll, got %d", got[0].InstalledFileID)
	}
}

func TestMergeModStatusFirstSeenSetsInstalled(t *testing.T) {
	now := metav1.Now()
	info := map[int64]curseforge.ModInfo{
		42: {ID: 42, Slug: "y", LatestFileID: 999, LatestVersion: "v9"},
	}
	got, changed := mergeModStatus(nil, info, now)
	if changed {
		t.Error("expected changed=false on first sight")
	}
	if got[0].InstalledFileID != 999 || got[0].LatestFileID != 999 {
		t.Errorf("first-sight should pin Installed=Latest, got %+v", got[0])
	}
}

func TestMergeModStatusComputesAffectedMaps(t *testing.T) {
	cluster := &arkv1.ArkCluster{
		Spec: arkv1.ArkClusterSpec{
			GlobalSettings: arkv1.GlobalSettings{Mods: []int64{927090}},
			Maps: []arkv1.MapSpec{
				{ID: "TheIsland_WP"},
				{ID: "ScorchedEarth_WP", Mods: []int64{1056780}},
			},
		},
	}
	info := map[int64]curseforge.ModInfo{
		927090:  {ID: 927090, LatestFileID: 100},
		1056780: {ID: 1056780, LatestFileID: 200},
	}
	got, _ := mergeModStatus(cluster, info, metav1.Now())
	byID := map[int64]arkv1.TrackedMod{}
	for _, tm := range got {
		byID[tm.ID] = tm
	}
	if want := []string{"TheIsland_WP"}; !equalStrings(byID[927090].AffectedMaps, want) {
		t.Errorf("927090 AffectedMaps = %+v, want %+v", byID[927090].AffectedMaps, want)
	}
	if want := []string{"ScorchedEarth_WP"}; !equalStrings(byID[1056780].AffectedMaps, want) {
		t.Errorf("1056780 AffectedMaps = %+v, want %+v", byID[1056780].AffectedMaps, want)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHashTrackedStable(t *testing.T) {
	t1 := []arkv1.TrackedMod{{ID: 1, LatestFileID: 100}, {ID: 2, LatestFileID: 200}}
	t2 := []arkv1.TrackedMod{{ID: 1, LatestFileID: 100}, {ID: 2, LatestFileID: 200}}
	if hashTracked(t1) != hashTracked(t2) {
		t.Error("hashTracked not stable")
	}
	t3 := []arkv1.TrackedMod{{ID: 1, LatestFileID: 100}, {ID: 2, LatestFileID: 999}}
	if hashTracked(t1) == hashTracked(t3) {
		t.Error("hash should change when file IDs differ")
	}
}
