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
	existing := &arkv1.ModStatus{Tracked: []arkv1.TrackedMod{
		{ID: 1, InstalledFileID: 100, LatestFileID: 100},
	}}
	info := map[int64]curseforge.ModInfo{
		1: {ID: 1, Slug: "x", LatestFileID: 200, LatestVersion: "v2"},
	}
	got, changed := mergeModStatus(existing, info, now)
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
