// Package fake provides an in-process HTTP handler that mimics the slice of
// the CurseForge v1 API used by the operator. Used in tests.
package fake

import (
	"encoding/json"
	"io"
	"net/http"
)

type Mod struct {
	Slug          string
	LatestFileID  int64
	LatestVersion string
}

type modsRequest struct {
	ModIDs []int64 `json:"modIds"`
}

type fileEntry struct {
	ID          int64  `json:"id"`
	DisplayName string `json:"displayName"`
	IsLatest    bool   `json:"isLatestFile"`
}

type dataEntry struct {
	ID    int64       `json:"id"`
	Slug  string      `json:"slug"`
	Files []fileEntry `json:"latestFiles"`
}

type response struct {
	Data []dataEntry `json:"data"`
}

// Handler returns an http.HandlerFunc that mimics POST /v1/mods, returning the
// supplied mods. Unknown IDs are silently omitted (matches CurseForge behavior).
func Handler(mods map[int64]Mod) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Read request body for /v1/mods (best-effort; we don't actually filter by it
		// in the fake — tests pass the set of mods they want to see in the map).
		body, _ := io.ReadAll(r.Body)
		var req modsRequest
		_ = json.Unmarshal(body, &req)

		out := response{}
		if len(req.ModIDs) == 0 {
			for id, m := range mods {
				out.Data = append(out.Data, dataEntry{
					ID: id, Slug: m.Slug,
					Files: []fileEntry{{ID: m.LatestFileID, DisplayName: m.LatestVersion, IsLatest: true}},
				})
			}
		} else {
			for _, id := range req.ModIDs {
				m, ok := mods[id]
				if !ok {
					continue
				}
				out.Data = append(out.Data, dataEntry{
					ID: id, Slug: m.Slug,
					Files: []fileEntry{{ID: m.LatestFileID, DisplayName: m.LatestVersion, IsLatest: true}},
				})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
}
