// Package curseforge implements the slice of the CurseForge v1 API the
// ark-asa-operator needs: POST /v1/mods to fetch a set of mods (with their
// latest file IDs).
//
// Used by ModUpdateController to detect mod-version changes and trigger
// blue/green rolls on affected ArkClusters.
package curseforge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type ModInfo struct {
	ID            int64
	Slug          string
	LatestFileID  int64
	LatestVersion string
}

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func NewClient(baseURL, apiKey string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{baseURL: baseURL, apiKey: apiKey, http: hc}
}

// modsAPIResponse matches CurseForge's POST /v1/mods response (the slice we use).
type modsAPIResponse struct {
	Data []struct {
		ID    int64  `json:"id"`
		Slug  string `json:"slug"`
		Files []struct {
			ID          int64  `json:"id"`
			DisplayName string `json:"displayName"`
			IsLatest    bool   `json:"isLatestFile"`
		} `json:"latestFiles"`
	} `json:"data"`
}

// GetFiles fetches the latest-file info for the given mod IDs.
func (c *Client) GetFiles(ctx context.Context, ids []int64) (map[int64]ModInfo, error) {
	body, err := json.Marshal(map[string]any{"modIds": ids})
	if err != nil {
		return nil, fmt.Errorf("marshal modIds: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v1/mods", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("curseforge: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("curseforge status %d", resp.StatusCode)
	}
	var out modsAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	result := map[int64]ModInfo{}
	for _, m := range out.Data {
		var latestID int64
		var latestName string
		for _, f := range m.Files {
			if f.IsLatest {
				latestID = f.ID
				latestName = f.DisplayName
			}
		}
		result[m.ID] = ModInfo{ID: m.ID, Slug: m.Slug, LatestFileID: latestID, LatestVersion: latestName}
	}
	return result, nil
}
