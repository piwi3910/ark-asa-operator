package curseforge

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/piwi3910/ark-asa-operator/internal/curseforge/fake"
)

func TestGetFiles(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(gotBody)) // re-set for the fake to read
		fake.Handler(map[int64]fake.Mod{
			927090: {Slug: "structures-plus", LatestFileID: 4912100, LatestVersion: "5.5.0"},
		})(w, r)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "api-key-stub", nil)
	got, err := c.GetFiles(context.Background(), []int64{927090})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(gotBody, []byte(`"modIds"`)) || !bytes.Contains(gotBody, []byte("927090")) {
		t.Errorf("request body missing modIds; got %s", gotBody)
	}
	if len(got) != 1 || got[927090].LatestFileID != 4912100 {
		t.Errorf("unexpected: %+v", got)
	}
	if got[927090].Slug != "structures-plus" {
		t.Errorf("slug missing: %+v", got[927090])
	}
}

func TestGetFiles500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "key", nil)
	_, err := c.GetFiles(context.Background(), []int64{1})
	if err == nil {
		t.Error("expected error on 500")
	}
}
