package lastfm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"
)

func TestParsePage(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       string
		tracks     int
		totalPages int
	}{
		{"array", `{"recenttracks":{"track":[{"name":"a"},{"name":"b"}],"@attr":{"totalPages":"3"}}}`, 2, 3},
		// The API drops the array when a window holds exactly one track.
		{"single object", `{"recenttracks":{"track":{"name":"a"},"@attr":{"totalPages":"1"}}}`, 1, 1},
		{"empty array", `{"recenttracks":{"track":[],"@attr":{"totalPages":"0"}}}`, 0, 1},
		{"missing key", `{"recenttracks":{"@attr":{"totalPages":"1"}}}`, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := parsePage([]byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			if len(res.tracks) != tc.tracks {
				t.Errorf("got %d tracks, want %d", len(res.tracks), tc.tracks)
			}
			if res.totalPages != tc.totalPages {
				t.Errorf("got totalPages %d, want %d", res.totalPages, tc.totalPages)
			}
		})
	}
}

func TestParsePageAPIError(t *testing.T) {
	var e apiError
	body := []byte(`{"error":6,"message":"User not found"}`)
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatal(err)
	}
	if e.Code != 6 || e.retryable() {
		t.Errorf("error 6 = %+v, retryable %v", e, e.retryable())
	}
	if !(&apiError{Code: 29}).retryable() {
		t.Error("rate limit should be retryable")
	}
}

func TestStoreWrite(t *testing.T) {
	store := Store{Dir: t.TempDir()}
	day := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)

	if store.Has(day) {
		t.Fatal("empty store should not have the day")
	}
	tracks := []json.RawMessage{json.RawMessage(`{ "name" : "a" }`), json.RawMessage(`{"name":"b"}`)}
	if err := store.Write(day, tracks); err != nil {
		t.Fatal(err)
	}
	if !store.Has(day) {
		t.Fatal("day missing after write")
	}

	got, err := os.ReadFile(store.Path(day))
	if err != nil {
		t.Fatal(err)
	}
	if want := `[{"name":"a"},{"name":"b"}]`; string(got) != want {
		t.Errorf("wrote %s, want %s", got, want)
	}

	if err := store.Write(day, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(store.Path(day)); string(got) != "[]" {
		t.Errorf("empty day wrote %s, want []", got)
	}

	// The staging file must not be left behind for the build to trip over.
	entries, err := os.ReadDir(store.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("store holds %d files, want 1", len(entries))
	}
}

func TestDays(t *testing.T) {
	start := time.Date(2020, 2, 27, 0, 0, 0, 0, time.UTC)
	end := time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC)

	got := Days(start, end)
	want := []string{"2020-02-27", "2020-02-28", "2020-02-29", "2020-03-01"}
	if len(got) != len(want) {
		t.Fatalf("got %d days, want %d", len(got), len(want))
	}
	for i, w := range want {
		if d := got[i].Format(DateFormat); d != w {
			t.Errorf("day %d = %s, want %s", i, d, w)
		}
	}
}

func TestClientDayPaginates(t *testing.T) {
	day := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)

	var pages []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		for param, want := range map[string]string{
			"method":   "user.getrecenttracks",
			"user":     "someone",
			"api_key":  "secret",
			"format":   "json",
			"extended": "1",
			"limit":    "200",
			"from":     "1577923200",
			"to":       "1578009600",
		} {
			if got := q.Get(param); got != want {
				t.Errorf("%s = %q, want %q", param, got, want)
			}
		}
		pages = append(pages, q.Get("page"))
		fmt.Fprintf(w, `{"recenttracks":{"track":[{"page":%q}],"@attr":{"totalPages":"3"}}}`, q.Get("page"))
	}))
	defer srv.Close()

	client := &Client{HTTP: srv.Client(), APIKey: "secret", User: "someone", BaseURL: srv.URL}
	tracks, err := client.Day(context.Background(), day)
	if err != nil {
		t.Fatal(err)
	}

	if want := []string{"1", "2", "3"}; !slices.Equal(pages, want) {
		t.Errorf("requested pages %v, want %v", pages, want)
	}
	if len(tracks) != 3 {
		t.Fatalf("got %d tracks, want 3", len(tracks))
	}

	store := Store{Dir: t.TempDir()}
	if err := store.Write(day, tracks); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(store.Path(day))
	if err != nil {
		t.Fatal(err)
	}
	if want := `[{"page":"1"},{"page":"2"},{"page":"3"}]`; string(got) != want {
		t.Errorf("wrote %s, want %s", got, want)
	}
}

func TestClientDayRetriesThenFails(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer srv.Close()

	client := &Client{HTTP: srv.Client(), APIKey: "k", User: "u", BaseURL: srv.URL, Retries: 1}
	if _, err := client.Day(context.Background(), time.Now()); err == nil {
		t.Fatal("want an error after exhausting retries")
	}
	if calls != 2 {
		t.Errorf("made %d requests, want 2", calls)
	}
}

func TestClientDayStopsOnAPIError(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		fmt.Fprint(w, `{"error":10,"message":"Invalid API key"}`)
	}))
	defer srv.Close()

	client := &Client{HTTP: srv.Client(), APIKey: "k", User: "u", BaseURL: srv.URL, Retries: 3}
	_, err := client.Day(context.Background(), time.Now())
	var apiErr *apiError
	if !errors.As(err, &apiErr) || apiErr.Code != 10 {
		t.Fatalf("got %v, want a non-retryable API error", err)
	}
	if calls != 1 {
		t.Errorf("made %d requests, want 1", calls)
	}
}
