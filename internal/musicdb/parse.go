package musicdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

// scrobble mirrors one element of a day file. The image arrays stay raw: they
// are stored as JSON text, so decoding them would only cost time.
type scrobble struct {
	Artist struct {
		URL   string          `json:"url"`
		Name  string          `json:"name"`
		Image json.RawMessage `json:"image"`
		MBID  string          `json:"mbid"`
	} `json:"artist"`
	Date *struct {
		UTS  string `json:"uts"`
		Text string `json:"#text"`
	} `json:"date"`
	MBID       string          `json:"mbid"`
	Name       string          `json:"name"`
	Image      json.RawMessage `json:"image"`
	URL        string          `json:"url"`
	Streamable string          `json:"streamable"`
	Album      struct {
		MBID string `json:"mbid"`
		Text string `json:"#text"`
	} `json:"album"`
	Attr *struct {
		NowPlaying string `json:"nowplaying"`
	} `json:"@attr"`
	Loved *string `json:"loved"`
}

// dayFiles lists the day files in date order, which is also the order rows are
// inserted in, so ids stay stable from one build to the next.
func dayFiles(dir string) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	slices.Sort(paths)
	return paths, nil
}

// eachDay decodes the day files in parallel but hands them to fn in the order
// given, so the normaliser stays single-threaded and deterministic. Results
// are only produced a window ahead of fn, which keeps the parsed JSON for the
// whole archive from piling up in memory at once.
func eachDay(paths []string, fn func(path string, day []scrobble) error) error {
	type result struct {
		day []scrobble
		err error
	}

	window := 2 * runtime.NumCPU()
	pending := make(chan chan result, window)
	go func() {
		defer close(pending)
		for _, path := range paths {
			slot := make(chan result, 1)
			pending <- slot
			go func() {
				day, err := parseDay(path)
				slot <- result{day, err}
			}()
		}
	}()

	for i := 0; ; i++ {
		slot, ok := <-pending
		if !ok {
			return nil
		}
		r := <-slot
		if r.err != nil {
			return fmt.Errorf("%s: %w", paths[i], r.err)
		}
		if err := fn(paths[i], r.day); err != nil {
			return err
		}
	}
}

func parseDay(path string) ([]scrobble, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// A day the API reports with no plays was historically written as `[null]`;
	// those entries carry no track and must not reach the normaliser.
	var entries []*scrobble
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, err
	}
	day := make([]scrobble, 0, len(entries))
	for _, e := range entries {
		if e == nil {
			continue
		}
		e.Image = compactJSON(e.Image)
		e.Artist.Image = compactJSON(e.Artist.Image)
		day = append(day, *e)
	}
	return day, nil
}

// compactJSON normalises a raw value so two scrobbles describing the same
// artist collapse to one row however their day files happen to be spaced.
func compactJSON(raw json.RawMessage) json.RawMessage {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return raw
	}
	return buf.Bytes()
}

func (s *scrobble) nowPlaying() *string {
	if s.Attr == nil {
		return nil
	}
	return &s.Attr.NowPlaying
}

func (s *scrobble) date() (uts, text *string) {
	if s.Date == nil {
		return nil, nil
	}
	return &s.Date.UTS, &s.Date.Text
}

// key joins fields into a map key. Day data never contains a NUL byte, so the
// separator cannot collide with field content.
func key(fields ...string) string {
	return strings.Join(fields, "\x00")
}
