package musicdb

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

// scrobble mirrors one element of a day file. The image arrays are kept raw
// because they are stored as JSON text rather than read as structure; see
// canonicalJSON for why they still cannot be copied through byte for byte.
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

// dayFiles lists the day files across dirs in date order, which is also the
// order rows are inserted in, so ids stay stable from one build to the next.
//
// The archive is split into a settled half and a pending tail, and a date must
// live in exactly one of them: a day read twice would count every one of its
// listens twice, which no later check would catch.
func dayFiles(dirs []string) ([]string, error) {
	var paths []string
	for _, dir := range dirs {
		found, err := filepath.Glob(filepath.Join(dir, "*.json"))
		if err != nil {
			return nil, err
		}
		paths = append(paths, found...)
	}
	// By date rather than by path, so the two halves interleave correctly.
	slices.SortFunc(paths, func(a, b string) int {
		return cmp.Compare(filepath.Base(a), filepath.Base(b))
	})

	for i := 1; i < len(paths); i++ {
		if filepath.Base(paths[i]) == filepath.Base(paths[i-1]) {
			return nil, fmt.Errorf("%s and %s are the same day", paths[i-1], paths[i])
		}
	}
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
		e.Image = canonicalJSON(e.Image)
		e.Artist.Image = canonicalJSON(e.Artist.Image)
		day = append(day, *e)
	}
	return day, nil
}

// canonicalJSON re-encodes a raw value so two scrobbles describing the same artist
// collapse to one row however their day file was written. The same image list
// reaches the archive in more than one spelling: the API escapes the slashes in
// its URLs and the jq-based scraper that wrote most of the archive did not, and
// day files differ in spacing and key order too. All of that is one value after
// a round trip, and none of it is after a byte-level compaction.
func canonicalJSON(raw json.RawMessage) json.RawMessage {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return raw
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Match the archive, which holds raw ampersands and angle brackets.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return raw
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
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
