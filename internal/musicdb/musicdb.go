// Package musicdb turns the scraped day files into the SQLite database the
// Datasette site serves.
//
// The shape of that database is inherited from the sqlite-utils pipeline this
// replaced: listens are normalised into tracks, albums and artists, each track
// carries a play count and a duration estimated from the gap to the next
// scrobble, and the names are searchable through FTS5.
package musicdb

import (
	"cmp"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
)

// maxGap is the longest gap between consecutive scrobbles that still counts as
// one track playing to its end; anything longer means the listening stopped.
const maxGap = 10 * 60

// Stats summarises a build, for logging.
type Stats struct {
	Days    int
	Listens int
	Tracks  int
	Albums  int
	Artists int
}

func (s Stats) String() string {
	return fmt.Sprintf("%d days, %d listens, %d tracks, %d albums, %d artists",
		s.Days, s.Listens, s.Tracks, s.Albums, s.Artists)
}

// Build reads every day file in dirs and writes a fresh database to path,
// replacing whatever was there.
func Build(dirs []string, path string) (Stats, error) {
	paths, err := dayFiles(dirs)
	if err != nil {
		return Stats{}, err
	}

	m := newModel()
	if err := eachDay(paths, func(_ string, day []scrobble) error {
		m.add(day)
		return nil
	}); err != nil {
		return Stats{}, err
	}
	m.summarise()

	if err := m.write(path); err != nil {
		return Stats{}, err
	}
	return Stats{
		Days:    len(paths),
		Listens: len(m.listens),
		Tracks:  m.liveTracks,
		Albums:  len(m.albums),
		Artists: len(m.artists),
	}, nil
}

type artist struct {
	name, url, image, mbid string
	plays                  sql.NullInt64
}

type album struct {
	name, mbid string
	artistID   int64
	plays      sql.NullInt64
}

type track struct {
	mbid, name, image, streamable, url string
	albumID                            int64
	plays                              int64
	duration                           sql.NullFloat64
}

type listen struct {
	trackID                         int64
	uts, utsText, loved, nowPlaying sql.NullString
}

// model accumulates the whole archive in memory. Deduplicating here rather
// than in SQL is what makes the build fast: the normalised tables are written
// once, instead of being carved out of a wide listens table afterwards.
type model struct {
	artists []artist
	albums  []album
	tracks  []track
	listens []listen

	artistIDs map[string]int64
	albumIDs  map[string]int64
	trackIDs  map[string]int64

	liveTracks int
}

func newModel() *model {
	return &model{
		artistIDs: make(map[string]int64),
		albumIDs:  make(map[string]int64),
		trackIDs:  make(map[string]int64),
	}
}

func (m *model) add(day []scrobble) {
	for i := range day {
		s := &day[i]
		trackID := m.trackID(s, m.albumID(s, m.artistID(s)))
		uts, utsText := s.date()
		m.listens = append(m.listens, listen{
			trackID:    trackID,
			uts:        nullString(uts),
			utsText:    nullString(utsText),
			loved:      nullString(s.Loved),
			nowPlaying: nullString(s.nowPlaying()),
		})
	}
}

func (m *model) artistID(s *scrobble) int64 {
	image := string(s.Artist.Image)
	k := key(s.Artist.Name, s.Artist.URL, image, s.Artist.MBID)
	if id, ok := m.artistIDs[k]; ok {
		return id
	}
	m.artists = append(m.artists, artist{
		name:  s.Artist.Name,
		url:   s.Artist.URL,
		image: image,
		mbid:  s.Artist.MBID,
	})
	id := int64(len(m.artists))
	m.artistIDs[k] = id
	return id
}

func (m *model) albumID(s *scrobble, artistID int64) int64 {
	k := key(s.Album.Text, s.Album.MBID, strconv.FormatInt(artistID, 10))
	if id, ok := m.albumIDs[k]; ok {
		return id
	}
	m.albums = append(m.albums, album{
		name:     s.Album.Text,
		mbid:     s.Album.MBID,
		artistID: artistID,
	})
	id := int64(len(m.albums))
	m.albumIDs[k] = id
	return id
}

func (m *model) trackID(s *scrobble, albumID int64) int64 {
	image := string(s.Image)
	k := key(s.MBID, s.Name, image, s.Streamable, s.URL, strconv.FormatInt(albumID, 10))
	if id, ok := m.trackIDs[k]; ok {
		return id
	}
	m.tracks = append(m.tracks, track{
		mbid:       s.MBID,
		name:       s.Name,
		image:      image,
		streamable: s.Streamable,
		url:        s.URL,
		albumID:    albumID,
	})
	id := int64(len(m.tracks))
	m.trackIDs[k] = id
	return id
}

// summarise fills in the per-track duration estimate and the play counts that
// roll up from tracks to albums to artists.
func (m *model) summarise() {
	type play struct {
		trackID int64
		at      int64
	}
	plays := make([]play, 0, len(m.listens))
	for _, l := range m.listens {
		if !l.uts.Valid {
			continue
		}
		at, err := strconv.ParseInt(l.uts.String, 10, 64)
		if err != nil {
			continue
		}
		plays = append(plays, play{l.trackID, at})
	}
	slices.SortStableFunc(plays, func(a, b play) int { return cmp.Compare(a.at, b.at) })

	// Index 0 is unused so that a track id indexes its own slot directly.
	sum := make([]float64, len(m.tracks)+1)
	measured := make([]int64, len(m.tracks)+1)
	counts := make([]int64, len(m.tracks)+1)
	for i, p := range plays {
		counts[p.trackID]++
		if i+1 == len(plays) {
			continue // nothing followed the last scrobble, so its length is unknown
		}
		if gap := plays[i+1].at - p.at; gap <= maxGap {
			sum[p.trackID] += float64(gap)
			measured[p.trackID]++
		}
	}

	albumPlays := make([]sql.NullInt64, len(m.albums)+1)
	for i := range m.tracks {
		t := &m.tracks[i]
		id := int64(i + 1)
		t.plays = counts[id]
		if t.plays == 0 {
			continue // only tracks with a dated listen reach the tracks table
		}
		m.liveTracks++
		if measured[id] > 0 {
			t.duration = sql.NullFloat64{Float64: sum[id] / float64(measured[id]), Valid: true}
		}
		p := &albumPlays[t.albumID]
		p.Int64 += t.plays
		p.Valid = true
	}

	artistPlays := make([]sql.NullInt64, len(m.artists)+1)
	for i := range m.albums {
		a := &m.albums[i]
		a.plays = albumPlays[i+1]
		if !a.plays.Valid {
			continue
		}
		p := &artistPlays[a.artistID]
		p.Int64 += a.plays.Int64
		p.Valid = true
	}
	for i := range m.artists {
		m.artists[i].plays = artistPlays[i+1]
	}
}

func nullString(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}
