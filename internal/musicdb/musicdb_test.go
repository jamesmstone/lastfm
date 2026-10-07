package musicdb

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func build(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "music.db")
	// Two dirs, as a real archive has: the settled days and the pending tail.
	stats, err := Build([]string{"testdata/days", "testdata/pending"}, path)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// Six scrobbles across the four days, two of them now-playing entries that
	// are not listens.
	want := Stats{Days: 4, Listens: 4, Tracks: 3, Albums: 3, Artists: 3}
	if stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestBuildTracks(t *testing.T) {
	db := build(t)

	// One plays twice: 120s before Two started, then a gap too long to measure.
	// Three's only dated listen is the last in the archive, so it has nothing
	// to measure against. Four is never scrobbled with a date at all.
	want := []struct {
		name     string
		plays    int64
		duration sql.NullFloat64
		total    sql.NullFloat64
	}{
		{"One", 2, nf(120), nf(240)},
		{"Two", 1, nf(60), nf(60)},
		{"Three", 1, sql.NullFloat64{}, sql.NullFloat64{}},
	}

	rows, err := db.Query(`SELECT name, plays, avg_estimated_duration, total_estimated_listen_time FROM tracks ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got int
	for rows.Next() {
		var name string
		var plays int64
		var duration, total sql.NullFloat64
		if err := rows.Scan(&name, &plays, &duration, &total); err != nil {
			t.Fatal(err)
		}
		if got >= len(want) {
			t.Fatalf("more tracks than expected, got %q", name)
		}
		w := want[got]
		if name != w.name || plays != w.plays || duration != w.duration || total != w.total {
			t.Errorf("track %d = %q plays=%d duration=%v total=%v, want %q plays=%d duration=%v total=%v",
				got, name, plays, duration, total, w.name, w.plays, w.duration, w.total)
		}
		got++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got != len(want) {
		t.Errorf("got %d tracks, want %d", got, len(want))
	}
}

func TestBuildRollsPlaysUp(t *testing.T) {
	// Third holds only a now-playing scrobble, so no track of it survives and
	// its play count stays unknown rather than becoming zero.
	for _, tc := range []struct {
		query string
		want  map[string]sql.NullInt64
	}{
		{
			`SELECT name, plays FROM albums`,
			map[string]sql.NullInt64{"First": ni(3), "Second": ni(1), "Third": {}},
		},
		{
			`SELECT name, plays FROM artists`,
			map[string]sql.NullInt64{"X": ni(3), "Y": ni(1), "Z": {}},
		},
	} {
		db := build(t)
		rows, err := db.Query(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]sql.NullInt64{}
		for rows.Next() {
			var name string
			var plays sql.NullInt64
			if err := rows.Scan(&name, &plays); err != nil {
				t.Fatal(err)
			}
			got[name] = plays
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(got) != len(tc.want) {
			t.Errorf("%s returned %d rows, want %d", tc.query, len(got), len(tc.want))
		}
		for name, want := range tc.want {
			if got[name] != want {
				t.Errorf("%s: %s plays = %v, want %v", tc.query, name, got[name], want)
			}
		}
	}
}

func TestBuildNormalises(t *testing.T) {
	db := build(t)

	var albums int
	if err := db.QueryRow(`SELECT count(*) FROM albums a JOIN artists ar ON a.artists_id = ar.id WHERE ar.name = 'X'`).Scan(&albums); err != nil {
		t.Fatal(err)
	}
	if albums != 1 {
		t.Errorf("artist X has %d albums, want 1", albums)
	}

	var image string
	if err := db.QueryRow(`SELECT image FROM artists WHERE name = 'X'`).Scan(&image); err != nil {
		t.Fatal(err)
	}
	// Re-encoded rather than copied through, which is what collapses the
	// spellings the archive holds; that also settles the key order.
	if want := `[{"#text":"x.png","size":"small"}]`; image != want {
		t.Errorf("artist image = %s, want %s", image, want)
	}
}

func TestBuildDropsNowPlaying(t *testing.T) {
	db := build(t)

	for _, tc := range []struct {
		what  string
		query string
	}{
		{"undated", `SELECT count(*) FROM listens WHERE date_uts IS NULL`},
		{"flagged", `SELECT count(*) FROM listens WHERE [@attr_nowplaying] IS NOT NULL`},
		{"Four in tracks", `SELECT count(*) FROM tracks WHERE name = 'Four'`},
	} {
		var got int
		if err := db.QueryRow(tc.query).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != 0 {
			t.Errorf("%s = %d, want 0", tc.what, got)
		}
	}

	// Every surviving listen is dated, so none of them drop out of the view the
	// way the undated ones used to.
	var details int
	if err := db.QueryRow(`SELECT count(*) FROM listen_details`).Scan(&details); err != nil {
		t.Fatal(err)
	}
	if details != 4 {
		t.Errorf("listen_details has %d rows, want 4", details)
	}
}

func TestBuildStoresDateUTSAsANumber(t *testing.T) {
	db := build(t)

	var kinds string
	if err := db.QueryRow(`SELECT group_concat(DISTINCT typeof(date_uts)) FROM listens`).Scan(&kinds); err != nil {
		t.Fatal(err)
	}
	if kinds != "integer" {
		t.Errorf("date_uts storage classes = %q, want integer", kinds)
	}
}

func TestBuildOrdersDateUTSNumerically(t *testing.T) {
	// 999 sorts after 1000 as text, so the column being numeric is what stops
	// the older play being read as the most recent one.
	dir := t.TempDir()
	for name, uts := range map[string]string{"2020-01-01.json": "1000", "2020-01-02.json": "999"} {
		play := `[{"artist":{"url":"u","name":"X","image":[],"mbid":""},"date":{"uts":"` + uts +
			`","#text":"t"},"mbid":"t1","name":"One","image":[],"url":"u","streamable":"0",` +
			`"album":{"mbid":"","#text":"A"},"loved":"0"}]`
		if err := os.WriteFile(filepath.Join(dir, name), []byte(play), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(t.TempDir(), "music.db")
	if _, err := Build([]string{dir}, path); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var latest int64
	if err := db.QueryRow(`SELECT date_uts FROM listens ORDER BY date_uts DESC LIMIT 1`).Scan(&latest); err != nil {
		t.Fatal(err)
	}
	if latest != 1000 {
		t.Errorf("latest date_uts = %d, want 1000", latest)
	}
}

func TestBuildRejectsAnUnparseableDate(t *testing.T) {
	dir := t.TempDir()
	play := `[{"artist":{"url":"u","name":"X","image":[],"mbid":""},"date":{"uts":"later",` +
		`"#text":"t"},"mbid":"t1","name":"One","image":[],"url":"u","streamable":"0",` +
		`"album":{"mbid":"","#text":"A"},"loved":"0"}]`
	if err := os.WriteFile(filepath.Join(dir, "2020-01-01.json"), []byte(play), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Build([]string{dir}, filepath.Join(t.TempDir(), "music.db"))
	if err == nil {
		t.Fatal("want an error when date.uts is not a number")
	}
	// Naming the day and the value is what makes a bad scrape findable.
	for _, want := range []string{"2020-01-01.json", `"later"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("got %v, want it to mention %s", err, want)
		}
	}
}

func TestBuildIndexesNamesForSearch(t *testing.T) {
	db := build(t)

	for table, query := range map[string]string{
		"tracks":  "Three",
		"albums":  "Second",
		"artists": "Y",
	} {
		var name string
		q := `SELECT name FROM ` + table + `_fts WHERE ` + table + `_fts MATCH ?`
		if err := db.QueryRow(q, query).Scan(&name); err != nil {
			t.Fatalf("%s_fts match %q: %v", table, query, err)
		}
		if name != query {
			t.Errorf("%s_fts match %q = %q", table, query, name)
		}
	}
}

func nf(f float64) sql.NullFloat64 { return sql.NullFloat64{Float64: f, Valid: true} }
func ni(i int64) sql.NullInt64     { return sql.NullInt64{Int64: i, Valid: true} }

func TestBuildRejectsADayInBothHalves(t *testing.T) {
	settled, pending := t.TempDir(), t.TempDir()
	for _, dir := range []string{settled, pending} {
		if err := os.WriteFile(filepath.Join(dir, "2020-01-01.json"), []byte("[]"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	_, err := Build([]string{settled, pending}, filepath.Join(t.TempDir(), "music.db"))
	if err == nil {
		t.Fatal("want an error when a day is settled and pending at once")
	}
	if !strings.Contains(err.Error(), "same day") {
		t.Errorf("got %v, want it to name the duplicated day", err)
	}
}

func TestBuildCollapsesEquivalentJSON(t *testing.T) {
	// The API escapes the forward slashes in its URLs and the jq-based scraper
	// that wrote most of the archive did not, so the same artist reaches the
	// build in two spellings. They have to be one row, or every artist, album
	// and track scraped on both sides of that change is counted twice.
	play := func(uts, image string) string {
		return `{"artist":{"url":"https://last.fm/X","name":"X","image":` + image +
			`,"mbid":""},"date":{"uts":"` + uts + `","#text":"t"},"mbid":"t1","name":"One","image":` +
			image + `,"url":"https://last.fm/X/_/One","streamable":"0","album":{"mbid":"","#text":"A"},"loved":"0"}`
	}
	plain := `[{"size":"small","#text":"https://img/x.png"}]`
	escaped := strings.ReplaceAll(`[{"#text":"https://img/x.png","size":"small"}]`, "/", `\/`)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "2020-01-01.json"),
		[]byte("["+play("1000", plain)+"]"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "2020-01-02.json"),
		[]byte("["+play("2000", escaped)+"]"), 0o644); err != nil {
		t.Fatal(err)
	}

	stats, err := Build([]string{dir}, filepath.Join(t.TempDir(), "music.db"))
	if err != nil {
		t.Fatal(err)
	}
	want := Stats{Days: 2, Listens: 2, Tracks: 1, Albums: 1, Artists: 1}
	if stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
}
