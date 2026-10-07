package musicdb

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

// tracks.tracks_id duplicates tracks.id. It is a leftover of the join the
// sqlite-utils pipeline used to attach the duration estimate, kept so saved
// Datasette queries against it keep working.
//
// listens.@attr_nowplaying is kept for the same reason, but is always NULL now
// that now-playing entries are not recorded as listens at all.
const schema = `
CREATE TABLE [artists] (
  [id] INTEGER PRIMARY KEY,
  [url] TEXT,
  [name] TEXT,
  [image] TEXT,
  [mbid] TEXT,
  [plays] INTEGER
);
CREATE TABLE [albums] (
  [id] INTEGER PRIMARY KEY,
  [artists_id] INTEGER REFERENCES [artists]([id]),
  [mbid] TEXT,
  [name] TEXT,
  [plays] INTEGER
);
CREATE TABLE [tracks] (
  [id] INTEGER PRIMARY KEY,
  [albums_id] INTEGER REFERENCES [albums]([id]),
  [mbid] TEXT,
  [name] TEXT,
  [image] TEXT,
  [url] TEXT,
  [streamable] TEXT,
  [tracks_id] INTEGER,
  [avg_estimated_duration] FLOAT,
  [plays] INTEGER,
  [total_estimated_listen_time] FLOAT
);
CREATE TABLE [listens] (
  [tracks_id] INTEGER REFERENCES [tracks]([id]),
  [date_uts] INTEGER,
  [date_#text] TEXT,
  [loved] TEXT,
  [@attr_nowplaying] TEXT
);
`

// The unique indexes are the natural keys the rows were deduplicated on, so
// creating them also checks that the normalisation collapsed everything it
// should have.
const indexes = `
CREATE UNIQUE INDEX [idx_artists_name_url_image_mbid] ON [artists] ([name], [url], [image], [mbid]);
CREATE UNIQUE INDEX [idx_albums_name_mbid_artists_id] ON [albums] ([name], [mbid], [artists_id]);
CREATE INDEX [idx_albums_artists_id] ON [albums] ([artists_id]);
CREATE INDEX [idx_tracks_albums_id] ON [tracks] ([albums_id]);
CREATE INDEX [idx_listens_tracks_id] ON [listens] ([tracks_id]);
CREATE INDEX [idx_listens_date_uts] ON [listens] ([date_uts]);
`

const search = `
CREATE VIRTUAL TABLE [tracks_fts] USING FTS5 ([name], content=[tracks]);
INSERT INTO [tracks_fts] (rowid, [name]) SELECT [rowid], [name] FROM [tracks];
INSERT INTO [tracks_fts] ([tracks_fts]) VALUES ('optimize');
CREATE VIRTUAL TABLE [albums_fts] USING FTS5 ([name], content=[albums]);
INSERT INTO [albums_fts] (rowid, [name]) SELECT [rowid], [name] FROM [albums];
INSERT INTO [albums_fts] ([albums_fts]) VALUES ('optimize');
CREATE VIRTUAL TABLE [artists_fts] USING FTS5 ([name], content=[artists]);
INSERT INTO [artists_fts] (rowid, [name]) SELECT [rowid], [name] FROM [artists];
INSERT INTO [artists_fts] ([artists_fts]) VALUES ('optimize');
`

const views = `
CREATE VIEW listen_details AS select
  l.*,
  t.*,
  album.*,
  artist.*
from
  listens l
  inner join tracks t on l.tracks_id = t.id
  left join albums album on t.albums_id = album.id
  left join artists artist on album.artists_id = artist.id;
`

// write builds the database beside path and moves it into place, so a reader
// holding the old file never sees a half-written one.
func (m *model) write(path string) error {
	tmp := path + ".building"
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Durability buys nothing here: the database is derived data, and a failed
	// build is thrown away rather than recovered.
	db, err := sql.Open("sqlite3", tmp+"?_journal_mode=OFF&_synchronous=OFF")
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`PRAGMA cache_size = -262144; PRAGMA temp_store = MEMORY; PRAGMA locking_mode = EXCLUSIVE`); err != nil {
		return fmt.Errorf("pragmas: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	if err := m.insertRows(db); err != nil {
		return err
	}
	for _, step := range []struct {
		name  string
		stmts string
	}{{"indexes", indexes}, {"search", search}, {"views", views}} {
		if _, err := db.Exec(step.stmts); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}
	if err := db.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *model) insertRows(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insert := func(query string, rows int, bind func(i int, exec func(...any) error) error) error {
		stmt, err := tx.Prepare(query)
		if err != nil {
			return fmt.Errorf("%s: %w", query, err)
		}
		defer stmt.Close()
		exec := func(args ...any) error {
			_, err := stmt.Exec(args...)
			return err
		}
		for i := range rows {
			if err := bind(i, exec); err != nil {
				return fmt.Errorf("%s: %w", query, err)
			}
		}
		return nil
	}

	err = insert(`INSERT INTO [artists] ([id],[url],[name],[image],[mbid],[plays]) VALUES (?,?,?,?,?,?)`,
		len(m.artists), func(i int, exec func(...any) error) error {
			a := &m.artists[i]
			return exec(i+1, a.url, a.name, a.image, a.mbid, a.plays)
		})
	if err != nil {
		return err
	}

	err = insert(`INSERT INTO [albums] ([id],[artists_id],[mbid],[name],[plays]) VALUES (?,?,?,?,?)`,
		len(m.albums), func(i int, exec func(...any) error) error {
			a := &m.albums[i]
			return exec(i+1, a.artistID, a.mbid, a.name, a.plays)
		})
	if err != nil {
		return err
	}

	// Only tracks with a dated listen are written: the duration estimate and
	// play count are derived from those listens, and a track that has none
	// carries neither.
	err = insert(`INSERT INTO [tracks] ([id],[albums_id],[mbid],[name],[image],[url],[streamable],[tracks_id],[avg_estimated_duration],[plays],[total_estimated_listen_time]) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		len(m.tracks), func(i int, exec func(...any) error) error {
			t := &m.tracks[i]
			if t.plays == 0 {
				return nil
			}
			id := i + 1
			var total sql.NullFloat64
			if t.duration.Valid {
				total = sql.NullFloat64{Float64: float64(t.plays) * t.duration.Float64, Valid: true}
			}
			return exec(id, t.albumID, t.mbid, t.name, t.image, t.url, t.streamable, id, t.duration, t.plays, total)
		})
	if err != nil {
		return err
	}

	err = insert(`INSERT INTO [listens] ([tracks_id],[date_uts],[date_#text],[loved],[@attr_nowplaying]) VALUES (?,?,?,?,?)`,
		len(m.listens), func(i int, exec func(...any) error) error {
			l := &m.listens[i]
			return exec(l.trackID, l.uts, l.utsText, l.loved, l.nowPlaying)
		})
	if err != nil {
		return err
	}

	return tx.Commit()
}
