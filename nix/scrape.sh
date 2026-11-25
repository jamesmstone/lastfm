# One scrape: refresh the day files, rebuild music.db and hand it to Datasette.
#
# The state directory is the only copy of the archive: nothing here clones,
# commits or pushes, so a run needs no git, no ssh key and no forge. The
# deployment backs the directory up instead.
#
#   $LASTFM_STATE/data/lastfm/<date>.json   settled days, written once
#   $LASTFM_STATE/data/pending/<date>.json  the tail, re-scraped every run
#   $LASTFM_STATE/music.db                  built from both, then published
#
# Runs unattended from a systemd timer, so nothing may ever wait on a prompt;
# every Last.fm request carries its own timeout.
set -euo pipefail

: "${LASTFM_STATE:?set LASTFM_STATE}"
refresh=${LASTFM_REFRESH:-2}
publish=${LASTFM_PUBLISH:-}

data=$LASTFM_STATE/data
settled=$data/lastfm
pending=$data/pending

# Passed as a systemd credential rather than an environment variable, so the key
# is not readable in the unit's definition.
if [ -z "${LASTFM_API_KEY:-}" ] && [ -r "${CREDENTIALS_DIRECTORY:-/nonexistent}/api-key" ]; then
  LASTFM_API_KEY=$(cat "$CREDENTIALS_DIRECTORY/api-key")
  export LASTFM_API_KEY
fi

# One-time migration from the git-scrape layout, where the archive lived in a
# clone at $LASTFM_STATE/repo. Its working tree is copied rather than moved, so
# the clone is left exactly as it was (and any days a killed run left
# uncommitted come across too); delete it by hand once the new layout has run.
# Staged and renamed so a run killed mid-copy starts the copy again instead of
# scraping on top of half an archive.
legacy=$LASTFM_STATE/repo
if [ ! -d "$data" ] && [ -d "$legacy/.git" ] && [ -d "$legacy/data" ]; then
  echo "migrating the archive out of the git clone at $legacy" >&2
  staging=$LASTFM_STATE/.data.migrating
  rm -rf "$staging"
  cp -a --reflink=auto "$legacy/data" "$staging"
  mv "$staging" "$data"
fi

mkdir -p "$settled" "$pending"

lastfm fetch -data "$settled" -pending "$pending" -refresh "$refresh" ${LASTFM_FROM:+-from "$LASTFM_FROM"} ${LASTFM_TO:+-to "$LASTFM_TO"}

db=$LASTFM_STATE/music.db
lastfm build -data "$settled" -pending "$pending" -db "$db"

if [ -n "$publish" ]; then
  # Rename rather than write in place: Datasette holds the database open, and
  # install(1) truncates, so a reader can see a torn file mid-write.
  staged="$(dirname "$publish")/.$(basename "$publish").new"
  install -m644 "$db" "$staged"
  mv -f "$staged" "$publish"
fi
