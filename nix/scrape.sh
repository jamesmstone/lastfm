# One scrape: refresh the day files, commit what changed, rebuild music.db and
# hand it to Datasette.
#
# Runs unattended from a systemd timer, so every network call is bounded and
# nothing may ever wait on a prompt: an unbounded git call in a unit stalls
# `nixos-rebuild switch` behind it.
set -euo pipefail

: "${LASTFM_REMOTE:?set LASTFM_REMOTE}"
: "${LASTFM_STATE:?set LASTFM_STATE}"
refresh=${LASTFM_REFRESH:-2}
publish=${LASTFM_PUBLISH:-}

repo=$LASTFM_STATE/repo
settled=$repo/data/lastfm
pending=$repo/data/pending

export GIT_TERMINAL_PROMPT=0
export GIT_SSH_COMMAND="ssh -o BatchMode=yes -o ConnectTimeout=10"
# HOME is inherited only so ssh finds the key that pushes. Everything else this
# repo needs is configured below, so the unit does not pick up whatever is in
# the running user's ~/.gitconfig.
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null

# Passed as a systemd credential rather than an environment variable, so the key
# is not readable in the unit's definition.
if [ -z "${LASTFM_API_KEY:-}" ] && [ -r "${CREDENTIALS_DIRECTORY:-/nonexistent}/api-key" ]; then
  LASTFM_API_KEY=$(cat "$CREDENTIALS_DIRECTORY/api-key")
  export LASTFM_API_KEY
fi

mkdir -p "$LASTFM_STATE"
if [ ! -d "$repo/.git" ]; then
  timeout 30m git clone "$LASTFM_REMOTE" "$repo"
fi

git -C "$repo" config user.name "Automated"
git -C "$repo" config user.email "lastfmauto@jamesst.one"
# Twelve commits a day would otherwise trip auto-gc inside a scrape, at an
# unpredictable moment and under Nice=19. Repack from a separate timer instead.
git -C "$repo" config gc.auto 0

timeout 10m git -C "$repo" fetch --prune origin

# Never rebase and never discard: a day file is the only copy of those plays,
# and nothing else writes this archive, so a real divergence means something is
# wrong. Park it where a human can find it rather than throwing scrapes away.
if ! git -C "$repo" merge --ff-only origin/main >/dev/null 2>&1; then
  parked="diverged/$(date -u +%Y%m%dT%H%M%SZ)"
  echo "WARNING: diverged from origin/main; parking local work on $parked" >&2
  git -C "$repo" branch "$parked"
  timeout 5m git -C "$repo" push origin "$parked" || true
  git -C "$repo" reset --hard origin/main
fi
# Scoped away from data/: a run killed between the fetch and the commit leaves
# newly scraped days untracked there, and an unscoped clean would delete them.
git -C "$repo" clean -fdq -- ':!data'

lastfm fetch -data "$settled" -pending "$pending" -refresh "$refresh" ${LASTFM_FROM:+-from "$LASTFM_FROM"} ${LASTFM_TO:+-to "$LASTFM_TO"}

if [ -n "$(git -C "$repo" status --porcelain -- data)" ]; then
  git -C "$repo" add data
  git -C "$repo" commit -q -m "Latest data: $(date -u)"
fi
# Unconditional: a run whose push failed leaves a commit behind that the next
# one has to carry, and by then its working tree is clean again.
timeout 10m git -C "$repo" push origin HEAD:main

db=$LASTFM_STATE/music.db
lastfm build -data "$settled" -pending "$pending" -db "$db"

if [ -n "$publish" ]; then
  # Rename rather than write in place: Datasette holds the database open, and
  # install(1) truncates, so a reader can see a torn file mid-write.
  staged="$(dirname "$publish")/.$(basename "$publish").new"
  install -Dm644 "$db" "$staged"
  mv -f "$staged" "$publish"
fi
