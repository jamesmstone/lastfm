#!/bin/bash
set -e # Exit with nonzero exit code if anything fails
set -o pipefail
set -o errexit

downloadDir="data/lastfm"
dockerGitHistory="git-history"
dockerSQLUtil="sqlite-utils"

TZ=UTC

function buildDocker() {
  docker build --tag "$dockerGitHistory" --pull --file git-history.Dockerfile .
  docker build --tag "$dockerSQLUtil" --file sqlite-utils.Dockerfile .

}

function git-history() {
  docker run \
    -i \
    -u"$(id -u):$(id -g)" \
    -v"$(pwd):/wd" \
    -w /wd \
    "$dockerGitHistory" \
    "$@"
}
function sql-utils() {
  docker run \
    -i \
    -u"$(id -u):$(id -g)" \
    -v"$(pwd):/wd" \
    -w /wd \
    "$dockerSQLUtil" \
    "$@"
}

function ensureDownloadDir() {
  mkdir -p "$downloadDir"
}

function download() {
  curl --connect-timeout 40 \
    --max-time 300 \
    --retry 5 \
    --retry-delay 5 \
    --retry-max-time 40 \
    --silent \
    --show-error \
    --fail \
    --proto =http,https \
    "$@"
}

function downloadDate() {
  local yesterday=$(date -d@"$(($(date +%s) - (24 * 60 * 60)))" +"%Y-%m-%d")

  local start=${1:-$yesterday}

  local startUnix=$(date -d"$start" "+%s")
  local endUnix=$(date -d@"$(($startUnix + (24 * 60 * 60)))" "+%s")

  local page="1"
  local totalPages="2"
  local recenttracks="[]"
  while [ $page -le $totalPages ]; do
    local url="https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=jamesmstone711&api_key=$LASTFM_API_KEY&format=json&from=$startUnix&to=$endUnix&limit=20&extended=1&page=$page"
    local res=$(download "$url")
    totalPages=$(echo $res | jq -r ".recenttracks[\"@attr\"].totalPages")

    local tracks=$(echo $res | jq -r ".recenttracks.track")
    page=$(($page + 1))
    local recenttracks=$(jq -s '.[0] + [.[1]] | flatten' <(echo "$recenttracks") <(echo "$tracks"))
    sleep 0.2
  done
  echo $recenttracks
}

downloadDateToFile() {
  local date=${1}
  local file="$downloadDir/$date.json"
  ensureDownloadDir
  downloadDate "$date" >"$file"
}

function ensureHaveDate() {
  local date=${1}
  local file="$downloadDir/$date.json"
  if [ -f "$file" ]; then
    return 0
  fi
  downloadDateToFile "$date"
}

function ensureHaveAllSinceDate() {
  local today=$(date "+%Y-%m-%d")
  local startDate=${1}
  local endDate=${2:-$today}

  local curUnix=$(date -d"$startDate" "+%s")
  local endUnix=$(date -d"$endDate" "+%s")

  while [[ $curUnix -le $endUnix ]]; do
    local curDate=$(date -d@"$curUnix" "+%Y-%m-%d")
    echo "ensuring have: $curDate" >&2
    ensureHaveDate "$curDate"

    curUnix=$(date -d@"$(($curUnix + (24 * 60 * 60)))" "+%s")

  done
}

function addDate() {
  local date=${1}
  local file="$downloadDir/$date.json"

  sql-utils insert "$db" "listens" "$file" --alter
}

function addAllSinceDate() {
  local today=$(date "+%Y-%m-%d")
  local db=${1}
  local startDate=${2}
  local endDate=${3:-$today}

  local curUnix=$(date -d"$startDate" "+%s")
  local endUnix=$(date -d"$endDate" "+%s")

  while [[ $curUnix -le $endUnix ]]; do
    local curDate=$(date -d@"$curUnix" "+%Y-%m-%d")

    echo "Adding: $curDate" >&2
    addDate "$curDate"

    curUnix=$(date -d@"$(($curUnix + (24 * 60 * 60)))" "+%s")

  done
}

makeDB() {
  local db="$1"
  rm -rf "$db" || true
  local addSince="$2"
  addAllSinceDate "$db" "$addSince"
  sql-utils extract "$db" listens --table tracks artist album mbid name image streamable url
  sql-utils extract "$db" tracks --table artists artist
  sql-utils extract "$db" tracks --table albums album artists_id

  sql-utils schema "$db" artists
  sql-utils convert "$db" artists artist \
    'import json
return json.loads(value)
' --multi --drop

  sql-utils convert "$db" albums album \
    'import json
return json.loads(value)
' --multi --drop

  sql-utils enable-fts "$db" tracks name
  sql-utils enable-fts "$db" artists name
  sql-utils enable-fts "$db" albums album
  sql-utils vacuum "$db"
}

commitDB() {
  local dbBranch="db"
  local db="$1"
  local tempDB="$(mktemp)"
  git branch -D "$dbBranch" || true
  git checkout --orphan "$dbBranch"
  mv "$db" "$tempDB"
  rm -rf *
  mv "$tempDB" "$db"
  git add "$db"
  git commit "$db" -m "push db"
  git push origin "$dbBranch" -f
}
commitData() {
  git config user.name "Automated"
  git config user.email "actions@users.noreply.github.com"
  git add -A
  timestamp=$(date -u)
  git commit -m "Latest data: ${timestamp}" || exit 0
  git push
}

publishDB() {
  local dockerDatasette="datasette"
  docker build --tag "$dockerDatasette" --pull --file datasette.Dockerfile .
  docker run \
    -v"$(pwd):/wd" \
    -w /wd \
    "$dockerDatasette" \
    publish vercel "$db" --token $VERCEL_TOKEN --project=lastfmlog
}

run() {
  local db="music.db"
  local today=$(date "+%Y-%m-%d")
  local yesterday=$(date -d@"$(($(date +%s) - (24 * 60 * 60)))" +"%Y-%m-%d")

  local startDate="2012-11-01"

  ensureHaveAllSinceDate "$startDate"
  # force re download
  downloadDateToFile "$yesterday"
  downloadDateToFile "$today"

  set -x # debug: print commands before they are executed

  commitData

  makeDB "$db" "$startDate"
  publishDB "$db"
  commitDB "$db"

}

buildDocker
run "$@"
