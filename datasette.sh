#!/usr/bin/env bash
set -euo pipefail

exec nix run nixpkgs#datasette -- -p 8001 -h 127.0.0.1 music.db "$@"
