{
  description = "Scrape a Last.fm listening history and build the Datasette database from it";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      inherit (nixpkgs) lib;

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forEachSystem = f: lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # Only the Go sources: including data/ would copy the whole archive into
      # the store and rebuild the binary every time a day is scraped.
      src = lib.fileset.toSource {
        root = ./.;
        fileset = lib.fileset.unions [
          ./go.mod
          ./go.sum
          ./main.go
          ./internal
        ];
      };
    in
    {
      packages = forEachSystem (pkgs: rec {
        default = lastfm;

        lastfm = pkgs.buildGoModule {
          pname = "lastfm";
          version = "0.1.0";
          inherit src;

          vendorHash = "sha256-q9BCDpLndmvFq67BZEyA8i4q/gyDwiTa7+Sz3JlrnQM=";

          # go-sqlite3 compiles FTS5 out unless it is asked for, and the
          # database exposes full-text search over the track, album and artist
          # names.
          tags = [ "sqlite_fts5" ];
          ldflags = [
            "-s"
            "-w"
          ];

          meta = {
            description = "Last.fm scraper and music.db builder";
            mainProgram = "lastfm";
          };
        };
      });

      checks = forEachSystem (pkgs: {
        # buildGoModule runs `go test` as part of this.
        build = self.packages.${pkgs.system}.lastfm;

        gofmt = pkgs.runCommandLocal "lastfm-gofmt" { nativeBuildInputs = [ pkgs.go ]; } ''
          unformatted=$(cd ${src} && gofmt -l .)
          if [ -n "$unformatted" ]; then
            echo "not gofmt'd:" >&2
            echo "$unformatted" >&2
            exit 1
          fi
          touch $out
        '';
      });

      devShells = forEachSystem (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.go
            pkgs.gcc
            pkgs.sqlite
          ];
        };
      });

      formatter = forEachSystem (pkgs: pkgs.nixfmt-tree);
    };
}
