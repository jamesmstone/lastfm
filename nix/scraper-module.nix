{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.lastfm-scraper;
in
{
  options.services.lastfm-scraper = {
    enable = lib.mkEnableOption "the Last.fm scraper";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.scrape;
      defaultText = lib.literalExpression "lastfm.packages.\${system}.scrape";
      description = "The scraper package to run.";
    };

    remote = lib.mkOption {
      type = lib.types.str;
      description = "Git remote holding the archive; the scraper clones and pushes to it.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "james";
      description = ''
        User to run as. Its ~/.ssh must be able to push to `remote`, which is
        why the unit does not set ProtectHome. It also needs write access to
        `publish`, if that is set.
      '';
    };

    interval = lib.mkOption {
      type = lib.types.str;
      default = "*-*-* 00/2:06:00";
      description = "systemd OnCalendar expression for how often to scrape.";
    };

    stateDir = lib.mkOption {
      type = lib.types.str;
      default = "/var/lib/lastfm-scraper";
      description = "Holds the scraper's checkout of the archive and the database it builds.";
    };

    apiKeyFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      description = ''
        File holding the Last.fm API key, passed to the unit as a credential.
        Only a run with days to scrape needs one.
      '';
    };

    publish = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "/var/lib/datasette-sites/lastfm/music.db";
      description = ''
        Where to install the rebuilt database, replaced by rename so a reader
        never sees a partial file. Null builds it in `stateDir` and stops there.
      '';
    };

    refresh = lib.mkOption {
      type = lib.types.int;
      default = 2;
      description = ''
        How many of the most recent days to treat as still in progress. Those
        days are re-scraped every run and held out of the settled archive, so a
        build over that archive alone is reproducible between scrapes.
      '';
    };

    from = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Earliest day to scrape, YYYY-MM-DD. Null uses the binary's default.";
    };

    to = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Latest day to scrape, YYYY-MM-DD. Null scrapes up to today.";
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.tmpfiles.rules = [ "d ${cfg.stateDir} 0750 ${cfg.user} - - -" ];

    systemd.services.lastfm-scraper = {
      description = "Scrape the Last.fm listening history and rebuild music.db";
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];

      environment = {
        LASTFM_REMOTE = cfg.remote;
        LASTFM_STATE = cfg.stateDir;
        LASTFM_REFRESH = toString cfg.refresh;
      }
      // lib.optionalAttrs (cfg.publish != null) { LASTFM_PUBLISH = cfg.publish; }
      // lib.optionalAttrs (cfg.from != null) { LASTFM_FROM = cfg.from; }
      // lib.optionalAttrs (cfg.to != null) { LASTFM_TO = cfg.to; };

      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        ExecStart = lib.getExe cfg.package;
        # The box also runs the forge; a scrape is never the urgent thing on it.
        Nice = 19;
        IOSchedulingClass = "idle";
        NoNewPrivileges = true;
        PrivateTmp = true;
        LoadCredential = lib.optional (cfg.apiKeyFile != null) "api-key:${cfg.apiKeyFile}";
      };
    };

    systemd.timers.lastfm-scraper = {
      description = "Scrape the Last.fm listening history on a schedule";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = cfg.interval;
        # A box that was off at the scheduled minute should still catch up.
        Persistent = true;
        RandomizedDelaySec = "2m";
        Unit = "lastfm-scraper.service";
      };
    };
  };
}
