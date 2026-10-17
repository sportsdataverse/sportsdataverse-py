# metal_esports fixtures — Metal Ligaen (eSports hokejovyzapis S3 bucket `den.hokejovyzapis.cz`)

- `*.json` (top level): copies of `sdv-internal-refs/esports-hokejovyzapis/captures/den/` at refs
  commit `ad053b8`, captured live 2026-10-12. Arrays are truncated to 3 items at every depth (one of
  each event type / shot result first), so they fix column NAMES, not dtypes. One file per path the
  bucket serves, named after the path template: `https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/<path>`
  with season 2026 (START year, i.e. 2026/27), match 575, team 8, league 1.
- `full/`: untruncated bodies fetched 2026-10-12 from the same bucket and game, the source of the
  returns-schema dtypes (`full/matches__match_id__period-events.json` = `matches/575/period-events.json`; Denmark has no shot data, every shot file is an empty shell). Same file names as the captures.

Public data the leagues' own widgets read anonymously; nothing is edited out.
