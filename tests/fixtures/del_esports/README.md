<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [del_esports fixtures — DEL (eSports hokejovyzapis S3 bucket `de.hokejovyzapis.cz`)](#del_esports-fixtures--del-esports-hokejovyzapis-s3-bucket-dehokejovyzapiscz)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# del_esports fixtures — DEL (eSports hokejovyzapis S3 bucket `de.hokejovyzapis.cz`)

- `*.json` (top level): copies of the maintainers' captures of the `de` bucket,
  captured live 2026-10-12. Arrays are truncated to 3 items at every depth (one of
  each event type / shot result first), so they fix column NAMES, not dtypes. One file per path the
  bucket serves, named after the path template: `https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/<path>`
  with season 2026 (START year, i.e. 2026/27), match 4444, team 12, league 1.
- `full/`: untruncated bodies fetched 2026-10-12 from the same bucket and game, the source of the
  returns-schema dtypes (`full/visualization__shots__match_id.json` = `visualization/shots/4444.json`; `full/matches__match_id__period-events.json` = `matches/4444/period-events.json`; `full/league-matches__season__league_id.json` = `league-matches/2026/1.json`, the whole 2026/27 DEL schedule (237 KB), which adds the overtime / shootout score columns the capture lacks). Same file names as the captures.

Public data the leagues' own widgets read anonymously; nothing is edited out.
