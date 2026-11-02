<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [icehl_esports fixtures — ICE Hockey League (eSports hokejovyzapis S3 bucket `icehl.hokejovyzapis.cz`)](#icehl_esports-fixtures--ice-hockey-league-esports-hokejovyzapis-s3-bucket-icehlhokejovyzapiscz)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# icehl_esports fixtures — ICE Hockey League (eSports hokejovyzapis S3 bucket `icehl.hokejovyzapis.cz`)

- `*.json` (top level): copies of the maintainers' captures of the `icehl` bucket,
  captured live 2026-10-12. Arrays are truncated to 3 items at every depth (one of
  each event type / shot result first), so they fix column NAMES, not dtypes. One file per path the
  bucket serves, named after the path template: `https://s3.eu-west-1.amazonaws.com/icehl.hokejovyzapis.cz/<path>`
  with season 2026 (START year, i.e. 2026/27), match 8161, team 4, league 1, league part 56.
- `full/`: untruncated bodies fetched 2026-10-12 from the same bucket and game, the source of the
  returns-schema dtypes (`full/visualization__shots__match_id.json` = `visualization/shots/8161.json`). Same file names as the captures.

Public data the leagues' own widgets read anonymously; nothing is edited out.
