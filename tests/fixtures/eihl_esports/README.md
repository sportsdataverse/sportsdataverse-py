<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [eihl_esports fixtures — EIHL (eSports hokejovyzapis S3 bucket `eihl.hokejovyzapis.cz`)](#eihl_esports-fixtures--eihl-esports-hokejovyzapis-s3-bucket-eihlhokejovyzapiscz)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# eihl_esports fixtures — EIHL (eSports hokejovyzapis S3 bucket `eihl.hokejovyzapis.cz`)

- `*.json` (top level): copies of the maintainers' captures of the `eihl` bucket,
  captured live 2026-10-12. Arrays are truncated to 3 items at every depth (one of
  each event type / shot result first), so they fix column NAMES, not dtypes. One file per path the
  bucket serves, named after the path template: `https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/<path>`
  with season 2026 (START year, i.e. 2026/27), match 4440, team 96, league 3.
- `full/`: untruncated bodies fetched 2026-10-12 from the same bucket and game, the source of the
  returns-schema dtypes (`full/visualization__shots__match_id.json` = `visualization/shots/4440.json`). Same file names as the captures.

Public data the leagues' own widgets read anonymously; nothing is edited out.
