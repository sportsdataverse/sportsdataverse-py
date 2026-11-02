<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [metal_esports fixtures — Metal Ligaen (eSports hokejovyzapis S3 bucket `den.hokejovyzapis.cz`)](#metal_esports-fixtures--metal-ligaen-esports-hokejovyzapis-s3-bucket-denhokejovyzapiscz)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# metal_esports fixtures — Metal Ligaen (eSports hokejovyzapis S3 bucket `den.hokejovyzapis.cz`)

- `*.json` (top level): copies of the maintainers' captures of the `den` bucket,
  captured live 2026-10-12. Arrays are truncated to 3 items at every depth (one of
  each event type / shot result first), so they fix column NAMES, not dtypes. One file per path the
  bucket serves, named after the path template: `https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/<path>`
  with season 2026 (START year, i.e. 2026/27), match 575, team 8, league 1.
- `full/`: untruncated bodies fetched 2026-10-12 from the same bucket and game, the source of the
  returns-schema dtypes (`full/matches__match_id__period-events.json` = `matches/575/period-events.json`; Denmark has no shot data, every shot file is an empty shell). Same file names as the captures.

Public data the leagues' own widgets read anonymously; nothing is edited out.
