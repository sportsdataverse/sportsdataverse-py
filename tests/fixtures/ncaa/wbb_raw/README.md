<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [A halves-era WBB producer bundle](#a-halves-era-wbb-producer-bundle)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# A halves-era WBB producer bundle

`wbb/raw/2015/385627.json.gz` is copied byte-for-byte from the `ncaa-wbb-hoops-raw` producer
repo (`wbb/raw/2015/385627.json.gz`, repo head `a5c6282709`), laid out as a raw tree so the
parse stage's `_main(league, root)` can run on this directory as its `--root`.

| field | value |
|---|---|
| contest | `385627`, Alabama A&M at Southeast Mo. St., 2014-11-14 (final 38-66) |
| season | `"2015"` (2014-15: women's college basketball still played two 20-minute halves) |
| pages | `play_by_play`, `box_score`, `individual_stats` from `https://stats.ncaa.org/contests/385627/{page}` |
| captured | `2026-08-14T10:35:41Z` by the producer's capture stage (`captured_at` in the bundle) |

The play-by-play page writes names in the V1 `LAST,FIRST event` grammar (B-L39). Used by
`tests/scrape/ncaa/test_parse_era.py` (B-M35) and, for its three clean pages,
`tests/scrape/ncaa/test_capture_behaviour.py` (B-M36).
