# B.LEAGUE fixtures - provenance

Source: sdv-internal-refs `bleague/` (commit a35a537 and later), captured live and keyless on 2026-10-12/13
(requests >= 1 s apart; robots.txt sets Crawl-delay only for msnbot/bingbot). Tests read these; nothing reads
the private refs checkout.

| Path | What |
|---|---|
| `full/v2_504734.json.gz` | FULL, untruncated v2 context (`b-league.s3.amazonaws.com/web_json/v2_genius_contexts/504734/110.json`, fetched 2026-10-13): B1 2025-26 sec 1, Alvark Tokyo 56-81 Utsunomiya Brex. Numeric dtypes come from this. gzip because it is 576 KB raw |
| `full/v1_10693.json.gz` | FULL v1 context (`.../v1_contexts/10693/592.json`): B3 2025-26 sec 29, Veertien Mie 81-93 Tachikawa Dice (412 KB raw) |
| `captures/*.json` | the refs captures (arrays cut to 3 rows): v2 504734 / 505508 / 500017 / 5856, v1 10693 / 2988, the parsed `game_detail` (ScheduleKey 1, 2016-17) and schedule / standings records |
| `html/game_detail_1.html` | a bare page around the refs `game_detail.json` record as `_contexts_s3id.data` (the real page is 0.9 MB) |
| `html/schedule_2025_1004.html.gz`, `html/standings_2025_b1.html.gz` | the REAL pages `schedule/?tab=1&year=2025&event=2&mon=10&day=04` and `standings/?year=2025&tab=1`, gzipped |
| `spec_paths.json` | the spec's routes, written by `tools/codegen/gen_bleague.py` (CI has no refs checkout) |
