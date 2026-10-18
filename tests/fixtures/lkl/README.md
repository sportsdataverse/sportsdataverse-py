# LKL fixtures - provenance

All captured from `https://lkl.lt` on 2026-10-12 (keyless, requests >= 1 s apart; robots.txt `Disallow:` empty, no
crawl-delay). Tests read these; nothing reads the private `sdv-internal-refs` checkout.

| Path | What |
|---|---|
| `full/*.json` | FULL, untruncated bodies of ONE finished overtime game, 11446 (Nevezis-Paskolu klubas 88-85 Jonavos Hipocredit, 2026-02-14, 2025-26): the seven `/api/livestream/{route}/{gameId}` bodies as served and `rungtynes.json`, the game page parsed by `lkl_parsers._game_record` (the runtime's record). Fetched through `lkl_runtime` itself, with the `_request` stamp removed (the tests restamp it from the page). Numeric dtypes in the returns schemas come from these. `play-by-play.json` is 220 KB. |
| `captures/*.json` | The refs captures (`sdv-internal-refs/lkl/captures`, every array cut to 3 rows): five games (bare = 11590 of 2026-27, `@11116` first play-by-play game 2024-25, `@11446` overtime, `@2000` 2007-08) per route, plus the parsed season records `loadResults*` (seasons 49436, 39252, 3090) and `loadStandings__seasonId*` (35, 33, 3). |
| `html/game_11590.html` | `/rungtynes/11590`: only the `<game-info-component>` element, in a bare page. |
| `html/results_49436_p{1,2,3}.html` | `/loadResults?team=-&opponent=-&month=-&season=49436&page=1..3`, all three pages of 2026-27 (10 + 10 + 2 games). |
| `html/rezultatai_selects.html`, `html/turnyrine_select.html` | `/rezultatai` `select#season-lkl` and `/turnyrine-lentele` `select#season`: the season selects the runtime maps the END year through (every LKL season 1993-94 to 2026-27). |
| `html/standings_{35,34,3}.html` | `/loadStandings/{id}` (2026-27 in progress, 2025-26, 2007-08 with a placeholder final table); each table keeps its header, 3 rows and its last row. |
| `spec_paths.json` | The spec's 10 routes, written by `tools/codegen/gen_lkl.py` (CI has no refs checkout). |

Whitespace in the HTML is collapsed and inline `<svg>` icons are dropped; nothing else changes. The site publishes
no contact or birth data on these routes (`lkl_runtime` still redacts such keys by name).
