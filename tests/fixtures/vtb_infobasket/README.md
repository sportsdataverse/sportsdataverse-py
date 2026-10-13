# VTB Infobasket fixtures (`org.infobasket.su`)

Every file is a body as the host sends it, except that the runtime's personal-data scrub (`vtb_infobasket_runtime._scrub`)
has been applied: federation licence numbers, contact and operator-account fields are removed for everyone, and birth
dates, birth years and ages are removed for coaches, referees and other officials (players' public bio stays). The tests
stamp the request under `_request` the way `vtb_infobasket_runtime._get` does.

| Path | What |
|---|---|
| `full/*` | **Full, untruncated** bodies fetched 2026-10-13 with plain `requests` through the shared polite session (1 s apart, no key, no proxy); the large ones are gzipped. The returns-schema dtypes (`vtb_dtypes.py`) and the dtype tests come from these: the refs captures cut arrays to 3 rows. Game 921391 (Lokomotiv Kuban 85-86 Uralmash, 2025-09-28): `online_921391.json.gz` (`/Widget/GetOnline/921391`, 1,152 plays, 424 KB raw). Season 2026 (CompID 50714): `calendar_50714.json.gz` (254 games, 499 KB raw), `team_stats_50714.json.gz` (`/Comp/GetTeamStats/50714?page=0`, 11 teams with players and coaches, 438 KB raw), `standings_50720.json` (`/Widget/CompTeamResults/50720`, the regular season). Team 2650: `roster_2650_50714.json` (`TeamRoster`), `team_season_stats_2650_50714.json` (`TeamStats`). Player 423011: `player_game_log_423011_50714.json.gz` (`PlayerStats`, team 2650, 50 games), `player_season_stats_423011.json` (`PlayerSeasonStats`). |
| `captures/*.json` | The sdv-internal-refs samples (`vtb/captures/ib/`, refs commit `0697dd9` (vtb/, 2026-10-12), captured 2026-10-12 from the droplet with `curl_cffi`), arrays cut to 3 rows (`OnlinePlays` keeps a located 2-point shot, a located 3-point shot and a child play; `OnlineStarts` a player, a coach and an official), personal data nulled by the capture. Games: `921391` (2026), `647209` (2022-23, the first play-by-play season), `684495` (2013-14, box score only). |
| `spec_paths.json` | The spec's 24 routes, written by `tools/codegen/gen_vtb.py` so the tests check the YAML without the refs checkout (CI has none). |

Hosts and URLs: `https://org.infobasket.su/{Comp,Widget}/...` (see `tools/codegen/endpoints/vtb_infobasket.yaml`).
`DaysFromToday`, `IsToday` and `Age` depend on the capture day.
