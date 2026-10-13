# KZS fixtures (`api.kzs.si/api/v1/public`)

Every file is a body as the API sends it: the `{"status": "OK", "data": ...}` envelope. The tests wrap `data` and stamp
the request the way `kzs_runtime._get` does.

| Path | What |
|---|---|
| `full/*` | **Full, untruncated** bodies fetched 2026-10-12 (plain `requests`, 1.2 s apart, no key, no proxy) and redacted with the refs capture's own `redact` + `sweep_text` (contact data by key and by text). The returns-schema dtypes (`kzs_dtypes.py`) and the dtype tests come from these: the refs captures cut arrays to 3 rows and mistype rates. Game 101839 (men's Liga OTP banka 2025/26, 95-93): `match__101839` (header), `stats__101839` (box score), `playByPlay__101839.json.gz` (816 actions, 358 KB raw so gzipped). Competition 579: `standings__579`, `playerStats__579`, `teamStats__579` (limit 1000), `matches__579_2025-10` (21 matches of October 2025). Also `seasons` (all 39), `competitions__season26` (44 competitions of season id 26, which also proves the `seasonId` filter), `player_stats__106285_26`. |
| `captures/*.json` | The sdv-internal-refs samples (`kzs/captures/`, refs commit bae9a4f of the local checkout, 2026-10-12), arrays cut to 3 rows: men's 2025-26 (comp 579, game 101839), women's 2020-21 (comp 30, game 62586), men's 2015-16 (comp 9, game 29150, the first play-by-play season) and men's 2007-08 (comp 1, game 5787, scores only). |
| `spec_paths.json` | The spec's 18 routes, written by `tools/codegen/gen_kzs.py` so the tests check the YAML without the refs checkout (CI has none). |

The advanced box-score fields (`offensive_rating`, `usage_percentage`, ...) are 0 in every observed game, so the
generator types them Float64 from the start (`gen_kzs.FLOAT_COLS`).
