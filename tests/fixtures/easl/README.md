# EASL fixtures

Bodies of the UNOFFICIAL third-party sync server `https://easl.joshricha.com/api/v1` (an individual's Express server that
mirrors Sportradar DataCore, organisation `b1e0z`; keyless; it can change or vanish).

| Dir | What | Source | Captured |
|---|---|---|---|
| `captures/` | the refs captures: every route, arrays cut to 3 rows at every depth; the bare name is the active season (2026-27), `@2025` / `@2024` / `@2023` the editions by START year (2025 = EASL 25-26, sdv 2026) | `sdv-internal-refs` `easl/captures/` at commit `0fb6793` (`easl/tools/capture.py`) | 2026-10-12 |
| `full/` | UNTRUNCATED bodies: `/metadata/seasons`, `/matches?seasonId=` (2025-26), the 2025-26 final's `/boxscore`, `/playbyplay`, `/teamstats` and `/teamstats/by/period`, `/standings` and `/leaders` (2025-26), `/players?seasonId=&teamId=` (Utsunomiya Brex roster) and `/teams`. The returns schemas take their numeric dtypes from these | live GET of `https://easl.joshricha.com/api/v1/<route>` by the easl implementer (10 requests, 1.3 s apart, re-serialised compact and UTF-8) | 2026-10-13 |
| `spec_paths.json` | the spec's 18 routes and the route each wrapper reads (CI has no refs checkout) | written by `tools/codegen/gen_easl.py` from `easl/easl.openapi.yaml` | |

Games: 2025-26 fixture `10af8fd4-106e-11f1-960b-5798087c7aa3` (the EASL Championship: Taoyuan Pauian Pilots 81-90
Utsunomiya Brex, Macau); the other captures use the 2024-25 final `bde3aaa1-da50-11ef-97fd-97d4eecf418b`, the 2023-24
Championship Game `6e3c321b-d078-11ee-bd9b-cfe0b28e3077` and the active season's game `e123336e-a6d0-11f1-9d9e-dd8ab99a2c59`.
Season ids: 2023-24 `c24ce68a-6420-11ee-9daf-6b6188281bd0`, 2024-25 `57cefc32-5ace-11ef-81a0-3d1f6018efb7`, 2025-26
`77441bb3-9759-11f0-b659-0561c09a0222`, 2026-27 `1409787d-a683-11f1-aea7-b596f55be5ed`.

No personal contact data occurs in these bodies; a player's birth date and photo URL are public biography fields.
The refs `captures/presence/` sidecars (types of every column over the FULL bodies) are not copied: `gen_easl.py` reads
them from the refs checkout to check the parsers' statistic blocks.
