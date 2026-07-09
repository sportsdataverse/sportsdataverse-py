<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [247Sports Site-Pages Port — Implementation Plan (Track 4)](#247sports-site-pages-port--implementation-plan-track-4)
  - [Goal](#goal)
  - [Architecture](#architecture)
  - [Tech Stack](#tech-stack)
  - [Global Constraints](#global-constraints)
  - [Parallel-contention note](#parallel-contention-note)
  - [Route inventory (35 routes → 17 schemas, from `site-pages.openapi.yaml`)](#route-inventory-35-routes-%E2%86%92-17-schemas-from-site-pagesopenapiyaml)
  - [Tasks](#tasks)
    - [Task 1 — `sports247_site_pages_runtime._get` (curl_cffi Chrome, no JWT, no slash-rewrite)](#task-1--sports247_site_pages_runtime_get-curl_cffi-chrome-no-jwt-no-slash-rewrite)
    - [Task 2 — `parse_sports247_site_page` (FK-surfacing + string-numeric cast) + fixtures](#task-2--parse_sports247_site_page-fk-surfacing--string-numeric-cast--fixtures)
    - [Task 3 — Generator `gen_sports247_site_pages.py`](#task-3--generator-gen_sports247_site_pagespy)
    - [Task 4 — Run the generator; commit endpoints YAML + returns-schemas](#task-4--run-the-generator-commit-endpoints-yaml--returns-schemas)
    - [Task 5 — Register in `FLAT_APIS` + `_FLAT_API_DOC`](#task-5--register-in-flat_apis--_flat_api_doc)
    - [Task 6 — Regenerate wrappers + docs; drift `--check`](#task-6--regenerate-wrappers--docs-drift---check)
    - [Task 7 — Returns-table descriptions in `manual_column_descriptions.yaml`](#task-7--returns-table-descriptions-in-manual_column_descriptionsyaml)
    - [Task 8 — Live-gated smoke tests](#task-8--live-gated-smoke-tests)
    - [Task 9 — Full gate sweep](#task-9--full-gate-sweep)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 247Sports Site-Pages Port — Implementation Plan (Track 4)

> Executable by agentic workers via TDD (failing test → run → implement → pass → commit). Each task is bite-sized and independently verifiable.

- **Date:** 2026-07-08
- **Track:** T4 of the `2026-07-08-recruiting-pff-codegen-ports-design.md` program (2nd PR in the rollout: after T3 sports247 RDB expand, before T2 on3 / T1 pff).
- **Design spec:** `docs/superpowers/specs/2026-07-08-recruiting-pff-codegen-ports-design.md` (§ "Track 4").
- **OpenAPI source:** `C:/Users/saiem/Documents/sdv-internal-refs/247sports/site-pages.openapi.yaml` (35 routes, 17 component schemas) + `247sports/ENDPOINTS.md` + `site-pages-returns.md` + `captures/site-pages/*.json` (36 real captures) + `schemas/site-pages/*.json`.

## Goal

Ship a NEW codegen-generated flat-API stem `sports247_site_pages` (prefix `cfb`) wrapping the 35 auth-free front-end page-model routes on host `247sports.com` (the `*.json` surface — **distinct** from the guest-JWT `ipa.247sports.com/rdb/v1` RDB that the existing `sports247` stem covers). Every wrapper returns a tidy `polars.DataFrame` by default; `return_as_pandas=True` for pandas; `return_parsed=False`/`raw` for the raw dict/list. Parser bakes in the three captured gotchas: (a) numeric fields serialize as **strings** → cast to real dtypes at the boundary; (b) nested entities are **bare integer foreign keys** → surface the FK columns, no auto-traversal; (c) the season path segment is `{year}-{Sport}` (e.g. `2022-Football`).

## Architecture

One-parser / one-runtime / codegen-generated-wrappers, cloned from the sibling `sports247` stem but **auth-free** and **no trailing-slash rewrite** (site-pages URLs terminate in `.json`):

```
sportsdataverse/cfb/
  sports247_site_pages.py            # GENERATED wrappers (35), name_pattern sports247_site_pages_{short}
  sports247_site_pages_parsers.py    # hand-written: parse_sports247_site_page (one generic parser)
  sports247_site_pages_runtime.py    # hand-written: _get (curl_cffi impersonate=chrome, injectable transport, no JWT, no slash-rewrite)
tools/codegen/
  gen_sports247_site_pages.py        # NEW generator: OpenAPI -> endpoints YAML + returns-schemas
  endpoints/sports247_site_pages.yaml            # GENERATED
  schemas/native/sports247_site_pages/*.yaml     # GENERATED (17 entity schemas)
  generate.py                        # register in FLAT_APIS + _FLAT_API_DOC (shared append point — see contention note)
  manual_column_descriptions.yaml    # 17 schema-keyed description blocks (shared append point)
tests/
  fixtures/sports247_site_pages/*.json           # copied from internal-refs captures/site-pages/
  test_sports247_site_pages_parsers.py           # offline: parser + runtime + wrapper wiring
  test_sports247_site_pages_live.py              # @skip_if_no_247_live gated smoke tests
```

## Tech Stack

polars 1.x (modern API), pandas (`json_normalize` for nested flattening), curl_cffi (lazy optional import, `impersonate="chrome"`), PyYAML (generator), pytest, uv toolchain, ruff, mypy.

## Global Constraints

- **polars pinned `>=1.0,<2.0` — modern API only.** `group_by` not `groupby`; `pl.len()`; `with_row_index`; bool masks explicit (`pl.col("c") == True`); Rust/polars regex has **no lookaround** (use inline `(?i)...(?-i:...)` toggle if ever needed — not expected here).
- **String-numeric fields cast at the boundary** (ID/join-key dtype discipline). FK id columns arrive as bare ints from `json_normalize` (Int64); string-numeric measure fields (`Latitude`, `CompositeRating`, `OverallRank`, …) are cast to Int64/Float64 only when *every* non-null value parses (no new nulls). Genuine strings (`Name`, `DefaultName`, `Height="6-5"`, `Rankable="True"`) stay Utf8. Never a float→Utf8 paper-over cast.
- **Returns-table column descriptions live ONLY in `tools/codegen/manual_column_descriptions.yaml`** (schema-keyed), NEVER in `schemas/**.yaml` (clobbered on re-capture / re-generate).
- **mypy files-ratchet:** append each new hand-written typed module to the `[tool.mypy] files = [...]` list in `pyproject.toml` once it types cleanly (generated `sports247_site_pages.py` is generated; the ratchet targets the hand-written `_runtime`/`_parsers`).
- **uv for everything:** `uv run pytest|ruff|mypy`. After any `uv run mypy`/`pytest`, `git status` — if `uv.lock` re-locked, `git checkout uv.lock` (don't let it ride into a commit).
- **Regenerate generated files + `--check` before every commit that touches sources.** `uv run python tools/codegen/generate.py` then `uv run python tools/codegen/generate.py --check`. Drift gate runs in CI + the `sdv-codegen` pre-commit hook.
- **Conventional Commits; NO AI co-author trailers** (human is sole author).
- **curl_cffi is a lazy optional import** (`tests`/`all` extras, not a hard runtime dep); missing → clear `ImportError` directing to `pip install curl_cffi` / `sportsdataverse[all]`. Transport injectable so offline tests never hit the network.
- **Live-test gate: `@skip_if_no_247_live` (env `SDV_PY_247_LIVE=1`)** — the shared 247-family gate defined once in Wave 0 F1, **imported (never redefined) here**. **Reconciled with T3 (see master plan):** even though site-pages is auth-free and curl_cffi `impersonate="chrome"` handles the Fastly TLS block, it is unproven that `www.247sports.com` *fails fast* (rather than silently *hangs*) on datacenter/CI IPs the way `stats.nba.com` does — so both 247 tracks stay CI-off under `SDV_PY_247_LIVE`, never the CI-set `SDV_PY_LIVE_TESTS`. Guessing CI-safe wrong hangs CI; relaxing both later is a trivial follow-up. Keep the tests resilient to upstream flakiness.

## Parallel-contention note

Three files are **shared append points** with sibling tracks (T2 on3, T3 sports247) that may land in overlapping PRs:
- `tools/codegen/generate.py` — `FLAT_APIS` list + `_FLAT_API_DOC` dict.
- `tools/codegen/manual_column_descriptions.yaml` — schema-keyed blocks.
- The canonical `uv run python tools/codegen/generate.py` re-run at integration re-renders ALL flat stems; run it fresh (not from a stale tree) right before the final `--check`.

Land T4's edits to these files in the T4 PR only; rebase on `main` before the final regenerate if a sibling track merged first.

---

## Route inventory (35 routes → 17 schemas, from `site-pages.openapi.yaml`)

Short-name scheme (generator-derived, snake_case; path placeholders → python names `key`/`season`/`school_slug`/`league_id`/`page_id`/`slug`/`league`):

| `short` | path (as emitted, placeholders renamed) | returns_schema | shape |
|---|---|---|---|
| `season_recruits` | `/Season/{season}/Recruits.json` | `recruit` | array |
| `institution` | `/Institution/{key}.json` | `institution` | object |
| `institution_location` | `/Institution/{key}/Location.json` | `location` | object |
| `institution_list` | `/Institution.json` | `institution` | array |
| `institution_timeline_events` | `/college/{school_slug}/Institution/{key}/TimelineEvents.json` | `timeline_event` | array |
| `league_institutions` | `/League/{league_id}/Institutions.json` | `institution` | array |
| `player` | `/Player/{key}.json` | `player` | object |
| `player_high_school` | `/Player/{key}/PlayerHighSchool.json` | `player_institution` | object |
| `player_primary_sport` | `/Player/{key}/PrimaryPlayerSport.json` | `player_sport` | object |
| `player_current_institution` | `/Player/{key}/CurrentPlayerInstitution.json` | `player_institution` | object |
| `player_search` | `/Player.json` | `player` | array |
| `player_institution` | `/PlayerInstitution/{key}.json` | `player_institution` | object |
| `playersport` | `/playersport/{key}.json` | `player_sport` | object |
| `playersport_rank_history` | `/PlayerSport/{key}/RecruitRankHistory.json` | `player_sport_ranking` | array |
| `playersport_institution` | `/PlayerSport/{key}/PlayerInstitution.json` | `player_institution` | object |
| `position_rankings` | `/Position/{key}/playersportrankings.json` | `player_sport_ranking` | array |
| `page_feeds` | `/Page/{page_id}/Feeds.json` | `feed` | array |
| `coach` | `/Coach/{key}.json` | `coach` | object |
| `coach_rankings` | `/Coach/{key}/CoachRankings.json` | `coach_ranking` | array |
| `coach_hometown` | `/Coach/{key}/Hometown.json` | `location` | object |
| `coach_alma_mater` | `/Coach/{key}/AlmaMater.json` | `institution` | object |
| `coach_ranking` | `/CoachRanking/{key}.json` | `coach_ranking` | object |
| `season_current_expert_predictions` | `/Season/{season}/CurrentExpertPredictions.json` | `player_institution_prediction` | array |
| `event` | `/Event/{slug}.json` | `event` | object |
| `recruit_interest` | `/RecruitInterest/{key}.json` | `recruit_interest` | object |
| `season_recruit_interests` | `/Season/{season}/RecruitInterests.json` | `recruit_interest` | array |
| `season_recruit_interest_events` | `/Season/{season}/RecruitInterestEvents.json` | `recruit_interest_event` | array |
| `recruitment_institution` | `/Recruitment/{key}/Institution.json` | `institution` | object |
| `recruitment_interests` | `/Recruitment/{key}/Interests.json` | `institution` | array |
| `recruitment_offers` | `/Recruitment/{key}/Offers.json` | `institution` | array |
| `recruitment_player_sport` | `/Recruitment/{key}/PlayerSport.json` | `player_sport` | object |
| `recruitment_final_choice` | `/Recruitment/{key}/FinalChoice.json` | `player_sport` | object |
| `season_roster_embed` | `/Season/{season}/Roster/Embed.json` | `player_sport` | array |
| `player_institution_evaluation` | `/PlayerInstitutionEvaluation/{key}.json` | `player_institution_evaluation` | object |
| `league_draft_picks` | `/League/{league}/DraftPicks/ConfigureEmbed/.json` | `draft_pick` | array |

17 distinct schemas: `institution`, `location`, `player`, `player_institution`, `player_sport`, `coach`, `coach_ranking`, `player_sport_ranking`, `recruit_interest`, `recruit_interest_event`, `player_institution_prediction`, `draft_pick`, `event`, `player_institution_evaluation`, `recruit`, `timeline_event`, `feed`.

Query params by route: `Season/*/Recruits` → `Items`(int)/`Page`(int)/`Player.FullName`(str)/`Institution`(int); `Institution.json` + `League/*/Institutions` → `items`(int); `Player.json` → `FirstName`(str)/`LastName`(str); `League/*/DraftPicks` → `year`(int)/`round`(int). Python names snake_cased (`items`, `page`, `player_full_name`, `institution`, `first_name`, `last_name`, `year`, `round`), `query_key` = original.

---

## Tasks

### Task 1 — `sports247_site_pages_runtime._get` (curl_cffi Chrome, no JWT, no slash-rewrite)

**Files**
- Create: `sportsdataverse/cfb/sports247_site_pages_runtime.py`
- Test: `tests/test_sports247_site_pages_parsers.py` (runtime section)
- Modify: `pyproject.toml` (`[tool.mypy] files` ratchet)

**Interfaces**
- Produces: `_get(url: str, params: Optional[Dict[str,Any]] = None, headers: Optional[Dict[str,str]] = None, transport: Optional[Transport] = None, proxy_url: Optional[str] = None, **kwargs: Any) -> Union[Dict, List]` and `site_headers() -> Dict[str, str]`.
- `Transport = Callable[[str, dict, dict, Optional[str]], tuple]` → `(status_code, text)`.

**Steps**
- [ ] Write the failing runtime tests first (mirror `tests/test_sports247_parsers.py` runtime block, minus all JWT/auth cases). Key assertions that encode the design deltas from the sibling runtime:
  ```python
  # tests/test_sports247_site_pages_parsers.py  (runtime section)
  def test_get_does_not_append_trailing_slash():
      # site-pages URLs terminate in .json; a trailing slash would 404
      from sportsdataverse.cfb.sports247_site_pages_runtime import _get
      seen = {}
      def fake(url, params, headers, proxy_url):
          seen["url"] = url; return 200, '{"Key": 24099}'
      _get("https://247sports.com/Institution/24099.json", transport=fake)
      assert seen["url"] == "https://247sports.com/Institution/24099.json"  # unchanged

  def test_get_sends_no_authorization_header():
      from sportsdataverse.cfb.sports247_site_pages_runtime import _get
      seen = {}
      def fake(url, params, headers, proxy_url):
          seen["auth"] = headers.get("Authorization"); return 200, "[]"
      _get("https://247sports.com/Institution.json", {"items": 50}, transport=fake)
      assert seen["auth"] is None
      assert "User-Agent" in _get.__globals__  # sanity; real header check below

  def test_get_strips_none_params_and_parses_array():
      from sportsdataverse.cfb.sports247_site_pages_runtime import _get
      seen = {}
      def fake(url, params, headers, proxy_url):
          seen["params"] = params; return 200, '[{"Key": 1}]'
      out = _get("https://247sports.com/Institution.json", {"items": 50, "Page": None}, transport=fake)
      assert out == [{"Key": 1}] and seen["params"] == {"items": 50}

  def test_get_returns_empty_dict_on_non_200_or_bad_json():
      from sportsdataverse.cfb.sports247_site_pages_runtime import _get
      assert _get("https://247sports.com/Institution.json", transport=lambda *a: (403, "")) == {}
      assert _get("https://247sports.com/Institution.json", transport=lambda *a: (200, "<html>")) == {}
  ```
- [ ] Run — fails (module absent): `uv run pytest tests/test_sports247_site_pages_parsers.py -k runtime -q` → `ModuleNotFoundError`.
- [ ] Implement `sports247_site_pages_runtime.py`. Copy `sports247_runtime.py`'s `_curl_transport` (curl_cffi lazy import → `ImportError` w/ install hint; `impersonate="chrome"`, `timeout=30`, proxies) and `site_headers()` (Chrome UA, `Accept: application/json, text/plain, */*`, `Referer`/`Origin` `https://247sports.com`). `_get`: strip `None` params, **do NOT rewrite the path** (no trailing slash), **no JWT/auth loop**, single request, `json.loads` the text, return `dict`/`list` else `{}`; `{}` on non-200 or blank/unparseable body. Module docstring: name the Fastly TLS block + the two deltas vs `sports247_runtime` (no JWT, `.json` paths so no slash-rewrite).
- [ ] Run — passes: `uv run pytest tests/test_sports247_site_pages_parsers.py -k runtime -q`.
- [ ] Append `sportsdataverse/cfb/sports247_site_pages_runtime.py` to `[tool.mypy] files`; `uv run mypy sportsdataverse/cfb/sports247_site_pages_runtime.py` → clean. `uv run ruff check sportsdataverse/cfb/sports247_site_pages_runtime.py`. `git checkout uv.lock` if re-locked.
- [ ] Commit: `feat(cfb): sports247 site-pages runtime (curl_cffi chrome, auth-free)`.

### Task 2 — `parse_sports247_site_page` (FK-surfacing + string-numeric cast) + fixtures

**Files**
- Create: `sportsdataverse/cfb/sports247_site_pages_parsers.py`
- Create: `tests/fixtures/sports247_site_pages/*.json` (copied captures)
- Test: `tests/test_sports247_site_pages_parsers.py` (parser section)
- Modify: `pyproject.toml` (mypy ratchet)

**Interfaces**
- Produces: `parse_sports247_site_page(raw: Optional[Union[Dict[str,Any], List[Dict[str,Any]]]], *, return_as_pandas: bool = False) -> Union[pl.DataFrame, pd.DataFrame]`.

**Steps**
- [ ] Copy the real captures into the fixture dir (source of truth; do NOT hand-author fixtures):
  ```bash
  mkdir -p tests/fixtures/sports247_site_pages
  cp "C:/Users/saiem/Documents/sdv-internal-refs/247sports/captures/site-pages/"*.json \
     tests/fixtures/sports247_site_pages/
  ```
  Add `tests/fixtures/sports247_site_pages/README.md` noting provenance (URL + capture date 2026-07-08, from `sdv-internal-refs/247sports/captures/site-pages/`).
- [ ] Write failing parser tests against the real captures. Ground assertions in the captured bytes (`institution.json`: `{"Key":24099,...,"Latitude":"0.000000","State":44,"Location":32605,...}`; `recruits_season.json`: array with inlined `Player` object → flattens to `player_key`, `player_first_name`, `hometown_state`, …):
  ```python
  FIX = Path(__file__).parent / "fixtures" / "sports247_site_pages"
  def _load(name): return json.loads((FIX / f"{name}.json").read_text(encoding="utf-8"))

  def test_single_object_capture_is_one_row_with_int_fk_and_cast_numerics():
      from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page
      df = parse_sports247_site_page(_load("institution"))
      assert df.height == 1
      assert {"key", "name", "location", "state", "latitude"}.issubset(df.columns)
      assert df.schema["key"].is_integer()        # bare int PK
      assert df.schema["location"].is_integer()   # bare int FK, surfaced, NOT traversed
      assert df.schema["state"].is_integer()      # bare int FK
      assert df.schema["latitude"] == pl.Float64  # string "0.000000" cast to real dtype
      assert df.schema["name"] == pl.Utf8         # genuine string stays Utf8

  def test_array_capture_with_inlined_player_flattens_fk_columns():
      from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page
      df = parse_sports247_site_page(_load("recruits_season"))
      assert df.height > 1
      # inlined Player object -> flattened (sep="_"); bare FK ints surfaced not inlined
      assert {"key", "player_key", "player_full_name", "institution", "player_sport"}.issubset(df.columns)
      assert df.schema["player_key"].is_integer()

  @pytest.mark.parametrize("payload", [None, {}, [], "nope", 3])
  def test_zero_row_on_empty_or_malformed(payload):
      from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page
      assert parse_sports247_site_page(payload).height == 0

  def test_returns_pandas_when_asked():
      import pandas as pd
      from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page
      assert isinstance(parse_sports247_site_page(_load("institution"), return_as_pandas=True), pd.DataFrame)
  ```
- [ ] Run — fails (module absent): `uv run pytest tests/test_sports247_site_pages_parsers.py -k parse -q`.
- [ ] Implement `sports247_site_pages_parsers.py`. One generic parser (the design's "no graph resolver" decision — surface FKs, don't traverse):
  - `_extract_rows(raw)`: `list` → as-is; single `dict` → `[raw]` (detail routes return one object); else `[]`.
  - Flatten with `pandas.json_normalize(rows, sep="_")` (turns inline stubs `Player`/`Hometown`/`PlayerHighSchool` into `player_*`/`hometown_*` columns; bare int FKs remain scalar int columns), snake-case columns via `sportsdataverse.dl_utils.underscore`.
  - Any residual list/dict cell → stringify (defensive; flat schemas shouldn't hit this).
  - `pl.from_pandas(df)`, then `_cast_numeric_strings(df)`: for each `Utf8` column, trial `pl.col(c).cast(pl.Int64, strict=False)`; keep it iff it introduces **no new nulls** (every non-null original parsed); else trial `pl.Float64` under the same rule; else leave `Utf8`. This casts `Latitude`/`CompositeRating`/`OverallRank`-style string-numerics to real dtypes while leaving `Name`/`Height="6-5"`/`Rankable="True"` as Utf8. FK/PK columns already arrive Int64 from `json_normalize` and are untouched.
  - Empty/malformed → `pl.DataFrame()` (zero rows). Google-style docstring w/ `Example:` + `See Also:` recruitR link; note in the docstring that nested entities are bare-FK columns to be traversed via each entity's `.json` sub-route (document the walk, do not build a resolver).
  - `__all__ = ["parse_sports247_site_page"]`.
- [ ] Run — passes: `uv run pytest tests/test_sports247_site_pages_parsers.py -k parse -q`.
- [ ] Append parser module to mypy ratchet; `uv run mypy sportsdataverse/cfb/sports247_site_pages_parsers.py` clean; `uv run ruff check`. `git checkout uv.lock` if re-locked.
- [ ] Commit: `feat(cfb): sports247 site-pages parser (FK-surfacing + string-numeric cast)`.

### Task 3 — Generator `gen_sports247_site_pages.py`

**Files**
- Create: `tools/codegen/gen_sports247_site_pages.py`
- Test: `tests/codegen/test_gen_sports247_site_pages.py` (offline generator test)

**Interfaces**
- Produces (on run): `tools/codegen/endpoints/sports247_site_pages.yaml` + `tools/codegen/schemas/native/sports247_site_pages/<schema>.yaml` (17 files).
- Reads the OpenAPI from `SDV_INTERNAL_REFS_REPO` env (default the local workspace path `C:/Users/saiem/Documents/sdv-internal-refs`).

**Steps**
- [ ] Write a failing idempotence + shape test:
  ```python
  # tests/codegen/test_gen_sports247_site_pages.py
  def test_generator_is_idempotent_and_emits_expected_stem(tmp_path, monkeypatch):
      import importlib, yaml
      gen = importlib.import_module("tools.codegen.gen_sports247_site_pages")
      gen.main()
      ydoc = yaml.safe_load(open("tools/codegen/endpoints/sports247_site_pages.yaml", encoding="utf-8"))
      assert ydoc["api"] == "sports247_site_pages"
      assert ydoc["host"] == "https://247sports.com"
      assert ydoc["name_pattern"] == "sports247_site_pages_{short}"
      assert ydoc["parser_module"] == "cfb.sports247_site_pages_parsers"
      assert ydoc["getter_module"] == "sportsdataverse.cfb.sports247_site_pages_runtime"
      shorts = {e["short"] for e in ydoc["endpoints"]}
      assert {"institution", "season_recruits", "playersport", "league_draft_picks"} <= shorts
      assert len(ydoc["endpoints"]) == 35
      first = open("tools/codegen/endpoints/sports247_site_pages.yaml", encoding="utf-8").read()
      gen.main(); second = open("tools/codegen/endpoints/sports247_site_pages.yaml", encoding="utf-8").read()
      assert first == second  # idempotent
  ```
- [ ] Run — fails: `uv run pytest tests/codegen/test_gen_sports247_site_pages.py -q`.
- [ ] Implement `gen_sports247_site_pages.py`, modeled on `gen_nba_stats.py` (`_write_yaml` with `yaml.safe_dump(..., sort_keys=True)`; `_clean_generated_schema_dir`). Logic:
  - Load `site-pages.openapi.yaml` from `os.environ.get("SDV_INTERNAL_REFS_REPO", <default>) / "247sports/site-pages.openapi.yaml"`.
  - For each `paths[<path>].get`: derive `short` from the route (a small explicit `_SHORT` map keyed on the raw path is clearest and stable — the 35 names in the Route-inventory table above; do not over-engineer a path-parser). Rename path placeholders → snake_case python names and rewrite the emitted `path` string accordingly (`{schoolSlug}`→`{school_slug}`, `{leagueId}`→`{league_id}`, `{pageId}`→`{page_id}`; `{key}`/`{season}`/`{slug}`/`{league}` unchanged). Preserve capitalized literal segments and the trailing `.json` verbatim.
  - `path_params`: from OpenAPI `in: path` params — `name` (snake), `type` (`str` for `{season}`/`{school_slug}`/`{slug}`/`{league}`, else `int`), `required: true`. `{season}` description carries the `{year}-{Sport}` note.
  - `extra_params`: from OpenAPI `in: query` params — `name` (snake), `query_key` (original, e.g. `Player.FullName`), `type` (`int`/`str`), no default.
  - `returns_schema: native/sports247_site_pages/<schema>` (per the Route-inventory table), `parser: parse_sports247_site_page` on every endpoint, `summary` from the OpenAPI `summary`, `example_args` from the route's `x-example-url` where a path arg is needed (e.g. `{"key": 24099}`, `{"season": "2026-Football"}`).
  - Top-level stem keys: `api`, `host`, `name_pattern`, `module: sports247_site_pages`, `parser_module`, `getter_module`, `qualifier: ''`, `passthrough_query: true`, `runtime_imports: [_get]`.
  - Emit 17 returns-schemas: `{schema: <name>, kind: dataframe, columns: [{name: <snake>, type: <r-type>, description: ''}]}` from each OpenAPI component schema's properties. Map OpenAPI type → R-style: `integer`→`integer`, `number`→`numeric`, `string`→`character` (nested inline-stub objects like `Player.Hometown` flatten to `hometown_state`/`hometown_city` columns — enumerate their leaf props). **Descriptions stay `''` here** — authored only in `manual_column_descriptions.yaml` (Task 7).
- [ ] Run — passes: `uv run pytest tests/codegen/test_gen_sports247_site_pages.py -q`.
- [ ] `uv run ruff check tools/codegen/gen_sports247_site_pages.py`. Commit: `feat(codegen): sports247 site-pages generator from OpenAPI spec`.

### Task 4 — Run the generator; commit endpoints YAML + returns-schemas

**Files**
- Create (generated): `tools/codegen/endpoints/sports247_site_pages.yaml`, `tools/codegen/schemas/native/sports247_site_pages/*.yaml` (17).

**Steps**
- [ ] `uv run python tools/codegen/gen_sports247_site_pages.py` → prints `sports247_site_pages: 35 endpoints`.
- [ ] Sanity-check against `247sports/ENDPOINTS.md` (35 rows) and `recruitR-py notes/247links.csv` (Institution/Location/TimelineEvents/League-Institutions/Player/Position/Recruits routes present) — every `247links.csv` route resolves to a generated `short`.
- [ ] `git status` — confirm 1 endpoints YAML + 17 schema YAMLs. Commit: `feat(codegen): generate sports247 site-pages endpoints + returns-schemas`.

### Task 5 — Register in `FLAT_APIS` + `_FLAT_API_DOC`

**Files**
- Modify: `tools/codegen/generate.py`

**Steps**
- [ ] Add `("sports247_site_pages", "cfb")` to `FLAT_APIS` (after the `("sports247", "cfb")` line, ~L1470).
- [ ] Add `"sports247_site_pages": "247Sports Site Pages (247sports.com)"` to `_FLAT_API_DOC` (~L1961). (Shared append points — see contention note; rebase if a sibling track touched these first.)
- [ ] Commit: `feat(codegen): register sports247_site_pages flat-API stem`.

### Task 6 — Regenerate wrappers + docs; drift `--check`

**Files**
- Create (generated): `sportsdataverse/cfb/sports247_site_pages.py`, `docs/docs/cfb/reference/sports247_site_pages.md` (+ cfb index/category refresh).

**Steps**
- [ ] `uv run python tools/codegen/generate.py` (renders all flat stems + docs). Confirm `sportsdataverse/cfb/sports247_site_pages.py` has 35 `sports247_site_pages_*` wrappers wired to `_get` + `parse_sports247_site_page`, and a `docs/docs/cfb/reference/sports247_site_pages.md` grouping appears.
- [ ] Add a wrapper-wiring test to `tests/test_sports247_site_pages_parsers.py`:
  ```python
  def test_wrappers_route_fixtures_through_parser(monkeypatch):
      import sportsdataverse.cfb.sports247_site_pages as sp
      monkeypatch.setattr(sp, "_get", lambda *a, **k: _load("institution"))
      df = sp.sports247_site_pages_institution(key=24099)
      assert isinstance(df, pl.DataFrame) and "location" in df.columns
      assert isinstance(sp.sports247_site_pages_institution(key=24099, return_parsed=False), dict)

  def test_wrappers_exported_from_cfb_package():
      from sportsdataverse.cfb import sports247_site_pages_institution, sports247_site_pages_season_recruits
      assert callable(sports247_site_pages_institution)
  ```
- [ ] `uv run pytest tests/test_sports247_site_pages_parsers.py -q` (all offline tests green).
- [ ] **Drift gate:** `uv run python tools/codegen/generate.py --check` → no drift. `git checkout uv.lock` if re-locked.
- [ ] Commit: `feat(cfb): generate sports247 site-pages wrappers + reference docs`.

### Task 7 — Returns-table descriptions in `manual_column_descriptions.yaml`

**Files**
- Modify: `tools/codegen/manual_column_descriptions.yaml`
- Possibly Modify: `tools/codegen/extract_residual_columns.py` (`_DEFERRED_BUCKETS`) — only if residuals remain.

**Steps**
- [ ] Author a schema-keyed block per the 17 schemas (`institution`, `location`, `player`, `player_institution`, `player_sport`, `coach`, `coach_ranking`, `player_sport_ranking`, `recruit_interest`, `recruit_interest_event`, `player_institution_prediction`, `draft_pick`, `event`, `player_institution_evaluation`, `recruit`, `timeline_event`, `feed`). Seed descriptions from the OpenAPI `description:` fields (many columns already carry them, e.g. `Key`="Primary key of this entity (the id used in its `.json` route)", `Location`="FK -> Location (`/Institution/{Location}/Location.json`)", `CompositeRating`, `OverallRank`, …) + `247sports/site-pages-returns.md`. Keys are the snake_cased column names matching the generated schema output (including flattened `player_*`/`hometown_*` leaves for `recruit`). Note the string-numeric + bare-FK semantics in the FK/rating column descriptions.
- [ ] `uv run python tools/codegen/generate.py --docs` then `--check` — descriptions land in the reference tables, no drift.
- [ ] **Coverage ratchet:** if `extract_residual_columns.py`'s `deferred_columns()` surfaces uncovered `native/sports247_site_pages` columns and the ratchet goes red, EITHER finish authoring the residuals OR add `native/sports247_site_pages` to `_DEFERRED_BUCKETS` as a tracked follow-up (mirroring the `nba_stats` decision) — prefer authoring since the 17 schemas are small and mostly pre-described.
- [ ] Commit: `docs(cfb): sports247 site-pages returns-table descriptions`.

### Task 8 — Live-gated smoke tests

**Files**
- Create: `tests/test_sports247_site_pages_live.py`

**Steps**
- [ ] Add `@skip_if_no_247_live` smoke tests (env `SDV_PY_247_LIVE=1`), tolerant of upstream flakiness/pagination. Use real ids from the OpenAPI `x-example-url`s (`Institution/24099`, `Season/2026-Football/Recruits`, `Player/46051367`, `playersport/218200`):
  ```python
  from tests.conftest import skip_if_no_247_live
  pytestmark = skip_if_no_247_live

  def test_institution_detail_live():
      from sportsdataverse.cfb import sports247_site_pages_institution
      df = sports247_site_pages_institution(key=24099)
      assert df.height == 1 and df.schema["key"].is_integer()

  def test_season_recruits_live():
      from sportsdataverse.cfb import sports247_site_pages_season_recruits
      df = sports247_site_pages_season_recruits(season="2026-Football", items=15, page=1)
      assert df.height >= 0  # tolerate pagination/empty; column contract when populated
  ```
- [ ] Verify offline (skipped without the gate): `uv run pytest tests/test_sports247_site_pages_live.py -q` → skipped. Optionally exercise once locally with `SDV_PY_247_LIVE=1` (needs `curl_cffi`) to confirm the live path.
- [ ] Commit: `test(cfb): sports247 site-pages live smoke tests`.

### Task 9 — Full gate sweep

**Steps**
- [ ] `uv run python tools/codegen/generate.py && uv run python tools/codegen/generate.py --check` — clean (fresh tree; rebase on `main` first if a sibling flat-API track merged, then re-run).
- [ ] `uv run ruff check sportsdataverse/cfb/sports247_site_pages*.py tools/codegen/gen_sports247_site_pages.py tests/test_sports247_site_pages*.py`.
- [ ] `uv run mypy sportsdataverse/cfb/sports247_site_pages_runtime.py sportsdataverse/cfb/sports247_site_pages_parsers.py`.
- [ ] `uv run pytest tests/test_sports247_site_pages_parsers.py tests/codegen/test_gen_sports247_site_pages.py -q` — all green; `tests/test_sports247_site_pages_live.py` skipped.
- [ ] `git status` — `uv.lock` unchanged (`git checkout uv.lock` if the runners re-locked). Optional: add a `sports247_site_pages` example cell to the CFB intro notebook if one exists.
- [ ] Final review with the sdv-toolkit reviewers: `polars-1x-reviewer` (parser), `http-layer-reviewer` (runtime), `returns-table-auditor` + `docstring-auditor` (schemas/docstrings), `provider-shape-mapper` / `port-parity-reviewer` (FK/string-numeric dtype discipline). Open the T4 PR.
