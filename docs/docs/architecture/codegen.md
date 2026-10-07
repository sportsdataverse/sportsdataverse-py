---
title: Codegen
sidebar_label: Codegen
description: "How sdv-py's codegen produces both the API wrappers and this docs site, and how the two pipelines differ."
---
# Codegen

`tools/codegen/generate.py` is one CLI with two jobs: it emits the package's API
wrappers, and it emits this docs site. They share the same inputs and the same
drift gate, but they are not the same pipeline — see
[How the two differ](#how-the-two-differ).

## Inputs

| Input | What it carries |
|---|---|
| `endpoints/leagues.yaml`, `endpoints/parameters.yaml` | the league registry (30 leagues) and the shared parameter catalog |
| the five ESPN YAMLs in `generate.ESPN_APIS` | `espn_site_v2`, `espn_web_v3`, `espn_core_v2`, `espn_fitt_v3`, `espn_cdn` |
| the 36 flat-API YAMLs in `generate.FLAT_APIS` | one non-ESPN live API family each (NHL ×4, MLB ×2, NFL ×5 including both PFF APIs and Sleeper, F1, basketball, soccer, recruiting, prediction markets, TheSportsDB, ESPN content, the vendor networks) |
| `endpoints/releases.yaml` | every dataset loader, its release `base`, tag and asset URL |
| `espn_rename_map.yaml` | the ESPN short-name renames |
| `sources.yaml` | provider / category registry: which source each public function belongs to |
| `schemas/<name>.yaml`, `schemas/<name>/<league>.yaml`, `schemas/native/<stem>/`, `schemas/autodoc/<league>/` | the returns tables, in four shapes |
| `manual_column_descriptions.yaml`, `r_column_descriptions.yaml` | column descriptions (captured → manual → R, in that precedence) |
| `r_exports.yaml`, `r_parity_aliases.yaml`, `highlights.yaml`, `autodoc_example_args.yaml`, `coverage_allowlist.yaml` | parity tables, curated "start here" sets, capture args, gate exclusions |

Upstream one-shot generators write some of those inputs rather than being run per
build: `gen_asa` / `gen_cbs` / `gen_mls` / `gen_nwsl` / `gen_on3` / `gen_pff` /
`gen_pff_api` / `gen_sports247*` / `gen_yahoo` and `openapi_to_endpoints` turn
vendor OpenAPI specs into endpoint YAML; `gen_nba_stats` reads
`inputs/nba_canonical_catalog.json`; `vendor_captures.py` copies sdv-internal-refs
captures into `tests/fixtures/`; `gen_*_descriptions.py` mine column descriptions.
`extract*.py` are historical bootstraps and no longer run.

## API generation

A no-arg `uv run python tools/codegen/generate.py` runs six stages in order, and
each one `ruff format`s exactly what it wrote:

| Stage | Writes |
|---|---|
| `build()` | the `tools/codegen/_generated/` staging copies |
| `build_live()` | `sportsdataverse/<league>/<prefix>_espn_ext.py` + the container `__init__` files |
| `build_parsed_live()` | `sportsdataverse/parsed/*.py` (the deprecated aliases) |
| `build_flat_live()` | `sportsdataverse/<league>/<stem>.py` from `api_module.py.jinja` |
| `build_loaders_live()` | `sportsdataverse/<lg>/<lg>_loaders.py` for the 8 `_GENERATED_LOADER_LEAGUES` |
| `build_docs()` | the docs tree (below) |

Refresh flags are separate, deliberate, mostly-networked passes:
`--schemas` (introspect the parsers against captured fixtures, offline),
`--loader-schemas` (re-read release parquet footers), `--autodoc-schemas`
(call the hand-written DataFrame functions live), `--audit-releases`
(diff `releases.yaml` against the live release list), `--coverage` (the
docs-coverage report).

## Docs-site generation

`build_docs()` → `_render_docs_all()` owns `docs/docs/<league>/`,
`docs/docs/reference/`, the generated span of `docs/docs/intro.md`,
`docs/static/anchor-map.json`, `docs/src/data/leagues.json` and the three
changelog pages under `docs/src/pages/`. Conceptual pages outside those roots
(this page, `intro.md`'s prose, `quality-of-life.md`, `architecture/`, `parsers/`)
are hand-authored and preserved.

- **Per-league pages.** One `reference/<slug>.md` per API (via
  `render_reference_page`), `reference/loaders.md`, `reference/additional.md`
  (`render_autodoc_page`, which introspects the importable package), then
  `index.md` (`render_league_index`): the Data sources table, one section per
  provider, Tools and helpers, Examples, Python ↔ R parity.
- **Sources.** Every row and section comes from `sources.yaml` through
  `sources.resolve()`; the Additional page's family headings are registry labels in
  registry order.
- **Family split and anchor map.** A page over `_PAGE_MD_BUDGET` (70 KB of
  markdown ≈ 300 KB of built HTML) keeps its URL as an overview and moves its
  function blocks to one page per family under `reference/<page>/`.
  `docs/static/anchor-map.json` maps every old page URL's anchors to their new
  family slug, and `docs/src/clientModules/anchorForward.ts` forwards a stale deep
  link at runtime.
- **League registry.** `render_leagues_json()` writes `docs/src/data/leagues.json`
  — 58 leagues across six sports — which drives the sidebar's
  `sidebarItemsGenerator`, the home page's league grid and the per-league search
  contexts in `docusaurus.config.ts`.
- **Autodoc pages.** The *coverage gap*: in-scope public names not documented by
  any other generated page, rendered from live signatures and docstrings, with the
  returns table read offline from `schemas/autodoc/<scope>/<fn>.yaml`.
- **Changelog render.** `tools/hooks/sync_docs_changelog.py` splits the root
  `CHANGELOG.md` into three pages; `--check` covers their drift too.
- **Gates.** `_coverage_gaps()` fails on a public function that reaches no docs
  page; `_source_gaps()` fails on one that resolves to zero or two `sources.yaml`
  entries.

## How the two differ

| | API generation | Docs-site generation |
|---|---|---|
| Unit of work | one file per spec object | whole trees, full-clobbered, stale files deleted |
| Source of truth | the YAML only | the YAML **plus** the importable package (live signatures + docstrings) |
| Failure mode | a stale wrapper | a stale page, an orphan file, or an undocumented/unsourced function |
| Enforcement | byte-diff | byte-diff **plus** the coverage and sources gates |

## The drift gate

`uv run python tools/codegen/generate.py --check` re-renders everything to a temp
dir and byte-diffs it, orphan-checks the generated roots, and runs both gates. It
runs in three places:

- `.github/workflows/codegen.yml`, alongside `uv run pytest tests/codegen`;
- the `sdv-codegen` pre-commit hook;
- the `codegen-drift-prepush` pre-push hook, which also watches `docs/docs`.

Both hooks' `files:` filters match `tools/codegen/(endpoints|schemas|templates)/**`,
`tools/codegen/*.py` and `CHANGELOG.md` — so editing `generate.py` or the
changelog triggers the gate, which it did not before 0.1.5.

Regenerate before pushing. A stale tree is green locally and red in CI.

## Downstream

`tools/codegen/build_docs_index.py` imports `generate` and `spec` to build the
sdv-docs MCP retrieval index (`sdv_docs_v1.sqlite`), published to the rolling
`docs-index` release by `.github/workflows/docs-index.yml`. The index is a build
artefact and is never committed; its inputs are already drift-gated here.
