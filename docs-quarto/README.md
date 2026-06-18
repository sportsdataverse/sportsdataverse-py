<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Quarto + Great-Docs conversion — `docs-quarto/`](#quarto--great-docs-conversion--docs-quarto)
  - [What this is](#what-this-is)
  - [Site structure](#site-structure)
  - [How to render it yourself](#how-to-render-it-yourself)
  - [Rendered preview & screenshots (no build needed)](#rendered-preview--screenshots-no-build-needed)
- [Full-conversion evaluation: Quarto + Great Docs vs. Docusaurus](#full-conversion-evaluation-quarto--great-docs-vs-docusaurus)
  - [The Great Docs attempt — what happened](#the-great-docs-attempt--what-happened)
  - [What converted cleanly](#what-converted-cleanly)
  - [What was awkward or did not convert](#what-was-awkward-or-did-not-convert)
  - [Executable-cell reference vs. the Docusaurus schema-table reference](#executable-cell-reference-vs-the-docusaurus-schema-table-reference)
  - [Migration-effort estimate](#migration-effort-estimate)
  - [Per-package recommendation](#per-package-recommendation)
  - [The hybrid worth considering](#the-hybrid-worth-considering)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Quarto + Great-Docs conversion — `docs-quarto/`

## What this is

A **full documentation-site conversion** of `sportsdataverse-py` to
[Quarto](https://quarto.org), built to evaluate Quarto's literate,
*run-it-for-real* model against the production
[Docusaurus](https://py.sportsdataverse.org) site under `docs/`. It extends the
earlier single-page proof-of-concept into a complete, navigable site that mirrors
the real docs structure (Home / Guides / Reference / Architecture, with site
search).

The headline property is unchanged from the PoC and is the whole point:
**every table and number on the site is the genuine output of `sportsdataverse`
code, executed once at build time** (`quarto render`) and frozen into static
HTML — no client-side Python, no serverless proxy, no hand-maintained "expected
output" that can silently rot.

> **Great Docs note.** The brief asked to try
> [**Great Docs**](https://posit-dev.github.io/great-docs/) (Posit's Quarto
> documentation system) first, and to fall back to a clean Quarto `website`
> project if it genuinely would not work. Great Docs **installed fine**
> (`pip install great-docs`, v0.14.0) and its render engine partially works, but
> its **package auto-discovery is non-viable against this package's
> dynamically-generated, submodule-namespaced 800-wrapper surface** — every
> discovery path either times out or fails to resolve the dynamic names. The
> site here therefore uses a **clean Quarto `website` project** (the documented
> fallback). Full detail in [the evaluation](#the-great-docs-attempt--what-happened).

## Site structure

```
docs-quarto/
  _quarto.yml            # website project: navbar + sidebar + overlay search, light/dark themes
  index.qmd              # Home: what it is, install (pip/uv/conda), executable surface tour
  guides/
    index.qmd            # Guides overview
    installation.qmd     # install + verify import (executable)
    teams-and-scoreboard.qmd   # uniform espn_<sport>_teams() + past-dated scoreboard (executable)
    schedule-and-rosters.qmd   # team_id flow → schedule + roster (executable)
    parsed-dataframes.qmd      # return_parsed / return_as_pandas, sibling-API parsers (executable)
  reference/
    index.qmd            # Reference overview
    mlb.qmd              # hand-written MLB reference (sig + params + returns + REAL df output) ×4 fns
    nba.qmd              # hand-written NBA reference (same shape) ×3 fns
    generated/           # 21 .qmd stubs emitted by _gen_reference.py (codegen → .qmd proof)
      index.qmd
      *.qmd
  architecture.qmd       # ESPN cross-league core + parser layer + codegen pipeline + ecosystem
  _gen_reference.py      # the tiny generator: endpoint YAML + live signatures → reference/generated/*.qmd
  _freeze/               # committed executed-output cache (the proof the cells ran against live data)
  rendered-preview.html  # self-contained snapshot of a guide page (open in any browser, no build)
  quarto-home.png        # screenshot: Home
  quarto-guide.png       # screenshot: Teams & scoreboard guide (real tables)
  quarto-generated-ref.png  # screenshot: a generated reference stub with real executable output
```

**Reference generated for:** **MLB** (hand-written page for 4 functions + 21
generator-emitted stubs) and **NBA** (hand-written page for 3 functions). The
generator (`_gen_reference.py`) is single-sport (MLB) on purpose — it proves the
codegen → `.qmd` path without re-authoring all 50 leagues.

`quarto render` produces **32 HTML pages** with no execution errors; the
executable cells' real output (team names like *Celtics* / *Dodgers*, column
headers like `team_display_name`, inline-computed counts) is present in the HTML,
and the navbar + overlay search (108-doc `search.json` index) are wired up.

## How to render it yourself

Quarto executes Python cells through **Jupyter** (`nbclient` + `ipykernel`),
using whichever Python the `python3` kernel resolves to — in this repo that
interpreter already imports `sportsdataverse`, so no kernel override is needed.

```sh
cd docs-quarto
quarto render            # -> _site/  (32 pages); re-run is cache-aware via _freeze/

# regenerate the MLB reference stubs from package metadata:
python _gen_reference.py # -> reference/generated/*.qmd

# a single self-contained file:
quarto render index.qmd --to html --embed-resources --output preview.html
```

Requirements: `quarto` (>= 1.7), and `jupyter nbclient ipykernel` available to
the Python that imports `sportsdataverse`.

## Rendered preview & screenshots (no build needed)

- **`rendered-preview.html`** — a self-contained (`embed-resources`) render of a
  guide page. Open it in any browser to see the real tables with **no build
  step**.
- **`quarto-home.png` / `quarto-guide.png` / `quarto-generated-ref.png`** —
  screenshots of the rendered site (Home, a guide with real tables, and a
  generated reference stub with real executable output).

---

# Full-conversion evaluation: Quarto + Great Docs vs. Docusaurus

This is an honest comparison scoped to the SportsDataverse stack, not a generic
"which static-site generator is best" piece.

## The Great Docs attempt — what happened

Great Docs is a `pip install great-docs` CLI (`init` / `build` / `preview`) that
uses Quarto under the hood. It auto-discovers a package's public API from
`__all__` / `dir()` / static analysis, detects the docstring style, and
generates a full reference site with **zero manual authoring**. For a
conventional package that is a genuinely excellent story.

Against `sportsdataverse-py` it does not hold up, and the reason is structural,
not cosmetic:

| Step tried | Result |
|---|---|
| `pip install great-docs` | **OK** (v0.14.0). One fix needed: it requires `griffe >= 1.x`; this env had `griffe 0.47` (missing `griffe.Expr`), so `pip install "griffe>=1,<2"` was required. |
| `great-docs init` (auto-discovery) | **Timed out at 220s** in "dynamic introspection" — it walks the full package surface (50 leagues × ~120 wrappers each = 800+ dynamically-registered names). |
| `great-docs scan` (scoped, static) | **Timed out at 90s.** |
| `great-docs build --no-refresh`, static, scoped `reference:` to `sportsdataverse.nba.espn_nba_teams` | **Resolution failure:** static `griffe` analysis can't see names created at import time by `make_league_module`, so the dotted item "is not found". |
| `great-docs build --no-refresh`, dynamic, same scoped item | **Same resolution failure** — the resolver reports the missing item as the bare package `sportsdataverse`, i.e. it expects flat top-level members, not submodule-namespaced dynamic names. |
| `great-docs build --no-refresh`, sections-only, minimal README + one guide | **Timed out at Step 1/19 (200s)** — the build machinery still performs a heavy package pass. |

The root cause is the package's own architecture (documented in `CLAUDE.md`):
the ESPN surface is **one core + N thin extensions**, where
`make_league_module(sport, league, prefix, namespace)` *dynamically registers*
`espn_{prefix}_{short}` names into each league module at import time. There is no
static `__all__` of flat names for an introspection tool to discover, and a
runtime walk of all 800 is too heavy. Great Docs' model — *point it at one
package object and discover its members* — is a poor fit for a 50-submodule,
codegen-generated surface.

**What did work from Great Docs:** the build reached **18 of 19 steps** before
the reference step failed — it successfully built `index.qmd` from `README.md`,
wrote a themed `_quarto.yml`, and copied its full theme (gradient navbar, dark
mode, fonts, sidebar search, GitHub widget). So the *renderer/theme* is usable;
only the *auto-reference discovery* is not. Per the brief's fallback clause, the
site here is a clean Quarto `website` project that reproduces the same structure
by hand, and documents this deviation (here).

## What converted cleanly

- **Conceptual / narrative pages.** Home, Guides, and Architecture are prose +
  code; they map onto `.qmd` essentially 1:1 from the Docusaurus `intro.md` /
  `tutorials/` content. No MDX/React was in use on those pages, so nothing was
  lost.
- **Executable examples — strictly better.** The Docusaurus tutorials show code
  blocks; Quarto **runs them** and freezes the real polars/pandas frame into the
  page. This is the single biggest upgrade and the core reason to consider Quarto
  for a data package.
- **Navigation + search.** `_quarto.yml` gives a navbar, a collapsible sidebar
  per section, and a built-in lunr overlay search (108 docs indexed) with no
  extra config — parity with Docusaurus' sidebar + Algolia/local search.
- **Light/dark themes.** `theme: {light: cosmo, dark: darkly}` ships a dark-mode
  toggle out of the box — parity with Docusaurus.
- **Reproducibility / cache.** `execute: freeze: auto` caches executed output in
  `_freeze/`, so re-renders don't re-hit ESPN unless the source changes — and CI
  can fail if a doc's output drifts, turning examples into a lightweight
  integration test. Docusaurus has no equivalent.
- **The codegen → page path survives.** `_gen_reference.py` shows the existing
  endpoint YAML metadata can target `.qmd` exactly as it targets Docusaurus
  markdown (21 MLB stubs generated, each with real signature + params + parser +
  schema, and an executable example for the safe-to-call endpoints).

## What was awkward or did not convert

- **Auto-reference for the dynamic surface.** Covered above — the headline Great
  Docs feature is unusable here. A real conversion would keep the **existing
  endpoint-YAML codegen** and retarget its Jinja templates to emit `.qmd` instead
  of Docusaurus markdown (the `_reference_block.jinja` → `.qmd` swap is
  mechanical). `_gen_reference.py` is the proof of concept for that.
- **The React Playground has no Quarto analogue.** The JS docs' in-browser,
  serverless-proxy *Playground* (`docs/api/run.mjs`) runs code on the reader's
  click. Quarto's executable cells run at **build time**, not on click — a
  different (and for a data package, arguably better, because it's verified)
  guarantee, but **not** an interactive playground. There is no drop-in
  replacement; an interactive playground would need an external service
  (JupyterLite / Pyodide) bolted on.
- **TypeDoc-equivalent.** The JS package generates API reference from TypeScript
  types via TypeDoc. Great Docs *would* be the Python analogue (autodoc from
  signatures + docstrings) — and it's exactly the piece that failed here. So the
  "automatic API reference from types" story is the weakest part of the Quarto
  conversion for *this* package specifically.
- **Versioning + multi-version selector.** Docusaurus' `versioned_docs/` +
  `lastVersion` freeze a per-release snapshot with a version dropdown. Quarto has
  no first-class multi-version website feature; Great Docs adds one
  (`versions:` in `great-docs.yml`), but since Great Docs isn't viable here, a
  fallback Quarto site would need a hand-rolled approach (separate `profile`
  builds or a Netlify/Vercel alias scheme).
- **Windows console encoding.** Great Docs' CLI crashed on a Unicode checkmark
  (`'charmap' codec can't encode '✓'`) until `PYTHONUTF8=1` was set — a
  minor but real friction on Windows.

## Executable-cell reference vs. the Docusaurus schema-table reference

This is the crux of the evaluation. Compare the two reference styles directly:

| Aspect | Docusaurus (today) | Quarto executable (here) |
|---|---|---|
| Source of truth | `returns_schema` YAML (`col_name \| type \| description`) | the **live call** + its real frame |
| What the reader sees | an accurate **column list** | the accurate column list **plus the actual `df.head()`** |
| Drift risk | schema can silently lag the parser | re-render fails if the output shape changes |
| Authoring cost | one YAML schema per endpoint | one executable example per function |
| Coverage at scale | mechanical, covers all 800 endpoints | mechanical for the cell, but each example *runs* (slower builds, needs network/freeze) |
| Offline build | always | needs a live endpoint or a frozen `_freeze/` cache |

The hand-written `reference/mlb.qmd` and `reference/nba.qmd` show the upside:
each function carries the same signature + params + returns table the Docusaurus
page has, **and** an executable example whose output is the genuine frame. The
generated `reference/generated/*.qmd` stubs show the mechanical path — and make
the trade-off visible: the schema-table style scales to 800 endpoints cheaply,
while the executable style is the most valuable on the *common* endpoints a
reader actually copies (`teams`, `scoreboard`, `team_roster`). A pragmatic
conversion would generate schema-table reference for the long tail and add an
executable example block to the high-traffic endpoints.

## Migration-effort estimate

Assuming the **fallback** path (clean Quarto `website`, not Great Docs):

| Work item | Effort |
|---|---|
| Conceptual pages (intro, ~15 tutorials → guides, parsers, architecture) | **1–2 days** — mostly `.md` → `.qmd` + turn code fences into executable cells; verify each call exists and is stable/past-dated. |
| Retarget the codegen reference (`_reference_block.jinja` + page templates → `.qmd`) and wire the per-league sidebar | **3–5 days** — the data model already exists; it's a template + output-path swap plus the `_quarto.yml` sidebar generation. |
| Add executable examples to high-traffic endpoints + a `freeze` cache strategy for CI | **2–3 days** — pick the ~30–50 stable endpoints, past-date the date-bound calls, commit `_freeze/`. |
| Versioning + deploy (Vercel) + search/SEO parity | **2–3 days** — hand-rolled multi-version scheme since there's no first-class feature. |
| Replacing the JS Playground (if wanted) | **not estimated** — out of scope for Python/R; would need JupyterLite/Pyodide. |

**Total ≈ 8–13 engineering-days** for the Python package to reach
feature-parity-minus-Playground, with the executable-reference upgrade as the
payoff. If Great Docs ever supports submodule-namespaced dynamic discovery (or
the package grows a static `__all__` registry of its generated names), the
reference half collapses to near-zero authoring and the estimate drops sharply.

## Per-package recommendation

- **Python (`sportsdataverse-py`): Quarto is compelling — fallback mode, not
  Great Docs (yet).** The value of these docs is *tidy DataFrames out of live
  endpoints*; build-time executable cells show the **real** frame instead of a
  schema table that can drift. Recommend a clean Quarto `website` that **reuses
  the existing endpoint-YAML codegen** (retargeted to `.qmd`) for the reference
  long tail, plus executable examples on high-traffic endpoints. Re-evaluate
  Great Docs once it (or the package) can discover the dynamic surface.
- **R sisters (`hoopR`, `wehoop`, `cfbfastR`, `baseballr`, `fastRhockey`):
  Quarto is the natural fit** — it's the native literate-docs tool for R, and a
  single Quarto toolchain could document the **R and Python** packages with the
  same run-it-for-real model.
- **JS (`sportsdataverse-js`): keep Docusaurus.** The React Playground and
  TypeDoc surface have no Quarto equivalent, and the existing codegen → markdown
  → autosidebar pipeline + versioned docs are mature.

## The hybrid worth considering

The cheapest way to capture Quarto's best idea **without switching toolchains**
is a **build-time "real output" injector** in `tools/codegen`: execute a curated
set of example snippets and inject the *actual* `df.head()` output (as a markdown
table) into the generated Docusaurus reference pages — the literate-docs
guarantee, while keeping the single Docusaurus site, the autosidebar, and the
versioning. Quarto then becomes an optional second output (a unified R+Python
docs experiment, or a PDF/whitepaper export) rather than a replacement.
