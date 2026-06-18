<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Great Docs on sportsdataverse-py — what actually worked](#great-docs-on-sportsdataverse-py--what-actually-worked)
  - [Reproduce it](#reproduce-it)
  - [Why the first pass concluded it was non-viable — and what was missing](#why-the-first-pass-concluded-it-was-non-viable--and-what-was-missing)
    - [The fix the first pass didn't take](#the-fix-the-first-pass-didnt-take)
  - [What the working build produces](#what-the-working-build-produces)
  - [Great Docs auto-reference vs. the existing Docusaurus codegen reference](#great-docs-auto-reference-vs-the-existing-docusaurus-codegen-reference)
  - [Per-package recommendation (unchanged in spirit, sharpened)](#per-package-recommendation-unchanged-in-spirit-sharpened)
  - [Relationship to `docs-quarto/`](#relationship-to-docs-quarto)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Great Docs on sportsdataverse-py — what actually worked

This note documents a **second, successful** pass at building a real
[Great Docs](https://posit-dev.github.io/great-docs/) site for this package,
and corrects the conclusion reached in the first pass
(`docs-quarto/README.md` → *"The Great Docs attempt — what happened"*).

**Bottom line: real Great Docs auto-discovery DOES work for this package** once
you stop letting it enumerate the full dynamic surface and instead pin an
explicit, curated `reference:` in `great-docs.yml`. The site builds in ~100
seconds with one themed page per documented symbol — signature, parameters,
returns, and example all introspected live from the real objects.

The committed pieces are:

```
great-docs.yml                       # the working config (module + dynamic + curated reference + theme)
user_guide/00-getting-started.qmd    # executable guide: espn_nba_teams() / espn_wbb_teams() live
user_guide/01-scoreboards-and-schedules.qmd  # executable guide: scoreboard + schedule live
assets/logo.png                      # hero logo + auto-generated favicon
great-docs-preview/
  rendered-preview.html              # self-contained guide render (open in any browser, no build)
  great-docs-home.png                # screenshot: themed homepage (hero + navbar + GitHub widget)
  great-docs-reference.png           # screenshot: espn_nba_teams() reference page (sig/params/returns/example)
  great-docs-userguide.png           # screenshot: scoreboards guide with real executed tables
```

The ephemeral `great-docs/` build directory is **gitignored** (regenerated on
every `great-docs build`).

## Reproduce it

```bash
# Prereqs (already present in this env): great-docs 0.14.0, Quarto 1.7.33,
# Python 3.11 with `import sportsdataverse` working.
cd <repo root>            # the dir with pyproject.toml + great-docs.yml
PYTHONUTF8=1 great-docs build      # ~100s → great-docs/_site/index.html
great-docs preview                 # local server on http://localhost:3000
```

> **Windows note:** export `PYTHONUTF8=1` first. Without it the CLI crashes on a
> Unicode glyph in its progress output (`UnicodeEncodeError`).
>
> **Jupyter-kernel note:** the user-guide `.qmd` pages pin `jupyter: python3` in
> their front matter. This machine has three Python Jupyter kernels installed
> and Quarto otherwise auto-selects the *first* language=python kernel, which is
> a stale Anaconda env (`tf-gpu36`) whose interpreter no longer exists — that
> produces `[WinError 2] The system cannot find the file specified`. Pinning
> `python3` (the PATH `python`, which has `sportsdataverse` + `polars` +
> `ipykernel`) fixes it.

## Why the first pass concluded it was non-viable — and what was missing

The first pass let `great-docs init` / `scan` / `build` run **auto-discovery**
over the whole package, and they timed out. That observation was correct but the
*conclusion* (auto-reference discovery is structurally impossible here) was too
strong. Here is the actual mechanism, traced through the Great Docs 0.14.0
source (`great_docs/core.py`):

1. `import sportsdataverse` registers **~2,940 public names** at import time
   (2,678 functions, 163 submodules, 9 classes) via `make_league_module()`.
   There is no static `__all__`.
2. Auto-discovery reaches *"Discovered 2774 public names"* quickly — that part
   is **not** the bottleneck (resolving every name's `inspect.signature` +
   docstring takes ~0.1s total).
3. The bottleneck is the **"super-safe filtering" loop** in
   `_discover_package_exports`: it calls the renderer's
   `get_object("sportsdataverse:<name>")` — **griffe with `dynamic=True`** — on
   *every* discovered name to validate it can be documented. Each call costs
   **~2 seconds** for these dynamically-bound `functools.partial` / closure
   wrappers. 2,769 names × 2s ≈ **90 minutes** → every `init`/`build` times out.
4. The same per-name griffe cost recurs in **"View source" link generation**
   (`source.enabled: true`), which independently re-walks all 2,774 names.

So the dynamic surface does not *defeat* discovery; it just makes the
**O(n) × 2s validation walk** intractable at n≈2,800.

### The fix the first pass didn't take

Great Docs has a documented escape hatch — an explicit `reference:` block — and
`_create_api_sections_with_config()` **prioritizes it over auto-discovery**.
When `reference:` is set, Great Docs builds sections directly from the listed
names via the *lightweight* `_categorize_referenced_objects()` path and **skips
the 2,769-call validation loop entirely**. The per-symbol griffe resolution
still happens, but only for the ~95 names you actually document (a few minutes
of Quarto render, not 90). Pairing that with `source.enabled: false` removes the
other unscoped walk.

That is exactly what `great-docs.yml` does:

```yaml
module: sportsdataverse
parser: google
dynamic: true
source: {enabled: false}     # avoid the unscoped "View source" griffe walk
reference:                    # curated -> skips the slow auto-discovery loop
  sections:
    - {title: NBA,  contents: [espn_nba_teams, espn_nba_scoreboard, ...]}
    - {title: WNBA, contents: [...]}
    ...                       # 8 sports + NFL loaders + PBP/config classes
```

Each listed name is a **real top-level export** with a Google-style docstring
(all 64 curated `espn_<sport>_*` functions have full `Args:` / `Returns:` /
`Example:` blocks), so dynamic introspection produces complete pages.

## What the working build produces

- **19/19 build steps, ~100s**, `great-docs/_site/index.html` + 83 HTML pages.
- **One reference page per documented symbol** (e.g.
  `reference/espn_nba_teams.html`): the function **signature is the page
  title**, with **Parameters** (`return_as_pandas: bool = False`), **Returns**
  (`pl.DataFrame …`), a full **Example** block, and a **See Also** with the
  hoopR / nba_api cross-links — all introspected live, not hand-written.
- **An API-reference index** grouping the curated surface by sport, with the
  sidebar search/filter box.
- **Two executable user-guide pages** whose Python cells run against the **live
  ESPN API at build time** and freeze the real output into static HTML:
  `espn_nba_teams() returned 30 rows x 14 columns`, a scoreboard table with
  *MIN @ BOS / Boston Celtics / Final/OT*, and build-time-computed prose
  (*"10 games on 2024-01-10; the home team won 6 of them."*).
- **Theme:** gradient navbar (`navbar_style: slate`), `content_style: sky`,
  `dark_mode_toggle: true`, hero with logo + tagline, GitHub stars widget,
  Quarto overlay search, auto-generated favicon, and the default `llms.txt` /
  `llms-full.txt` / `skill.md` agent-context files.

## Great Docs auto-reference vs. the existing Docusaurus codegen reference

| Dimension | Great Docs (this build) | Docusaurus codegen (`docs/`, production) |
|---|---|---|
| Reference source of truth | **Live Python objects** — signature, params, returns, example introspected at build from the real function | **Endpoint YAML** (`tools/codegen/`) + hand-written returns-schemas → markdown |
| Coverage model | Curated allow-list (must list each name); the ~800 dynamic wrappers are *not* auto-enumerated | Full generated long-tail (every endpoint), driven by YAML — better breadth |
| Returns documentation | The docstring `Returns:` prose + a runnable example; **no per-column type table** | Rich `col_name \| type \| description` returns tables (the codegen's strength) |
| Drift risk | Low for signatures (introspected); examples can break loudly if an API changes | Schema tables are hand-maintained and *can* silently drift from the real frame |
| Executable output | **Yes** — guide cells render the real DataFrame at build time | No — schema tables only; no live execution |
| Authoring cost | ~Zero per documented symbol (just list the name) | YAML + schema authoring per endpoint |
| Theme / search / llms.txt | Batteries-included (gradient navbar, dark mode, search, llms.txt, SKILL.md) | Custom Docusaurus theme + React Playground + TypeDoc analogue (JS site) |

**Reading:** Great Docs' autodoc is genuinely good and **does** work here — its
sweet spot is the *curated public surface* (the per-sport entry points people
actually call) plus executable guides. The Docusaurus codegen still wins on
**breadth** (the full ~800-wrapper long tail) and on **per-column returns
tables**, because those are driven by the endpoint YAML rather than by listing
names by hand. They are complementary, not a strict upgrade either way.

## Per-package recommendation (unchanged in spirit, sharpened)

- **Python (`sportsdataverse-py`):** Great Docs is now a **real option**, not
  just a fallback. Use it for a curated, executable, batteries-included docs
  experience (hero, search, dark mode, llms.txt, live DataFrames). Keep the
  **endpoint-YAML codegen** for the full long-tail reference + the per-column
  returns tables Great Docs doesn't produce. A hybrid — Great Docs for the
  curated/executable surface, codegen for breadth — captures both.
- **R sisters (`hoopR`, `wehoop`, `cfbfastR`, `baseballr`, `fastRhockey`):**
  Quarto remains the natural fit (native R literate docs, and it runs Python
  cells too).
- **JS (`sportsdataverse-js`):** keep Docusaurus — the React Playground and
  TypeDoc surface have no Quarto/Great Docs equivalent.

## Relationship to `docs-quarto/`

`docs-quarto/` is the **first pass**: a hand-built clean Quarto `website`
project (the documented fallback) that mirrors the production Docusaurus
structure. It is still a valid, separate experiment and is kept as-is. This pass
(`great-docs.yml` + `user_guide/` + `great-docs-preview/`) is the **real Great
Docs tool**, configured to work against the dynamic surface. Where the
`docs-quarto/README.md` says Great Docs auto-reference "is non-viable," **this
note supersedes that** with the explicit-`reference:` recipe above.
