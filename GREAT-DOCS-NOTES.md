<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Great Docs on sportsdataverse-py — what actually worked](#great-docs-on-sportsdataverse-py--what-actually-worked)
  - [Brand + structural parity with the Docusaurus site](#brand--structural-parity-with-the-docusaurus-site)
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
great-docs.yml                       # the working config (theme + brand head + generated reference + user_guide)
assets/head.html                     # brand <head>: Plausible + fonts + glow (parity with the Docusaurus site)
assets/favicon.ico                   # brand favicon copied from docs/static/img/
assets/logo.png                      # hero logo
user_guide/getting-started.qmd       # executable guide: espn_nba_teams() / espn_wbb_teams() live
user_guide/scoreboards-and-schedules.qmd  # executable guide: scoreboard + schedule live
user_guide/02-quickstart.qmd … 16-other-espn-leagues-intro.qmd  # 15 tutorials ported from docs/docs/tutorials/
tools/convert_tutorials_to_qmd.py    # one-shot tutorial md -> qmd converter (re-runnable)
tools/codegen/great_docs_reference.py  # generates great-docs.yml's reference: block from leagues.yaml
great-docs-preview/                  # screenshots + a self-contained render (no build needed)
```

The ephemeral `great-docs/` build directory is **gitignored** (regenerated on
every `great-docs build`).

## Brand + structural parity with the Docusaurus site

A second round brought the Great Docs site to visual + structural parity with the
production Docusaurus site (`docs/`), all **additive** to the config:

- **Brand `<head>` via `include_in_header`** (`assets/head.html`, wired as
  `include_in_header: [{file: assets/head.html}]`). Ports, verbatim from
  `docs/docusaurus.config.ts` + `docs/src/css/custom.css`: the **Plausible**
  analytics tag (`data-domain: py.sportsdataverse.org`), the **Bungee / Chivo /
  Lato / Fira Mono** font stack (+ the FiraCode CDN), and the cyan **glow**
  `@keyframes` (`text-shadow: 0 0 10px #427FD4, 0 0 11px #39F8FF`), retargeted to
  Quarto/Bootstrap selectors (`.navbar-brand`, `h1`/`h2`, `code`). Verified in the
  built `_site/index.html <head>`.
  > **Gotcha:** pass the include as `{file: assets/head.html}`, **not** a bare
  > string. Great Docs' config normalizer turns a bare string into a `{text: ...}`
  > entry, so Quarto injects the *literal path* instead of reading the file.
- **Favicon** copied byte-for-byte from `docs/static/img/favicon.ico` and pinned
  via `favicon:`. `navbar_style: slate` (the dark gradient the glow sits on) is
  kept — a solid `navbar_color` would override it.
- **15 tutorials** ported from `docs/docs/tutorials/NN_*.md` to
  `user_guide/NN-*.qmd` by `tools/convert_tutorials_to_qmd.py`. Their cells are
  `#| eval: false` (they hit live ESPN / native / odds / Statcast APIs — keeping
  them non-executing makes the build fast + non-flaky). The two *getting-started*
  guides stay **executable** (real build-time output). Two explicit sidebar
  sections: "User guide" (the executable pair) + "Tutorials" (the 15).
  > **Honest call:** the upstream `01_quickstart` is a heavyweight all-sport tour
  > (29 cells across every league, some needing an odds API key). Executing it at
  > build time would be slow and non-deterministic, so it — like the rest — is
  > `eval: false`; the executable role is filled by the two small guides.
- **Codegen-generated reference.** The `reference:` block is now generated by
  `tools/codegen/great_docs_reference.py` from
  `tools/codegen/endpoints/leagues.yaml` (spliced between
  `# >>> generated reference` / `# <<< generated reference` markers), so it stays
  in sync and covers **every** league — the original 8 ESPN sports **plus**
  men's/women's college hockey, college baseball/softball, UFL/XFL/CFL, soccer,
  and cricket (17 league sections). It emits only `espn_<prefix>_<short>` names
  that are **real importable top-level exports** (Great Docs fails on a
  non-importable name) — the high-value entry points per league, not the full
  ~800-wrapper surface (which would bloat the build). The NFL-loaders +
  PBP/config tail sections are preserved.

  Regen + drift guard (also folded into the main `--check` CI gate):

  ```bash
  python tools/codegen/generate.py --great-docs          # regenerate the block
  python tools/codegen/generate.py --great-docs --check  # rc=1 if stale
  ```

  > **Gotcha:** the generator inserts the repo root at `sys.path[0]` so
  > `import sportsdataverse` resolves the **local** editable source. Run as a bare
  > script its `sys.path[0]` is `tools/codegen/`, and a stale **site-packages**
  > install wins — silently dropping the newer leagues (935 names vs 1,937).
  > It also emits a single trailing newline so it agrees with `end-of-file-fixer`.

- **Playground link-out (the one piece with NO analogue).** Great Docs is a
  static Quarto site, so it has no equivalent of the Docusaurus React in-browser
  code playground. Rather than fake one, a dismissable `announcement:` banner
  links OUT to the interactive site (`https://py.sportsdataverse.org`). This is
  the single dimension that cannot reach parity.

**Build now:** ~5 min, `great-docs/_site/index.html` + **175 reference pages**
(up from ~83) + **17 user-guide pages**, 3–4 link warnings (pre-existing
README/CONTRIBUTING, unrelated to this work).

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
| Coverage model | **Generated from `leagues.yaml`** — high-value entry points per league across all 17 leagues (still not the full ~800 long tail) | Full generated long-tail (every endpoint), driven by YAML — better breadth |
| Returns documentation | The docstring `Returns:` prose + a runnable example; **no per-column type table** | Rich `col_name \| type \| description` returns tables (the codegen's strength) |
| Drift risk | Low for signatures (introspected); examples can break loudly if an API changes | Schema tables are hand-maintained and *can* silently drift from the real frame |
| Executable output | **Yes** — guide cells render the real DataFrame at build time | No — schema tables only; no live execution |
| Authoring cost | ~Zero per documented symbol (just list the name) | YAML + schema authoring per endpoint |
| Theme / search / llms.txt | Batteries-included (gradient navbar, dark mode, search, llms.txt, SKILL.md) **+ brand parity** (Plausible, Bungee/Lato/Fira fonts, cyan glow, favicon via `include_in_header`) | Custom Docusaurus theme + React Playground + TypeDoc analogue (JS site) |
| Interactive playground | **None** — static site; a dismissable banner links OUT to the interactive docs | React in-browser code playground |

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
