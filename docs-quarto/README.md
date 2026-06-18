<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Quarto docs PoC — `docs-quarto/`](#quarto-docs-poc--docs-quarto)
  - [Contents](#contents)
  - [How to render it yourself](#how-to-render-it-yourself)
  - [What the PoC actually demonstrates](#what-the-poc-actually-demonstrates)
- [Quarto vs. Docusaurus — for *these* packages](#quarto-vs-docusaurus--for-these-packages)
  - [What the SportsDataverse currently runs (Docusaurus)](#what-the-sportsdataverse-currently-runs-docusaurus)
  - [What Quarto brings that Docusaurus does not](#what-quarto-brings-that-docusaurus-does-not)
  - [Recommendation](#recommendation)
  - [The hybrid worth considering](#the-hybrid-worth-considering)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Quarto docs PoC — `docs-quarto/`

A self-contained proof-of-concept demonstrating **Quarto** as a documentation
toolchain for `sportsdataverse-py`, so it can be compared side-by-side with the
**Docusaurus** site the SportsDataverse uses under `docs/`.

The PoC is deliberately small: one page (`index.qmd`) with a handful of
**build-time executable Python cells** that run real `sportsdataverse` code and
freeze the output (tidy polars / pandas DataFrames rendered as HTML tables) into
the static page.

## Contents

| File | What it is |
|---|---|
| `_quarto.yml` | Minimal Quarto website project config (HTML, `jupyter: python3`, `execute: freeze: auto`). |
| `index.qmd` | The PoC page — 7 executable Python cells + inline computed expressions. |
| `rendered-preview.html` | Self-contained (embedded-resources) render of the page. Open it in any browser to see the real tables with **no build step**. |
| `quarto-poc-render.png` | Screenshot of the rendered page (the tables, in case you can't open the HTML). |
| `_freeze/` | Quarto's cached execution results — the committed proof the cells ran against live data. |
| `_site/` | The full rendered website (gitignored — it's a build artifact). |

## How to render it yourself

Quarto executes Python cells through **Jupyter** (`nbclient` + `ipykernel`),
using whichever Python the `python3` Jupyter kernel resolves to. In this repo's
environment that interpreter already has `sportsdataverse` importable, so no
kernel override is needed.

```sh
# from this directory
cd docs-quarto
quarto render          # -> _site/index.html  (full website)

# or a single self-contained file:
quarto render index.qmd --to html --embed-resources --output preview.html
```

Requirements: `quarto` (>= 1.7), and `jupyter nbclient ipykernel` available to
the Python that imports `sportsdataverse`
(`python -c "import nbclient, ipykernel"`).

## What the PoC actually demonstrates

Every table and number on the rendered page is the **genuine output of
`sportsdataverse` code, executed once at build time** and frozen into static
HTML:

- `espn_nba_teams()` → a 30-row polars DataFrame, rendered as a table.
- `espn_nba_teams(return_as_pandas=True)` → the same data as pandas.
- `espn_nfl_teams()` → the cross-league uniform shape (32 teams).
- inline `` `{python} n_nba` `` expressions → computed numbers (30, 32) quoted
  *inside the prose* rather than copy-pasted.

There is **no client-side Python**, **no live API call when a reader loads the
page**, and **no hand-maintained "expected output"** that can silently rot.

---

# Quarto vs. Docusaurus — for *these* packages

This is an honest comparison scoped to the SportsDataverse stack, not a generic
"which static-site generator is best" piece.

## What the SportsDataverse currently runs (Docusaurus)

The Python package's docs (`docs/`) and the JS package's docs are
**Docusaurus 3** sites. That stack brings several things this PoC does *not*
attempt to replicate:

- **A React/MDX application.** Pages are a client-rendered SPA; MDX lets prose
  embed live React components.
- **A live serverless-proxy "Playground."** The JS docs ship an in-browser code
  playground that executes `sportsdataverse-js` against a serverless proxy
  (`docs/api/run.mjs`) — *interactive*, runs on the reader's click.
- **On-site TypeDoc.** API reference for the JS package is generated from
  TypeScript types directly into the site.
- **Versioned docs.** `lastVersion` / `versioned_docs/` freeze a snapshot per
  release while `current`/`main` tracks the code.
- **A codegen → markdown → autosidebar pipeline.** For `sportsdataverse-py`,
  `tools/codegen/generate.py --docs` rewrites the per-league reference subtree
  from YAML endpoint metadata, and `docs/sidebars.ts` auto-expands each league —
  new endpoints surface with no sidebar edits. A drift gate (`--check`) keeps
  generated docs honest in CI.

Docusaurus is the right tool for all of that. In particular the **JS package**
leans hard on React (the playground) and TypeScript (TypeDoc) — neither has a
Quarto analogue.

## What Quarto brings that Docusaurus does not

- **Build-time executable code cells with real, frozen output.** This is the
  headline. A `.qmd` cell *runs* at render time and the genuine DataFrame /
  plot / value is baked into the static page. The Docusaurus reference tables
  for the Python package are generated from **schema YAML** — accurate column
  lists, but not *actual* `df.head()` output. With Quarto, the example output a
  reader sees is the literal output the code produced.
- **Reproducible + cache-aware.** `execute: freeze: auto` caches results so a
  re-render doesn't re-hit ESPN unless the source changes — and CI can fail if a
  doc's output drifts, turning examples into a lightweight integration test.
- **Multi-format from one source.** The same `.qmd` renders to HTML **and** PDF
  (and more) — useful for a methods/whitepaper-style export that the React SPA
  can't produce.
- **Posit-ecosystem fit.** Quarto is the native literate-docs tool for the
  R world, and the SportsDataverse ships R sister packages (`hoopR`, `wehoop`,
  `cfbfastR`, `baseballr`, `fastRhockey`). One Quarto toolchain could document
  the **R and Python** packages with the *same* literate, run-it-for-real model.

## Recommendation

- **Quarto is compelling for the Python *and* R data packages.** Their value is
  tidy DataFrames out of live endpoints; build-time executable cells let the
  docs show the *real* frame instead of a schema table that can drift, and the
  one-toolchain story spans the R sister packages the SportsDataverse already
  maintains.
- **Docusaurus stays the better fit for the JS package** — the React playground
  and TypeDoc surface have no Quarto equivalent, and the existing codegen →
  markdown → autosidebar pipeline + versioned docs are mature.

## The hybrid worth considering

The SportsDataverse already has a `codegen` pipeline that emits markdown for
Docusaurus. The cheapest way to borrow Quarto's best idea **without switching
toolchains** is a **build-time "real output" injector**: a small step in
`tools/codegen` that executes a curated set of example snippets and injects the
*actual* `df.head()` output (as a markdown table) into the generated reference
pages — exactly the literate-docs guarantee Quarto gives, while keeping the
single Docusaurus site, the autosidebar, and the versioning. Quarto then becomes
an optional second output (PDF/whitepaper, or a unified R+Python docs experiment)
rather than a replacement.
