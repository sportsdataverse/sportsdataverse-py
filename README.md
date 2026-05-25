<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse-py <a href='https://py.sportsdataverse.org'><img src='https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/master/sdv-py-logo.png' align="right"  width="20%" min-width="100px" /></a>](#sportsdataverse-py-a-hrefhttpspysportsdataverseorgimg-srchttpsrawgithubusercontentcomsportsdataversesportsdataverse-pymastersdv-py-logopng-alignright--width20%25-min-width100px-a)
  - [Supported leagues and data sources](#supported-leagues-and-data-sources)
  - [Polars / pandas parser layer](#polars--pandas-parser-layer)
  - [Installation](#installation)
    - [Standard install (pip)](#standard-install-pip)
    - [Modern install (uv — recommended)](#modern-install-uv--recommended)
    - [Conda install](#conda-install)
    - [Development install](#development-install)
    - [Notes](#notes)
  - [Examples and tutorials](#examples-and-tutorials)
  - [Companion packages](#companion-packages)
- [**Our Authors**](#our-authors)
  - [**Citations**](#citations)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse-py <a href='https://py.sportsdataverse.org'><img src='https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/master/sdv-py-logo.png' align="right"  width="20%" min-width="100px" /></a>
<!-- badges: start -->

![Lifecycle:experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg?style=for-the-badge&logo=github)
[![PyPI](https://img.shields.io/pypi/v/sportsdataverse?label=sportsdataverse&logo=python&style=for-the-badge)](https://pypi.org/project/sportsdataverse/)<a href='https://pypi.org/project/sportsdataverse/'><img alt="PyPI - Down
loads" src="https://img.shields.io/pypi/dm/sportsdataverse?style=for-the-badge"></a>
![Contributors](https://img.shields.io/github/contributors/sportsdataverse/sportsdataverse-py?style=for-the-badge)
[![Twitter
Follow](https://img.shields.io/twitter/follow/sportsdataverse?color=blue&label=%40sportsdataverse&logo=twitter&style=for-the-badge)](https://twitter.com/sportsdataverse)

<!-- badges: end -->


See [CHANGELOG.md](https://py.sportsdataverse.org/CHANGELOG) for details.

The goal of [sportsdataverse-py](https://py.sportsdataverse.org) is to provide the community with a python package for working with sports data as a companion to the [cfbfastR](https://cfbfastR.sportsdataverse.org/), [hoopR](https://hoopR.sportsdataverse.org/), and [wehoop](https://wehoop.sportsdataverse.org/) R packages. Beyond data aggregation and tidying ease, one of the multitude of services that [sportsdataverse-py](https://py.sportsdataverse.org) provides is for benchmarking open-source expected points and win probability metrics for American Football.

## Supported leagues and data sources

| League | Module | Surfaces covered |
|---|---|---|
| NBA | `sportsdataverse.nba` | ESPN (Site v2 + Web v3 + Core v2) — 118 wrappers |
| WNBA | `sportsdataverse.wnba` | ESPN — 124 wrappers |
| MBB (NCAA M) | `sportsdataverse.mbb` | ESPN + NCAA-only (bracketology, rankings, recruits) — 121 wrappers |
| WBB (NCAA W) | `sportsdataverse.wbb` | ESPN + NCAA-only — 126 wrappers |
| CFB | `sportsdataverse.cfb` | ESPN + NCAA + football-only (QBR) — 123 wrappers |
| NFL | `sportsdataverse.nfl` | ESPN + football-only (QBR) — 119 wrappers |
| MLB | `sportsdataverse.mlb` | ESPN + MLB Stats API (`statsapi.mlb.com`) + Baseball Savant / Statcast — **175 wrappers** |
| NHL | `sportsdataverse.nhl` | `api-web.nhle.com/v1/` (game-feed) + NHL EDGE (player tracking) + Stats REST + Records site — **132 wrappers** |
| **Total** | | **~1,030 wrappers** |

## Polars / pandas parser layer

Every wrapper returns raw `Dict` by default. A parser layer turns
those payloads into tidy polars (or pandas) DataFrames.

For ESPN cross-league wrappers, pass `return_parsed=True` to get a
DataFrame directly — the raw-Dict contract is unchanged when the
kwarg is omitted, so existing callers are unaffected:

```python
from sportsdataverse.nba import espn_nba_team_roster

raw = espn_nba_team_roster(team_id=13)                          # → Dict (default)
df  = espn_nba_team_roster(team_id=13, return_parsed=True)      # → polars
pdf = espn_nba_team_roster(team_id=13,
                            return_parsed=True,
                            return_as_pandas=True)              # → pandas
```

For the NHL and MLB sibling-API wrappers, compose the wrapper with
its parser:

```python
from sportsdataverse.nhl import nhl_web_pbp, parse_nhl_web_pbp
df = parse_nhl_web_pbp(nhl_web_pbp(2023030417))                 # 331-row polars frame
```

See [py.sportsdataverse.org/docs/architecture/espn-cross-league](https://py.sportsdataverse.org/docs/architecture/espn-cross-league)
and [py.sportsdataverse.org/docs/parsers/index](https://py.sportsdataverse.org/docs/parsers/index)
for the full architecture + parser registry.

## Installation

The package metadata lives entirely in [`pyproject.toml`](pyproject.toml)
(PEP 621 `[project]` table). There is no `setup.py` source-of-truth.

### Standard install (pip)

```bash
pip install sportsdataverse
```

With optional extras (defined in `[project.optional-dependencies]` in
`pyproject.toml`):

```bash
pip install "sportsdataverse[all]"      # everything below
pip install "sportsdataverse[models]"   # extra deps for the EPA / WP model code
pip install "sportsdataverse[tests]"    # adds pytest, mypy, ruff, etc.
pip install "sportsdataverse[docs]"     # adds sphinx + sphinx-markdown-builder for the doc build
```

### Modern install (uv — recommended)

[uv](https://docs.astral.sh/uv/) is the fast, drop-in package manager we use day to day.

```bash
# Add to a uv-managed project:
uv add sportsdataverse

# With extras:
uv add "sportsdataverse[all]"

# Or install the latest dev snapshot from GitHub:
uv add "sportsdataverse @ git+https://github.com/sportsdataverse/sportsdataverse-py"
```

### Conda install

Once the conda-forge feedstock is published the package is also available via:

```bash
conda install -c conda-forge sportsdataverse
# or
mamba install -c conda-forge sportsdataverse
```

Until then, conda users can build a local package from this repo:

```bash
conda install conda-build conda-verify
conda build recipe/
conda install --use-local sportsdataverse
```

See [`recipe/README.md`](recipe/README.md) for the full conda workflow.

### Development install

For contributing or running the test suite:

```bash
git clone https://github.com/sportsdataverse/sportsdataverse-py.git
cd sportsdataverse-py

# uv (recommended) — fully resolved editable install with every extra:
uv pip install -e ".[all]"

# Plain pip works too if uv isn't available:
pip install -e ".[all]"
```

> Note: once we add a PEP 735 `[dependency-groups]` block (currently the
> repo only ships PEP 621 `[project.optional-dependencies]`),
> `uv sync --all-extras --all-groups` will become the one-shot dev incantation.
> Until then, `uv pip install -e ".[all]"` is the equivalent path.

Run the test suite:

```bash
uv run pytest                       # offline tests only
SDV_PY_LIVE_TESTS=1 uv run pytest   # include live API tests (slower; hits ESPN / nflverse)
```

For deeper dev-environment detail (lint, mypy, dep-bumping workflow), see
[CONTRIBUTING.md](CONTRIBUTING.md).

### Notes

- **Python target:** 3.9–3.14.
- **DataFrame engine:** polars 1.x. Most loaders accept `return_as_pandas=True`
  if you prefer pandas.
- **NFL caching:** loaders cache to memory by default. Set
  `SDV_PY_NFL_CACHE=filesystem` for cross-session reuse, or
  `SDV_PY_NFL_CACHE=off` to disable. See
  `sportsdataverse.nfl.config.update_config()` for runtime control.

## Examples and tutorials

Every public function ships a runnable `Example:` block in its docstring
showing a quick-start call, common parameter combinations, and a one-line
pipeline next-step. Render the API reference locally with
`bash create_docs.sh` or browse the live docs at
[py.sportsdataverse.org](https://py.sportsdataverse.org).

For longer-form walkthroughs, see the intro/intermediate Jupyter notebooks
under [`examples/notebooks/`](examples/notebooks):

| Notebook | Covers |
|---|---|
| `01_quickstart.ipynb` | Cross-sport intro — package layout, polars vs pandas, the `download()` retry layer |
| `02_cfb_intro.ipynb` | College football PBP, schedule, teams, `espn_cfb_play_participants` |
| `03_nfl_intro.ipynb` | NFL — nflreadpy parity surface, caching layer, current-season helpers |
| `04_nba_intro.ipynb` | NBA — PBP, schedule, teams, game rosters, shot distribution |
| `05_wbb_wnba_intro.ipynb` | Women's basketball — NCAA + WNBA parallels, multi-table stats |
| `06_mbb_intro.ipynb` | Men's college basketball — PBP, schedule, conference standings |
| `07_nhl_intro.ipynb` | NHL — PBP, schedule, teams, shot-event filter |

## Companion packages

`sportsdataverse-py` is one corner of the broader [SportsDataverse](https://www.sportsdataverse.org)
ecosystem. The R sister packages cover the same data sources with deeper
sport-specific coverage:

- [wehoop](https://wehoop.sportsdataverse.org) — women's basketball (WNBA + NCAA)
- [hoopR](https://hoopR.sportsdataverse.org) — men's basketball (NBA + NCAA)
- [cfbfastR](https://cfbfastR.sportsdataverse.org) — college football
- [baseballr](https://baseballr.sportsdataverse.org) — baseball (MLB + MiLB + NCAA)
- [fastRhockey](https://fastRhockey.sportsdataverse.org) — hockey (NHL + WHL)

The NFL submodule is a near drop-in replacement for [nflreadpy](https://github.com/nflverse/nflreadpy);
the broader [nflverse](https://nflverse.nflverse.com) ecosystem is the
upstream data source for many of those loaders.

# **Our Authors**

-   [Saiem Gilani](https://twitter.com/saiemgilani)
<a href="https://twitter.com/saiemgilani" target="blank"><img src="https://img.shields.io/twitter/follow/saiemgilani?color=blue&label=%40saiemgilani&logo=twitter&style=for-the-badge" alt="@saiemgilani" /></a>
<a href="https://github.com/saiemgilani" target="blank"><img src="https://img.shields.io/github/followers/saiemgilani?color=eee&logo=Github&style=for-the-badge" alt="@saiemgilani" /></a>


## **Citations**

To cite the [**`sportsdataverse-py`**](https://py.sportsdataverse.org) Python package in publications, use:

BibTex Citation

```bibtex
@misc{gilani_sdvpy_2021,
  author = {Gilani, Saiem},
  title = {sportsdataverse-py: The SportsDataverse's Python Package for Sports Data.},
  url = {https://py.sportsdataverse.org},
  season = {2021}
}
```
