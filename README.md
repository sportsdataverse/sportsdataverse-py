<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse-py <a href='https://py.sportsdataverse.org'><img src='https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/master/sdv-py-logo.png' align="right"  width="20%" min-width="100px" /></a>](#sportsdataverse-py-a-hrefhttpspysportsdataverseorgimg-srchttpsrawgithubusercontentcomsportsdataversesportsdataverse-pymastersdv-py-logopng-alignright--width20%25-min-width100px-a)
  - [Supported leagues and data sources](#supported-leagues-and-data-sources)
  - [Polars / pandas parser layer](#polars--pandas-parser-layer)
  - [Installation](#installation)
    - [Standard install (pip)](#standard-install-pip)
    - [Modern install (uv — recommended)](#modern-install-uv--recommended)
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
| NBA | `sportsdataverse.nba` | ESPN (Site v2 + Web v3 + Core v2) + Fox Sports (Bifrost) |
| WNBA | `sportsdataverse.wnba` | ESPN |
| MBB (NCAA M) | `sportsdataverse.mbb` | ESPN + NCAA-only (rankings, recruits) + Fox Sports (Bifrost) |
| WBB (NCAA W) | `sportsdataverse.wbb` | ESPN + NCAA-only |
| CFB | `sportsdataverse.cfb` | ESPN + NCAA + football-only (QBR) + Fox Sports (Bifrost) + Yahoo Sports |
| NFL | `sportsdataverse.nfl` | ESPN + **NFL.com API** (`api.nfl.com` "Shield") + **nflverse loaders** (nflreadpy parity) + football-only (QBR) |
| MLB | `sportsdataverse.mlb` | ESPN + MLB Stats API (`statsapi.mlb.com`) + Baseball Savant / Statcast (43-endpoint `mlb_statcast_*` surface) + Fox Sports (Bifrost) |
| NHL | `sportsdataverse.nhl` | `api-web.nhle.com/v1/` (game-feed) + NHL EDGE (player tracking) + Stats REST + Records site + Fox Sports (Bifrost) |

Each league exports 150–340 public functions (ESPN wrappers + that league's
native-API wrappers + dataset loaders + parsers); ~1,600 in total. **Fox Sports**
adds `fox_<league>_*` Bifrost wrappers (pbp / boxscore / odds / roster / stats /
standings / leaders) for nba, mbb, cfb, mlb, nhl; **Yahoo Sports** adds
`yahoo_cfb_*` season-stats / scoreboard wrappers for college football.

## Polars / pandas parser layer

Parser-backed wrappers return a tidy polars DataFrame **by default**
(0.0.54+). Pass `return_parsed=False` for the raw `Dict`, or
`return_as_pandas=True` for pandas. Wrappers without a registered
parser return the raw `Dict`.

```python
from sportsdataverse.nba import espn_nba_team_roster

df  = espn_nba_team_roster(team_id=13)                          # → polars (default)
raw = espn_nba_team_roster(team_id=13, return_parsed=False)     # → Dict
pdf = espn_nba_team_roster(team_id=13,
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
- **stats.nba.com / stats.wnba.com surface (`nba_stats_*` / `wnba_stats_*`):**
  151 NBA (+ G-League + Summer League via `league_id`) and 115 WNBA wrappers
  are available. Live calls to `stats.nba.com` require the `curl_cffi` package
  (TLS fingerprint protection); install it via
  `pip install "sportsdataverse[all]"` or `pip install curl_cffi` separately.

## Examples and tutorials

Every public function ships a runnable `Example:` block in its docstring
showing a quick-start call, common parameter combinations, and a one-line
pipeline next-step. Regenerate the API reference locally with
`uv run python tools/codegen/generate.py --docs` (then `cd docs && yarn build`
to preview the Docusaurus site) or browse the live docs at
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
