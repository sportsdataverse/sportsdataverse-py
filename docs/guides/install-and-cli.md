# Install and the command line

> Install sportsdataverse-py with pip or uv, choose the optional extras, and use the sdv command line and the sdv-docs MCP server.

sportsdataverse-py supports Python 3.10 through 3.14 and runs on polars 2
(`polars>=2.0,<3`). polars 1.x is no longer supported. Every loader and wrapper
returns a polars DataFrame by default, and most accept `return_as_pandas=True`
if you want pandas instead.

## Install with pip or uv

```bash
pip install --pre sportsdataverse   # 0.2.0b1, the 0.2.0 beta
```

Without `--pre`, pip installs 0.1.4, the latest final release, until 0.2.0 ships.

With [uv](https://docs.astral.sh/uv/), which the project uses day to day:

```bash
uv add sportsdataverse

# the latest commit on main, ahead of the next release
uv add "sportsdataverse @ git+https://github.com/sportsdataverse/sportsdataverse-py"
```

For a development checkout, clone the repository and sync every extra into the
checkout's own `.venv`:

```bash
git clone https://github.com/sportsdataverse/sportsdataverse-py.git
cd sportsdataverse-py
uv sync --all-extras
uv run pytest
```

`CONTRIBUTING.md` in the repository covers the rest of the contributor workflow
(lint, type checks, live-test gates, changelog fragments).

## Optional extras

The extras are defined in `[project.optional-dependencies]` in `pyproject.toml`.
None of them is needed for the core loaders and ESPN wrappers.

| Extra | Install | What it adds |
|---|---|---|
| `tests` | `pip install "sportsdataverse[tests]"` | pytest, mypy, ruff, `curl_cffi`, kloppy and pyyaml: the test toolchain |
| `soccer` | `pip install "sportsdataverse[soccer]"` | [kloppy](https://kloppy.pysport.org) for soccer event and tracking data (`soccer_open_events`) |
| `nflpro` | `pip install "sportsdataverse[nflpro]"` | Playwright, for the NFL Pro (pro.nfl.com) Next Gen Stats client |
| `models` | `pip install "sportsdataverse[models]"` | Nothing. It is kept so that existing `[models]` installs keep working; the models ship in the wheel |
| `pff` | `pip install "sportsdataverse[pff]"` | Playwright, for the optional headless sign-in of the legacy PFF Premium client |
| `sdvplot` | `pip install "sportsdataverse[sdvplot]"` | [sdvplot](https://sdvplot.sportsdataverse.org/) 0.1.1 or later with matplotlib (`sdvplot[mpl]`): team logos, colors and chart themes |
| `sportypy` | `pip install "sportsdataverse[sportypy]"` | [sportypy](https://sportypy.sportsdataverse.org/): rulebook fields, rinks and courts to draw coordinates on |
| `mcp` | `pip install "sportsdataverse[mcp]"` | The `mcp` SDK, for the [sdv-docs MCP server](#the-sdv-docs-mcp-server) |
| `all` | `pip install "sportsdataverse[all]"` | Every extra above except `mcp`, plus `patchright` (the stats.ncaa.org browser transport) |

Three notes on the extras:

- **`[all]` leaves out `mcp`.** Install `sportsdataverse[all,mcp]` to get both.
- **sportypy is GPL-3.0.** sportsdataverse-py is MIT and never bundles it; the
  extra installs it beside the package, so check that the GPL fits your project
  before you depend on it. sdvplot is MIT.
- **`curl_cffi` is in `tests` and `all` only.** The stats.nba.com / stats.wnba.com
  wrappers need it for live calls, as do a few international basketball sites. If
  it is missing, those calls raise an `ImportError` that names it
  (`pip install curl_cffi`). See [Troubleshooting](troubleshooting.md).

The `sdvplot` extra installs `sdvplot[mpl]`, so matplotlib comes with it. For another
backend, add sdvplot's own extra: `pip install "sdvplot[plotly]"` or `"sdvplot[altair]"`.

## The `sdv` command line

Installing the package puts an `sdv` console script on your path. It resolves names
to ESPN ids, searches the function index and controls the package-wide HTTP response
cache, without writing any Python. A global `--json` flag (placed before the
subcommand) prints JSON for piping to `jq`.

| Command | What it does |
|---|---|
| `sdv find-team NAME --league LG [--multi]` | Resolve a team name to its ESPN team record |
| `sdv find-athlete NAME --league LG [--team TEAM] [--multi]` | Resolve an athlete name; `--team` narrows the search and speeds it up |
| `sdv find-event DATE --league LG [--home TEAM] [--away TEAM] [--multi]` | Resolve a game on a date (`YYYY-MM-DD` or `YYYYMMDD`) to its ESPN event |
| `sdv list-functions [--league LG] [--search TEXT] [--parsers-only \| --wrappers-only]` | Search the callable index |
| `sdv function-count [--league LG]` | Count the callables per league |
| `sdv cache mode [--set off\|memory\|filesystem]` | Print or set the HTTP response cache mode |
| `sdv cache stats` | Show the cache's entries, bytes on disk and mode |
| `sdv cache clear [--pattern GLOB]` | Drop cached entries, all of them or those matching a filename glob |

The `--league` choices for the `find-*` commands, `list-functions` and
`function-count` are `nba`, `wnba`, `mbb`, `wbb`, `cfb`, `nfl`, `mlb` and `nhl`.

```bash
sdv find-team lakers --league nba            # abbreviation: LAL ... id: 13 ...
sdv --json find-event 2025-02-09 -l nfl      # {"id": "401671889", ... "Kansas City Chiefs at Philadelphia Eagles"}
sdv list-functions -l nba -s roster          # espn_nba_game_rosters, espn_nba_team_roster, ...
sdv cache stats                              # mode: off / entries: 0 / disk_bytes: 0
```

Exit codes: `0` when the command succeeded (for `find-*`, a match was found), `1`
when a `find-*` command found no match, and `2` on an error, printed as
`sdv: <message>` on stderr. An error covers invalid input, a host that answered
"nothing there" (`NoDataError`) and a fetch that failed (`AssetFetchError`).

The `cache` commands control the response cache that the package's HTTP layer
checks before going to the network (`sportsdataverse.cache`; it is off by default).
That cache is separate from the NFL loaders' cache, which the
[NFL cache recipe](recipes.md#nfl-cache-and-configuration) covers.

## The sdv-docs MCP server

`sdv-docs` is a [Model Context Protocol](https://modelcontextprotocol.io) server
that lets an AI assistant search this documentation: the function reference, the
Returns tables, the endpoint metadata and the R packages' indexes. It needs the
`mcp` extra:

```bash
pip install "sportsdataverse[mcp]"
sdv-docs --version
```

With no arguments `sdv-docs` serves its tools over stdio, so you register it with
your MCP client as a command (`sdv-docs`). It exposes six tools: `search`,
`get_function`, `find_columns`, `find_endpoints`, `list_datasets` and `index_info`.

The server never imports `sportsdataverse` itself, so it starts in about a second.
It reads a prebuilt SQLite index, downloaded on first use from the `docs-index`
release of the repository, cached locally and re-checked at most once a day. Set
`SDV_DOCS_DB` to the path of an index file to use that file and never download.
