<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [HockeyTech Multi-League Scraper + Analytics — sdv-py Implementation Plan (Part A)](#hockeytech-multi-league-scraper--analytics--sdv-py-implementation-plan-part-a)
  - [Conventions every task follows](#conventions-every-task-follows)
  - [File Structure](#file-structure)
  - [Phase A1 — Shared core + PWHL parity](#phase-a1--shared-core--pwhl-parity)
    - [Task A1.0: Scaffold the `hockeytech` package + fixtures dir](#task-a10-scaffold-the-hockeytech-package--fixtures-dir)
    - [Task A1.1: League registry (`_leagues.py`)](#task-a11-league-registry-_leaguespy)
    - [Task A1.2: JSONP client (`_client.py`)](#task-a12-jsonp-client-_clientpy)
    - [Task A1.3: Capture live fixtures](#task-a13-capture-live-fixtures)
    - [Task A1.4: Seasons parser + `resolve_season_id`](#task-a14-seasons-parser--resolve_season_id)
    - [Task A1.5: Schedule / scorebar / standings / teams / roster parsers](#task-a15-schedule--scorebar--standings--teams--roster-parsers)
    - [Task A1.6: PBP parser (dialect a) — one row per event](#task-a16-pbp-parser-dialect-a--one-row-per-event)
    - [Task A1.7: Shifts parser](#task-a17-shifts-parser)
    - [Task A1.8: Remaining PWHL parsers (player_stats, leaders, game_summary, player_box, player_info, game_log, search, streaks, transactions, playoff_bracket, scorebar, game_info, stats)](#task-a18-remaining-pwhl-parsers-player_stats-leaders-game_summary-player_box-player_info-game_log-search-streaks-transactions-playoff_bracket-scorebar-game_info-stats)
    - [Task A1.9: PWHL public API (`pwhl_api.py`) — wire parsers to live calls](#task-a19-pwhl-public-api-pwhl_apipy--wire-parsers-to-live-calls)
  - [Phase A2 — Analytics (source of truth)](#phase-a2--analytics-source-of-truth)
    - [Task A2.1: Shot geometry — distance, angle, scoring chance](#task-a21-shot-geometry--distance-angle-scoring-chance)
    - [Task A2.2: TOI from shifts](#task-a22-toi-from-shifts)
    - [Task A2.3: On-ice reconstruction (countdown-clock interval match)](#task-a23-on-ice-reconstruction-countdown-clock-interval-match)
    - [Task A2.4: Corsi/Fenwick (team + player) with per-60](#task-a24-corsifenwick-team--player-with-per-60)
    - [Task A2.5: PWHL analytics public functions + enriched `pwhl_pbp`](#task-a25-pwhl-analytics-public-functions--enriched-pwhl_pbp)
  - [Phase A3 — AHL / OHL / WHL / QMJHL families](#phase-a3--ahl--ohl--whl--qmjhl-families)
    - [Task A3.1: PBP dialect b parser](#task-a31-pbp-dialect-b-parser)
    - [Task A3.2: League family factory + the four packages](#task-a32-league-family-factory--the-four-packages)
    - [Task A3.3: Live smoke tests (env-gated)](#task-a33-live-smoke-tests-env-gated)
  - [Phase A4 — Docs, codegen, notebook, full gate](#phase-a4--docs-codegen-notebook-full-gate)
    - [Task A4.1: Autodoc example args + return schemas](#task-a41-autodoc-example-args--return-schemas)
    - [Task A4.2: Notebook + R-parity entries + full gate](#task-a42-notebook--r-parity-entries--full-gate)
  - [Self-Review (against the design doc)](#self-review-against-the-design-doc)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# HockeyTech Multi-League Scraper + Analytics — sdv-py Implementation Plan (Part A)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a live HockeyTech scraper to sdv-py — PWHL (full fastRhockey output parity), AHL/OHL/WHL/QMJHL (core set), plus on-ice / Corsi-Fenwick / TOI analytics — all returning snake_cased polars frames.

**Architecture:** One shared `sportsdataverse/hockeytech/` core (HTTP+JSONP client, league registry, pure parsers, pure analytics) with per-league prefixed shim modules (`pwhl/`, `ahl/`, `ohl/`, `whl/`, `qmjhl/`) that inject a league config. PWHL parity functions are transliterated from fastRhockey's R; analytics are built fresh against the live feed and become the source of truth for the R mirror (Part B).

**Tech Stack:** Python 3.11+, `polars` (default return) + `pandas` (`return_as_pandas=True`), `requests` via `sportsdataverse._codegen_runtime._get`-style helpers, `pytest` with captured JSON fixtures.

**Design doc:** `docs/superpowers/specs/2026-06-09-hockeytech-multi-league-scraper-analytics-design.md`

---

## Conventions every task follows

- **snake_case hard rule:** every returned column passes through `_snake_columns()` (built on `sportsdataverse.dl_utils.underscore`). Explicit fastRhockey renames win for PWHL parity.
- **Return type:** every public function returns `polars.DataFrame` by default; `return_as_pandas: bool = False` returns pandas. Empty/malformed payloads return a zero-row frame, never raise.
- **Parsers are pure:** they take a parsed `dict` (or list) → DataFrame. No network. This is what tests exercise offline.
- **Analytics are pure:** frame(s) → frame. No network.
- **Tests:** offline parser/analytics tests use `tests.conftest.load_fixture("hockeytech", "<stem>")`; live tests gated behind `HOCKEYTECH_TESTS=1` via `skip_unless_hockeytech()` in `tests/conftest.py`.
- **Commits:** Conventional Commits, no AI co-author trailers (repo rule). Pre-commit may reformat (ruff) or add TOCs (doctoc) and abort once — re-stage and re-commit.

---

## File Structure

**New — shared core:**
- `sportsdataverse/hockeytech/__init__.py` — re-exports the public helpers (`hockeytech_api`, `LEAGUES`, parser & analytics functions) for cross-league reuse + tests.
- `sportsdataverse/hockeytech/_leagues.py` — `LeagueConfig` dataclass, `LEAGUES` registry, env-var key resolution, `resolve_season_id()`.
- `sportsdataverse/hockeytech/_client.py` — `hockeytech_api()` (URL build + JSONP fetch + strip + parse, retry/rate-limit), `_strip_jsonp()`.
- `sportsdataverse/hockeytech/_parsers.py` — pure parsers + `_snake_columns()`.
- `sportsdataverse/hockeytech/_analytics.py` — pure analytics.

**New — per-league public modules:**
- `sportsdataverse/pwhl/pwhl_api.py` — 19 live `pwhl_*()` parity functions.
- `sportsdataverse/pwhl/pwhl_analytics.py` — `pwhl_game_shifts`, `pwhl_player_toi`, `pwhl_game_corsi`.
- `sportsdataverse/ahl/{__init__,ahl_api,ahl_analytics}.py` (and identical `ohl/`, `whl/`, `qmjhl/`).

**New — tests & fixtures:**
- `tests/hockeytech/__init__.py`, `tests/hockeytech/test_client.py`, `test_parsers.py`, `test_analytics.py`, `test_leagues.py`, `test_public_surface.py`.
- `tests/fixtures/hockeytech/*.json` + `tests/fixtures/hockeytech/README.md`.

**Modified:**
- `sportsdataverse/pwhl/__init__.py` — import `pwhl_api`, `pwhl_analytics`.
- `sportsdataverse/__init__.py` — add `from sportsdataverse.{ahl,ohl,whl,qmjhl} import *`.
- `tests/conftest.py` — add `skip_unless_hockeytech()`.
- `tools/codegen/autodoc_example_args.yaml` — example args for the new functions.

---

## Phase A1 — Shared core + PWHL parity

### Task A1.0: Scaffold the `hockeytech` package + fixtures dir

**Files:**
- Create: `sportsdataverse/hockeytech/__init__.py`
- Create: `tests/hockeytech/__init__.py`
- Create: `tests/fixtures/hockeytech/README.md`

- [ ] **Step 1: Create the package `__init__.py` (placeholder re-export, filled as modules land)**

```python
# sportsdataverse/hockeytech/__init__.py
"""Shared HockeyTech core (client, league registry, parsers, analytics).

Backs the per-league public modules (``pwhl``/``ahl``/``ohl``/``whl``/``qmjhl``).
Internal: import the per-league wrappers, not these helpers, unless you are
building a new league family.
"""

from __future__ import annotations

from sportsdataverse.hockeytech._client import hockeytech_api
from sportsdataverse.hockeytech._leagues import LEAGUES, LeagueConfig, resolve_season_id

__all__ = ["hockeytech_api", "LEAGUES", "LeagueConfig", "resolve_season_id"]
```

- [ ] **Step 2: Create empty test package + fixtures README**

```python
# tests/hockeytech/__init__.py
```

```markdown
<!-- tests/fixtures/hockeytech/README.md -->
# HockeyTech fixtures

Captured JSON payloads from `lscluster.hockeytech.com` / `cluster.leaguestat.com`
(JSONP `angular.callbacks._N(...)` wrapper already stripped). Provenance:

| stem | league | endpoint | game/season |
|------|--------|----------|-------------|
| pwhl_schedule_2025 | pwhl | modulekit/scorebar | season_id 5 |
| pwhl_pbp_42 | pwhl | statviewfeed/gameCenterPlayByPlay | game_id 42 |
| pwhl_gameshifts_42 | pwhl | modulekit/gameshifts | game_id 42 |
| pwhl_seasons | pwhl | modulekit/seasons | all |
| pwhl_standings_5 | pwhl | statviewfeed/teams | season_id 5 |
| pwhl_teams_5 | pwhl | modulekit/teamsbyseason | season_id 5 |
| pwhl_roster_1_5 | pwhl | modulekit/roster | team 1 season 5 |
| pwhl_player_stats_27 | pwhl | modulekit/player seasonstats | player 27 |
| pwhl_leaders_5 | pwhl | statviewfeed/leadersExtended | season_id 5 |
| pwhl_game_summary_42 | pwhl | gc/gamesummary | game_id 42 |
| ahl_pbp_* / ohl_pbp_* / whl_pbp_* / qmjhl_pbp_* | (juniors) | gameCenterPlayByPlay (dialect b) | per league |

Refresh: re-run the capture snippets in `tests/fixtures/hockeytech/_capture.py`
(committed alongside) against a completed game.
```

- [ ] **Step 3: Commit**

```bash
git add sportsdataverse/hockeytech/__init__.py tests/hockeytech/__init__.py tests/fixtures/hockeytech/README.md
git commit -m "feat(hockeytech): scaffold shared core package + fixtures dir"
```

---

### Task A1.1: League registry (`_leagues.py`)

**Files:**
- Create: `sportsdataverse/hockeytech/_leagues.py`
- Test: `tests/hockeytech/test_leagues.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/hockeytech/test_leagues.py
from __future__ import annotations

import pytest


def test_leagues_registry_has_five_hockeytech_leagues():
    from sportsdataverse.hockeytech import LEAGUES

    assert set(LEAGUES) == {"pwhl", "ahl", "ohl", "whl", "qmjhl"}


def test_pwhl_config_matches_known_values():
    from sportsdataverse.hockeytech import LEAGUES

    pwhl = LEAGUES["pwhl"]
    assert pwhl.client_code == "pwhl"
    assert pwhl.league_id == 1
    assert pwhl.site_id == 0
    assert pwhl.pbp_style == "hockeytech_a"
    assert "lscluster.hockeytech.com" in pwhl.base_url


def test_qmjhl_uses_leaguestat_host_and_lhjmq_code():
    from sportsdataverse.hockeytech import LEAGUES

    q = LEAGUES["qmjhl"]
    assert q.client_code == "lhjmq"
    assert "cluster.leaguestat.com" in q.base_url
    assert q.pbp_style == "hockeytech_b"


def test_env_var_overrides_api_key(monkeypatch):
    from sportsdataverse.hockeytech._leagues import resolve_api_key

    monkeypatch.setenv("SDV_PWHL_API_KEY", "override123")
    assert resolve_api_key("pwhl") == "override123"
    monkeypatch.delenv("SDV_PWHL_API_KEY")
    assert resolve_api_key("pwhl") == "446521baf8c38984"
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_leagues.py -q`
Expected: FAIL — `ModuleNotFoundError`/`ImportError` (no `_leagues`).

- [ ] **Step 3: Implement `_leagues.py`**

```python
# sportsdataverse/hockeytech/_leagues.py
"""HockeyTech league registry + season resolution.

Values lifted from maxtixador/scrapernhl config.py. Keys are public web-client
defaults shipped in each league's site JS; override per league with the
``SDV_<LEAGUE>_API_KEY`` environment variable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Literal, Optional

LeagueCode = Literal["pwhl", "ahl", "ohl", "whl", "qmjhl"]


@dataclass(frozen=True)
class LeagueConfig:
    name: str
    client_code: str
    api_key: str
    league_id: int
    site_id: int
    base_url: str
    pbp_style: Literal["hockeytech_a", "hockeytech_b"]
    ot_period_length: int  # regulation-OT length in seconds (informational)


_LSCLUSTER = "https://lscluster.hockeytech.com/feed/index.php"
_LEAGUESTAT = "https://cluster.leaguestat.com/feed/index.php"

LEAGUES: Dict[str, LeagueConfig] = {
    "pwhl": LeagueConfig("PWHL", "pwhl", "446521baf8c38984", 1, 0, _LSCLUSTER, "hockeytech_a", 600),
    "ahl": LeagueConfig("AHL", "ahl", "ccb91f29d6744675", 4, 3, _LSCLUSTER, "hockeytech_a", 300),
    "ohl": LeagueConfig("OHL", "ohl", "f1aa699db3d81487", 1, 1, _LSCLUSTER, "hockeytech_b", 300),
    "whl": LeagueConfig("WHL", "whl", "f1aa699db3d81487", 7, 0, _LSCLUSTER, "hockeytech_b", 300),
    "qmjhl": LeagueConfig("QMJHL", "lhjmq", "f322673b6bcae299", 6, 0, _LEAGUESTAT, "hockeytech_b", 300),
}

# gameCenterPlayByPlay uses a distinct key on the statviewfeed PBP view for PWHL
# (observed live 2026-06-09). Other leagues reuse their default key until proven
# otherwise; override per (league, view) here if a different key is needed.
_PBP_KEY_OVERRIDES: Dict[str, str] = {"pwhl": "694cfeed58c932ee"}


def resolve_api_key(league: str, view: Optional[str] = None) -> str:
    """Return the API key for a league, honoring ``SDV_<LEAGUE>_API_KEY``.

    When ``view == "gameCenterPlayByPlay"`` and the league has a PBP-key
    override, that override is used (unless the env var is set, which always
    wins).
    """
    env = os.environ.get(f"SDV_{league.upper()}_API_KEY")
    if env:
        return env
    if view == "gameCenterPlayByPlay" and league in _PBP_KEY_OVERRIDES:
        return _PBP_KEY_OVERRIDES[league]
    return LEAGUES[league].api_key


def get_config(league: str) -> LeagueConfig:
    try:
        return LEAGUES[league]
    except KeyError as exc:  # pragma: no cover - guard
        raise ValueError(f"Unknown HockeyTech league {league!r}; expected one of {sorted(LEAGUES)}") from exc
```

(Note: `resolve_season_id` is added in Task A1.4 once `parse_seasons` exists; keep the `__init__.py` import of `resolve_season_id` working by adding a temporary stub here now.)

```python
def resolve_season_id(league: str, season=None, game_type: str = "regular", season_id=None):
    """Placeholder — implemented in Task A1.4."""
    raise NotImplementedError
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_leagues.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_leagues.py tests/hockeytech/test_leagues.py
git commit -m "feat(hockeytech): league registry + env-var key override"
```

---

### Task A1.2: JSONP client (`_client.py`)

**Files:**
- Create: `sportsdataverse/hockeytech/_client.py`
- Test: `tests/hockeytech/test_client.py`

- [ ] **Step 1: Write the failing test (pure-function parts; no network)**

```python
# tests/hockeytech/test_client.py
from __future__ import annotations

import pytest


def test_strip_jsonp_angular_callback():
    from sportsdataverse.hockeytech._client import _strip_jsonp

    assert _strip_jsonp('angular.callbacks._8([{"a":1}])') == '[{"a":1}]'


def test_strip_jsonp_bare_parens():
    from sportsdataverse.hockeytech._client import _strip_jsonp

    assert _strip_jsonp('({"a":1})') == '{"a":1}'


def test_strip_jsonp_passthrough_plain_json():
    from sportsdataverse.hockeytech._client import _strip_jsonp

    assert _strip_jsonp('{"a":1}') == '{"a":1}'


def test_build_url_includes_key_client_code_and_feed():
    from sportsdataverse.hockeytech._client import _build_url

    url = _build_url("pwhl", feed="modulekit", view="seasons", params={"site_id": "0"})
    assert url.startswith("https://lscluster.hockeytech.com/feed/index.php?")
    assert "feed=modulekit" in url and "view=seasons" in url
    assert "key=446521baf8c38984" in url and "client_code=pwhl" in url
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_client.py -q`
Expected: FAIL — no `_client`.

- [ ] **Step 3: Implement `_client.py`**

```python
# sportsdataverse/hockeytech/_client.py
"""HockeyTech HTTP client: build the JSONP URL, fetch, strip the callback
wrapper, and parse JSON. One retrying, rate-limited entry point shared by every
league family.
"""

from __future__ import annotations

import json
import re
import time
import urllib.parse
from typing import Any, Dict, Optional, Union

import requests

from sportsdataverse.hockeytech._leagues import get_config, resolve_api_key

_UA = "Mozilla/5.0 (compatible; sportsdataverse/hockeytech)"
_CALLBACK_RE = re.compile(r"^[A-Za-z_$][\w.$]*\(")
_RATE_LIMIT_S = 0.4
_last_request_ts = 0.0


def _strip_jsonp(text: str) -> str:
    """Strip an ``angular.callbacks._N( ... )`` or bare ``( ... )`` JSONP wrapper."""
    text = text.strip()
    if _CALLBACK_RE.match(text) and text.endswith(")"):
        text = text[text.index("(") + 1 : -1]
    elif text.startswith("(") and text.endswith(")"):
        text = text[1:-1]
    return text.strip()


def _build_url(league: str, feed: str, view: str, params: Optional[Dict[str, Any]] = None) -> str:
    cfg = get_config(league)
    merged = {
        "feed": feed,
        "view": view,
        "key": resolve_api_key(league, view=view),
        "client_code": cfg.client_code,
        "site_id": str(cfg.site_id),
        "lang": "en",
    }
    if params:
        merged.update({k: str(v) for k, v in params.items() if v is not None})
    return cfg.base_url + "?" + urllib.parse.urlencode(merged)


def hockeytech_api(
    league: str,
    feed: str,
    view: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    timeout: int = 30,
    max_retries: int = 3,
    **kwargs,
) -> Union[Dict[str, Any], list, None]:
    """Fetch + parse one HockeyTech feed call. Returns parsed JSON (dict/list) or None."""
    global _last_request_ts
    url = _build_url(league, feed, view, params)
    headers = {"User-Agent": _UA, "Accept": "application/json", "Referer": "https://www.thepwhl.com/"}
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        elapsed = time.monotonic() - _last_request_ts
        if elapsed < _RATE_LIMIT_S:
            time.sleep(_RATE_LIMIT_S - elapsed)
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, **kwargs)
            _last_request_ts = time.monotonic()
            if resp.status_code == 200:
                return json.loads(_strip_jsonp(resp.text))
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_exc = exc
            time.sleep(1)
    if last_exc is not None:
        from sportsdataverse._codegen_runtime import cli_warn

        cli_warn(f"hockeytech_api({league}/{feed}/{view}) failed: {last_exc}")
    return None
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_client.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_client.py tests/hockeytech/test_client.py
git commit -m "feat(hockeytech): JSONP client (URL build, callback strip, retry, rate-limit)"
```

---

### Task A1.3: Capture live fixtures

**Files:**
- Create: `tests/fixtures/hockeytech/_capture.py`
- Create (output): `tests/fixtures/hockeytech/*.json`

- [ ] **Step 1: Write the capture script**

```python
# tests/fixtures/hockeytech/_capture.py
"""Capture HockeyTech fixtures. Run manually (hits the live API):
    python tests/fixtures/hockeytech/_capture.py
Writes <stem>.json (JSONP already stripped) next to this file.
"""

from __future__ import annotations

import json
import pathlib

from sportsdataverse.hockeytech._client import hockeytech_api

HERE = pathlib.Path(__file__).parent
CAPTURES = {
    "pwhl_seasons": ("pwhl", "modulekit", "seasons", {}),
    "pwhl_schedule_2025": ("pwhl", "modulekit", "scorebar",
                            {"numberofdaysback": 400, "numberofdaysahead": 0, "limit": 200, "league_id": 1}),
    "pwhl_pbp_42": ("pwhl", "statviewfeed", "gameCenterPlayByPlay", {"game_id": 42, "league_id": ""}),
    "pwhl_gameshifts_42": ("pwhl", "modulekit", "gameshifts", {"game_id": 42}),
    "pwhl_standings_5": ("pwhl", "statviewfeed", "teams",
                          {"groupTeamsBy": "division", "context": "overall", "special": "false",
                           "league_id": 1, "sort": "points", "season": 5}),
    "pwhl_teams_5": ("pwhl", "modulekit", "teamsbyseason", {"season": 5}),
    "pwhl_roster_1_5": ("pwhl", "modulekit", "roster", {"team_id": 1, "season_id": 5}),
    "pwhl_player_stats_27": ("pwhl", "modulekit", "player", {"player_id": 27, "category": "seasonstats"}),
    "pwhl_leaders_5": ("pwhl", "statviewfeed", "leadersExtended",
                        {"season": 5, "team_id": 0, "playerTypes": "skaters",
                         "skaterStatTypes": "points,goals", "activeOnly": 0}),
    "pwhl_game_summary_42": ("pwhl", "gc", "gamesummary", {"game_id": 42}),
}


def main() -> None:
    for stem, (lg, feed, view, params) in CAPTURES.items():
        data = hockeytech_api(lg, feed, view, params)
        (HERE / f"{stem}.json").write_text(json.dumps(data, indent=1), encoding="utf-8")
        print("wrote", stem)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the capture**

Run: `python tests/fixtures/hockeytech/_capture.py`
Expected: prints `wrote pwhl_seasons` … `wrote pwhl_game_summary_42`; 10 JSON files created. (If a junior `gameshifts`/PBP capture is needed, add `ahl_pbp_*` etc. entries with completed game ids found via each league's scorebar.)

- [ ] **Step 3: Commit**

```bash
git add tests/fixtures/hockeytech/_capture.py tests/fixtures/hockeytech/*.json
git commit -m "test(hockeytech): capture PWHL JSON fixtures"
```

---

### Task A1.4: Seasons parser + `resolve_season_id`

**Files:**
- Create: `sportsdataverse/hockeytech/_parsers.py`
- Modify: `sportsdataverse/hockeytech/_leagues.py` (replace the `resolve_season_id` stub)
- Test: `tests/hockeytech/test_parsers.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/hockeytech/test_parsers.py
from __future__ import annotations

import polars as pl

from tests.conftest import load_fixture


def _load(stem):
    return load_fixture("hockeytech", stem)


def test_parse_seasons_columns_and_year_derivation():
    from sportsdataverse.hockeytech._parsers import parse_seasons

    df = parse_seasons(_load("pwhl_seasons"))
    assert isinstance(df, pl.DataFrame)
    for col in ("season_id", "season_name", "season_short", "season_yr", "game_type_label"):
        assert col in df.columns
    # "2024-25 Regular Season" -> end-year 2025, label "regular"
    row = df.filter(pl.col("season_name").str.contains("2024-25 Regular"))
    if row.height:
        assert row["season_yr"][0] == 2025
        assert row["game_type_label"][0] == "regular"


def test_resolve_season_id_end_year_to_integer(monkeypatch):
    from sportsdataverse.hockeytech import _parsers, _leagues

    monkeypatch.setattr(_leagues, "_fetch_seasons_raw", lambda league: _load("pwhl_seasons"))
    sid = _leagues.resolve_season_id("pwhl", season=2025, game_type="regular")
    assert isinstance(sid, int) and sid > 0


def test_resolve_season_id_passthrough_explicit_id():
    from sportsdataverse.hockeytech import _leagues

    assert _leagues.resolve_season_id("pwhl", season_id=5) == 5
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_parsers.py -q`
Expected: FAIL — no `parse_seasons`.

- [ ] **Step 3: Implement `_parsers.py` base + `parse_seasons` + `_snake_columns`**

```python
# sportsdataverse/hockeytech/_parsers.py
"""Pure HockeyTech parsers: parsed JSON (dict/list) -> snake_cased polars frame.

No network here — tests drive these from captured fixtures. Every parser
tolerates empty/None payloads by returning a zero-row frame.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [underscore(str(c)).replace(".", "_").replace("__", "_") for c in df.columns]
    return df


def _to_frame(records: List[Dict[str, Any]], return_as_pandas: bool) -> Any:
    pdf = pd.json_normalize(records or [], sep="_")
    pdf = _snake_columns(pdf)
    return pdf if return_as_pandas else pl.from_pandas(pdf) if len(pdf) else pl.DataFrame()


def _sitekit(payload: Any, key: str) -> Any:
    return ((payload or {}).get("SiteKit", {}) or {}).get(key)


def _derive_season_year(name: str) -> Optional[int]:
    m = re.search(r"(\d{4})-(\d{2})", name or "")
    if m:
        return int(m.group(1)[:2]) * 100 + int(m.group(2))
    m2 = re.search(r"(\d{4})", name or "")
    return int(m2.group(1)) if m2 else None


def _game_type_label(name: str) -> str:
    n = (name or "").lower()
    if re.search(r"pre[- ]?season", n):
        return "preseason"
    if re.search(r"playoff|post", n):
        return "playoffs"
    return "regular"


def parse_seasons(payload: Any, return_as_pandas: bool = False) -> Any:
    raw = _sitekit(payload, "Seasons") or []
    rows = []
    for s in raw:
        name = s.get("season_name")
        rows.append(
            {
                "season_id": int(s.get("season_id")) if s.get("season_id") else None,
                "season_name": name,
                "season_short": s.get("shortname"),
                "career": s.get("career", "0"),
                "playoff": s.get("playoff", "0"),
                "start_date": s.get("start_date"),
                "end_date": s.get("end_date"),
                "season_yr": _derive_season_year(name),
                "game_type_label": _game_type_label(name),
            }
        )
    return _to_frame(rows, return_as_pandas)
```

- [ ] **Step 4: Replace the `resolve_season_id` stub in `_leagues.py`**

```python
# sportsdataverse/hockeytech/_leagues.py  (append; remove the NotImplementedError stub)

# Hardcoded PWHL fallback (ported from fastRhockey pwhl_season_id) used when the
# live seasons feed is unreachable.
_PWHL_SEASON_FALLBACK = [
    {"season_id": 1, "season_yr": 2024, "game_type_label": "regular"},
    {"season_id": 3, "season_yr": 2024, "game_type_label": "playoffs"},
    {"season_id": 5, "season_yr": 2025, "game_type_label": "regular"},
    {"season_id": 6, "season_yr": 2025, "game_type_label": "playoffs"},
    {"season_id": 8, "season_yr": 2026, "game_type_label": "regular"},
]


def _fetch_seasons_raw(league: str):
    from sportsdataverse.hockeytech._client import hockeytech_api

    return hockeytech_api(league, "modulekit", "seasons", {})


def resolve_season_id(league: str, season=None, game_type: str = "regular", season_id=None):
    """Resolve an end-year ``season`` (e.g. 2025) to the integer HockeyTech
    ``season_id``. An explicit ``season_id`` short-circuits. PWHL falls back to a
    hardcoded table if the live feed is unreachable.
    """
    if season_id is not None:
        return int(season_id)
    if season is None:
        raise ValueError("Provide either season (end-year) or season_id")

    from sportsdataverse.hockeytech._parsers import parse_seasons

    payload = _fetch_seasons_raw(league)
    df = parse_seasons(payload)
    if df.height:
        hit = df.filter((df["season_yr"] == int(season)) & (df["game_type_label"] == game_type))
        if hit.height:
            return int(hit["season_id"][0])
    if league == "pwhl":
        for row in _PWHL_SEASON_FALLBACK:
            if row["season_yr"] == int(season) and row["game_type_label"] == game_type:
                return row["season_id"]
    raise ValueError(f"No {league} season for season={season}, game_type={game_type}")
```

- [ ] **Step 5: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_parsers.py -q`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add sportsdataverse/hockeytech/_parsers.py sportsdataverse/hockeytech/_leagues.py tests/hockeytech/test_parsers.py
git commit -m "feat(hockeytech): seasons parser + end-year season_id resolution"
```

---

### Task A1.5: Schedule / scorebar / standings / teams / roster parsers

**Files:**
- Modify: `sportsdataverse/hockeytech/_parsers.py`
- Test: `tests/hockeytech/test_parsers.py`

- [ ] **Step 1: Add failing tests**

```python
# append to tests/hockeytech/test_parsers.py
def test_parse_schedule_one_row_per_game_with_core_cols():
    from sportsdataverse.hockeytech._parsers import parse_schedule

    df = parse_schedule(_load("pwhl_schedule_2025"))
    import polars as pl
    assert isinstance(df, pl.DataFrame) and df.height > 0
    for col in ("game_id", "game_date", "home_team", "home_team_id",
                "away_team", "away_team_id", "home_score", "away_score"):
        assert col in df.columns


def test_parse_standings_has_team_rank_and_points():
    from sportsdataverse.hockeytech._parsers import parse_standings

    df = parse_standings(_load("pwhl_standings_5"))
    for col in ("team", "team_rank", "games_played", "points", "wins", "losses"):
        assert col in df.columns


def test_parse_teams_and_roster():
    from sportsdataverse.hockeytech._parsers import parse_teams, parse_roster

    teams = parse_teams(_load("pwhl_teams_5"))
    assert "team_name" in teams.columns and "team_id" in teams.columns
    roster = parse_roster(_load("pwhl_roster_1_5"))
    assert roster.height > 0
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_parsers.py -q`
Expected: FAIL — `parse_schedule`/`parse_standings`/`parse_teams`/`parse_roster` undefined.

- [ ] **Step 3: Implement the parsers**

> Implementer note: open each fixture (`tests/fixtures/hockeytech/pwhl_*.json`) to confirm the exact nesting before writing. Scorebar games live under `SiteKit.Scorebar`; standings under the statviewfeed `teams` payload (a list of section dicts → `sections[].data[].row`); teamsbyseason under `SiteKit.Teamsbyseason`; roster under `SiteKit.Roster`. Map raw keys to the fastRhockey column names below, then `_to_frame` snake-cases the remainder.

```python
# append to sportsdataverse/hockeytech/_parsers.py

# fastRhockey column contracts (see design doc §Output). Renames applied
# explicitly; everything else is snake_cased by _to_frame.
_SCOREBAR_RENAME = {
    "ID": "game_id", "GameDateISO8601": "game_date", "GameStatusStringLong": "game_status",
    "HomeLongName": "home_team", "HomeID": "home_team_id", "HomeGoals": "home_score",
    "VisitorLongName": "away_team", "VisitorID": "away_team_id", "VisitorGoals": "away_score",
    "venue_name": "venue", "SeasonID": "season_id",
}


def parse_schedule(payload: Any, return_as_pandas: bool = False) -> Any:
    games = _sitekit(payload, "Scorebar") or []
    rows = []
    for g in games:
        row = {new: g.get(old) for old, new in _SCOREBAR_RENAME.items()}
        row["game_type"] = g.get("game_type")
        rows.append(row)
    return _to_frame(rows, return_as_pandas)


def parse_standings(payload: Any, return_as_pandas: bool = False) -> Any:
    # statviewfeed/teams: list of section dicts; each has ["sections"][i]["data"][j]["row"]
    rows: List[Dict[str, Any]] = []
    sections = payload if isinstance(payload, list) else (payload or {}).get("sections", [])
    for sec in sections or []:
        for blk in sec.get("sections", [sec]) if isinstance(sec, dict) else []:
            for item in blk.get("data", []) or []:
                r = item.get("row") if isinstance(item, dict) else None
                if isinstance(r, dict):
                    rows.append(r)
    df = _to_frame(rows, False)
    # Normalize the few names fastRhockey fixes explicitly.
    ren = {"rank": "team_rank", "name": "team", "code": "team_code", "wins": "wins", "losses": "losses"}
    df = df.rename({k: v for k, v in ren.items() if k in df.columns})
    return df.to_pandas() if return_as_pandas else df


def parse_teams(payload: Any, return_as_pandas: bool = False) -> Any:
    raw = _sitekit(payload, "Teamsbyseason") or []
    rows = []
    for t in raw:
        rows.append(
            {
                "team_name": t.get("name"),
                "team_id": t.get("id"),
                "team_code": t.get("code"),
                "team_nickname": t.get("nickname"),
                "team_label": t.get("city"),
                "division": t.get("division_id") or t.get("division"),
                "team_logo": t.get("team_logo_url") or t.get("logo"),
            }
        )
    return _to_frame(rows, return_as_pandas)


def parse_roster(payload: Any, return_as_pandas: bool = False) -> Any:
    raw = _sitekit(payload, "Roster") or []
    return _to_frame(list(raw), return_as_pandas)
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_parsers.py -q`
Expected: PASS. If a real fixture nests differently than assumed, adjust the accessor (not the test's column expectations) until green.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_parsers.py tests/hockeytech/test_parsers.py
git commit -m "feat(hockeytech): schedule/standings/teams/roster parsers"
```

---

### Task A1.6: PBP parser (dialect a) — one row per event

**Files:**
- Modify: `sportsdataverse/hockeytech/_parsers.py`
- Test: `tests/hockeytech/test_parsers.py`

- [ ] **Step 1: Add failing test**

```python
# append to tests/hockeytech/test_parsers.py
def test_parse_pbp_a_one_row_per_event_with_fastrhockey_cols():
    from sportsdataverse.hockeytech._parsers import parse_pbp

    df = parse_pbp(_load("pwhl_pbp_42"), pbp_style="hockeytech_a", game_id=42)
    import polars as pl
    assert isinstance(df, pl.DataFrame) and df.height > 0
    # fastRhockey parity columns
    for col in ("game_id", "event", "team_id", "period_of_game", "time_of_period",
                "player_id", "player_name_first", "player_name_last",
                "x_coord", "y_coord", "goal", "goalie_id"):
        assert col in df.columns, f"missing {col}"
    # richer events surfaced beyond fastRhockey
    assert set(df["event"].unique().to_list()) & {"shot", "blocked_shot", "goal", "faceoff", "penalty"}
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_parsers.py::test_parse_pbp_a_one_row_per_event_with_fastrhockey_cols -q`
Expected: FAIL — `parse_pbp` undefined.

- [ ] **Step 3: Implement `parse_pbp` (dialect a) — transliterate fastRhockey `pwhl_pbp` + add blocked_shot/hit**

> Implementer reference: fastRhockey `R/pwhl_pbp.R` is the column contract. The
> statviewfeed payload is a flat list of `{event, details:{...}}`. Map each
> event type's `details` to the row schema below. `xLocation`/`yLocation` →
> `x_coord`/`y_coord` (explicit fastRhockey rename, snake-case pass keeps them).
> On-ice `plus`/`minus` arrays from `goal` events fill `plus_player_*`/`minus_player_*`.

```python
# append to sportsdataverse/hockeytech/_parsers.py

def _player(d: Optional[dict]) -> Dict[str, Any]:
    d = d or {}
    return {"id": d.get("id"), "first": d.get("firstName"), "last": d.get("lastName"),
            "pos": d.get("position")}


def parse_pbp(payload: Any, pbp_style: str = "hockeytech_a", game_id: Optional[int] = None,
              return_as_pandas: bool = False) -> Any:
    events = payload if isinstance(payload, list) else []
    rows: List[Dict[str, Any]] = []
    for e in events:
        if not isinstance(e, dict):
            continue
        ev = e.get("event")
        d = e.get("details", {}) or {}
        period = (d.get("period") or {}).get("id") if isinstance(d.get("period"), dict) else d.get("period")
        base = {
            "game_id": game_id,
            "event": ev,
            "team_id": d.get("team_id") or d.get("shooterTeamId") or d.get("teamId"),
            "period_of_game": period,
            "time_of_period": d.get("time"),
            "x_coord": d.get("xLocation"),
            "y_coord": d.get("yLocation"),
        }
        if ev in ("shot", "blocked_shot"):
            sh = _player(d.get("shooter"))
            gl = _player(d.get("goalie"))
            base.update({
                "player_id": sh["id"], "player_name_first": sh["first"],
                "player_name_last": sh["last"], "player_position": sh["pos"],
                "player_team_id": d.get("shooterTeamId"),
                "event_type": d.get("shotType"), "shot_quality": d.get("shotQuality"),
                "goal": bool(d.get("isGoal")) if ev == "shot" else False,
                "goalie_id": gl["id"], "goalie_first": gl["first"], "goalie_last": gl["last"],
            })
        elif ev == "goal":
            sc = _player(d.get("scoredBy"))
            assists = d.get("assists") or []
            props = d.get("properties") or {}
            base.update({
                "player_id": sc["id"], "player_name_first": sc["first"],
                "player_name_last": sc["last"], "player_position": sc["pos"],
                "goal": True,
                "empty_net": props.get("isEmptyNet"), "game_winner": props.get("isGameWinningGoal"),
            })
            for i, a in enumerate(assists[:2], start=2):
                pa = _player(a)
                base[f"player_{['two','three'][i-2]}_id"] = pa["id"]
                base[f"player_{['two','three'][i-2]}_name_first"] = pa["first"]
                base[f"player_{['two','three'][i-2]}_name_last"] = pa["last"]
            for sign, key in (("plus", "plus"), ("minus", "minus")):
                for j, p in enumerate(d.get(key, []) or [], start=1):
                    pp = _player(p)
                    base[f"{sign}_player_{j}_id"] = pp["id"]
                    base[f"{sign}_player_{j}_first"] = pp["first"]
                    base[f"{sign}_player_{j}_last"] = pp["last"]
        elif ev == "faceoff":
            base.update({"home_win": d.get("homeWin"),
                         "player_id": (d.get("homePlayer") or {}).get("id")})
        elif ev == "penalty":
            base.update({"penalty_length": d.get("minutes"), "event_type": d.get("description"),
                         "power_play": d.get("isPowerPlay")})
        rows.append(base)
    return _to_frame(rows, return_as_pandas)
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_parsers.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_parsers.py tests/hockeytech/test_parsers.py
git commit -m "feat(hockeytech): play-by-play parser (dialect a) with blocked_shot/hit"
```

---

### Task A1.7: Shifts parser

**Files:**
- Modify: `sportsdataverse/hockeytech/_parsers.py`
- Test: `tests/hockeytech/test_parsers.py`

- [ ] **Step 1: Add failing test**

```python
# append to tests/hockeytech/test_parsers.py
def test_parse_shifts_one_row_per_stint():
    from sportsdataverse.hockeytech._parsers import parse_shifts

    df = parse_shifts(_load("pwhl_gameshifts_42"), game_id=42)
    import polars as pl
    assert isinstance(df, pl.DataFrame) and df.height > 0
    for col in ("game_id", "player_id", "first_name", "last_name", "home",
                "period", "start_time", "end_time", "length", "start_s", "end_s"):
        assert col in df.columns
    # countdown clock: start_s > end_s within a shift
    assert (df["start_s"] >= df["end_s"]).all()
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_parsers.py::test_parse_shifts_one_row_per_stint -q`
Expected: FAIL — `parse_shifts` undefined.

- [ ] **Step 3: Implement `parse_shifts` (+ a shared `mmss_to_seconds`)**

```python
# append to sportsdataverse/hockeytech/_parsers.py

def mmss_to_seconds(value: Any) -> Optional[int]:
    """'03:16' -> 196. None/'' -> None."""
    if value in (None, ""):
        return None
    try:
        m, s = str(value).split(":")
        return int(m) * 60 + int(s)
    except (ValueError, AttributeError):
        return None


def parse_shifts(payload: Any, game_id: Optional[int] = None, return_as_pandas: bool = False) -> Any:
    gs = _sitekit(payload, "Gameshifts") or {}
    rows: List[Dict[str, Any]] = []
    for side in ("home", "visitor"):
        for player in gs.get(side, []) or []:
            for sh in player.get("shifts", []) or []:
                rows.append(
                    {
                        "game_id": game_id,
                        "player_id": player.get("player_id"),
                        "first_name": player.get("first_name"),
                        "last_name": player.get("last_name"),
                        "jersey_number": player.get("jersey_number"),
                        "home": int(player.get("home", 1 if side == "home" else 0)),
                        "period": int(sh.get("period")) if sh.get("period") else None,
                        "start_time": sh.get("start_time"),
                        "end_time": sh.get("end_time"),
                        "length": sh.get("length"),
                        "start_s": mmss_to_seconds(sh.get("start_time")),
                        "end_s": mmss_to_seconds(sh.get("end_time")),
                        "goal_on_shift": int(sh.get("goal_on_shift", 0) or 0),
                        "penalty_on_shift": int(sh.get("penalty_on_shift", 0) or 0),
                    }
                )
    return _to_frame(rows, return_as_pandas)
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_parsers.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_parsers.py tests/hockeytech/test_parsers.py
git commit -m "feat(hockeytech): shifts parser with countdown-clock seconds"
```

---

### Task A1.8: Remaining PWHL parsers (player_stats, leaders, game_summary, player_box, player_info, game_log, search, streaks, transactions, playoff_bracket, scorebar, game_info, stats)

**Files:**
- Modify: `sportsdataverse/hockeytech/_parsers.py`
- Test: `tests/hockeytech/test_parsers.py`

- [ ] **Step 1: Add failing tests for the captured ones (player_stats, leaders, game_summary)**

```python
# append to tests/hockeytech/test_parsers.py
def test_parse_player_stats_has_season_and_points():
    from sportsdataverse.hockeytech._parsers import parse_player_stats
    df = parse_player_stats(_load("pwhl_player_stats_27"))
    for col in ("season_id", "season_name", "games_played", "points", "team_id"):
        assert col in df.columns


def test_parse_leaders_has_player_and_team():
    from sportsdataverse.hockeytech._parsers import parse_leaders
    df = parse_leaders(_load("pwhl_leaders_5"))
    for col in ("player_id", "first_name", "last_name", "team_id"):
        assert col in df.columns


def test_parse_game_summary_returns_named_subframes():
    from sportsdataverse.hockeytech._parsers import parse_game_summary
    out = parse_game_summary(_load("pwhl_game_summary_42"), game_id=42)
    assert isinstance(out, dict)
    assert "game" in out and "goals" in out and "penalties" in out and "shots_by_period" in out
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_parsers.py -k "player_stats or leaders or game_summary" -q`
Expected: FAIL.

- [ ] **Step 3: Implement the parsers (match fastRhockey `@return` tables)**

> Implementer reference for each column contract: `R/pwhl_player_stats.R`,
> `R/pwhl_leaders.R`, `R/pwhl_game_summary.R`, `R/pwhl_player_box.R`,
> `R/pwhl_player_info.R`, `R/pwhl_player_game_log.R`, `R/pwhl_player_search.R`,
> `R/pwhl_streaks.R`, `R/pwhl_transactions.R`, `R/pwhl_playoff_bracket.R`,
> `R/pwhl_scorebar.R`, `R/pwhl_game_info.R`, `R/pwhl_stats.R` in the fastRhockey
> repo. Each is a flat list (or `SiteKit.<Key>` list) → `_to_frame`; the explicit
> renames mirror the R `dplyr::rename()`/`clean_names()` output. `game_summary`
> returns a **dict of frames** (game header, goals, penalties, shots_by_period,
> three_stars, rosters) parsed from the `gc.gamesummary` payload.

```python
# append to sportsdataverse/hockeytech/_parsers.py

def parse_player_stats(payload: Any, return_as_pandas: bool = False) -> Any:
    raw = _sitekit(payload, "Player") or {}
    seasons = raw.get("seasons") if isinstance(raw, dict) else raw
    return _to_frame(list(seasons or []), return_as_pandas)


def parse_leaders(payload: Any, return_as_pandas: bool = False) -> Any:
    # statviewfeed/leadersExtended: nested by stat category; flatten player rows.
    rows: List[Dict[str, Any]] = []
    data = payload if isinstance(payload, list) else (payload or {}).get("leaders", [])
    for cat in data or []:
        for p in (cat.get("leaders") or cat.get("players") or []):
            player = p.get("player", p) if isinstance(p, dict) else {}
            rows.append(player)
    return _to_frame(rows, return_as_pandas)


def parse_game_summary(payload: Any, game_id: Optional[int] = None) -> Dict[str, Any]:
    gc = (payload or {}).get("GC", {}) or {}
    summary = gc.get("Gamesummary", {}) or {}
    game = [{
        "game_id": game_id,
        "date": summary.get("date_played"),
        "status": summary.get("status"),
        "venue": summary.get("venue"),
        "attendance": summary.get("attendance"),
        "home_team": (summary.get("home") or {}).get("name"),
        "home_team_id": (summary.get("home") or {}).get("id"),
        "home_score": (summary.get("totalGoals") or {}).get("home"),
        "away_team": (summary.get("visitor") or {}).get("name"),
        "away_team_id": (summary.get("visitor") or {}).get("id"),
        "away_score": (summary.get("totalGoals") or {}).get("visitor"),
    }]
    goals = list(summary.get("goals", []) or [])
    penalties = list(summary.get("penalties", []) or [])
    shots = list(summary.get("shotsByPeriod", []) or [])
    stars = list(summary.get("threeStars", []) or [])
    return {
        "game": _to_frame(game, False),
        "goals": _to_frame(goals, False),
        "penalties": _to_frame(penalties, False),
        "shots_by_period": _to_frame(shots, False),
        "three_stars": _to_frame(stars, False),
    }
```

> The remaining flat parsers (`parse_player_info`, `parse_player_game_log`,
> `parse_player_search`, `parse_streaks`, `parse_transactions`,
> `parse_playoff_bracket`, `parse_scorebar`, `parse_game_info`, `parse_stats`)
> follow the same shape: pull the `SiteKit.<Key>` list, apply the fastRhockey
> rename map from the cited R file, return `_to_frame(rows, return_as_pandas)`.
> Add one test per parser as its fixture is captured; until a fixture exists,
> cover it with a synthetic-dict unit test asserting the column contract.

```python
def _flat_sitekit_parser(key: str, rename: Optional[Dict[str, str]] = None):
    def _parser(payload: Any, return_as_pandas: bool = False) -> Any:
        raw = _sitekit(payload, key) or []
        if rename:
            raw = [{rename.get(k, k): v for k, v in r.items()} for r in raw]
        return _to_frame(list(raw), return_as_pandas)
    return _parser


parse_player_info = _flat_sitekit_parser("Player")
parse_player_game_log = _flat_sitekit_parser("Player")
parse_player_search = _flat_sitekit_parser("Searchplayers")
parse_streaks = _flat_sitekit_parser("Streaks")
parse_transactions = _flat_sitekit_parser("Transactions")
parse_playoff_bracket = _flat_sitekit_parser("Brackets")
parse_scorebar = _flat_sitekit_parser("Scorebar")
parse_stats = _flat_sitekit_parser("Statviewtype")
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_parsers.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_parsers.py tests/hockeytech/test_parsers.py
git commit -m "feat(hockeytech): remaining PWHL parsers (stats/leaders/game_summary/etc.)"
```

---

### Task A1.9: PWHL public API (`pwhl_api.py`) — wire parsers to live calls

**Files:**
- Create: `sportsdataverse/pwhl/pwhl_api.py`
- Modify: `sportsdataverse/pwhl/__init__.py`
- Test: `tests/hockeytech/test_public_surface.py`

- [ ] **Step 1: Write the failing test (surface + offline monkeypatched call)**

```python
# tests/hockeytech/test_public_surface.py
from __future__ import annotations

import polars as pl
import pytest

from tests.conftest import load_fixture


def test_pwhl_api_exports_full_parity_surface():
    import sportsdataverse.pwhl as pwhl

    expected = {
        "pwhl_schedule", "pwhl_scorebar", "pwhl_game_info", "pwhl_game_summary",
        "pwhl_pbp", "pwhl_player_box", "pwhl_teams", "pwhl_team_roster",
        "pwhl_standings", "pwhl_player_info", "pwhl_player_stats",
        "pwhl_player_game_log", "pwhl_player_search", "pwhl_stats", "pwhl_leaders",
        "pwhl_streaks", "pwhl_transactions", "pwhl_playoff_bracket",
        "pwhl_season_id", "most_recent_pwhl_season",
    }
    missing = expected - set(dir(pwhl))
    assert not missing, f"missing PWHL functions: {sorted(missing)}"


def test_pwhl_pbp_parses_via_monkeypatched_client(monkeypatch):
    import sportsdataverse.pwhl.pwhl_api as api

    monkeypatch.setattr(api, "hockeytech_api",
                        lambda *a, **k: load_fixture("hockeytech", "pwhl_pbp_42"))
    df = api.pwhl_pbp(game_id=42)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert "game_id" in df.columns
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_public_surface.py -q`
Expected: FAIL — functions not exported.

- [ ] **Step 3: Implement `pwhl_api.py`**

> Each function: resolve season (where applicable) → `hockeytech_api(...)` →
> parser → frame. Signatures mirror fastRhockey (season = end-year). `pwhl_pbp`
> uses the PBP-key override automatically (handled in `_client`).

```python
# sportsdataverse/pwhl/pwhl_api.py
"""Live PWHL HockeyTech wrappers — full output parity with fastRhockey (R).

Season arguments use the **end year** (e.g. ``2026`` for 2025-26), matching
fastRhockey; they are resolved to the integer HockeyTech ``season_id``.
"""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse.hockeytech import hockeytech_api, resolve_season_id
from sportsdataverse.hockeytech import _parsers as P

__all__ = [
    "pwhl_schedule", "pwhl_scorebar", "pwhl_game_info", "pwhl_game_summary",
    "pwhl_pbp", "pwhl_player_box", "pwhl_teams", "pwhl_team_roster",
    "pwhl_standings", "pwhl_player_info", "pwhl_player_stats",
    "pwhl_player_game_log", "pwhl_player_search", "pwhl_stats", "pwhl_leaders",
    "pwhl_streaks", "pwhl_transactions", "pwhl_playoff_bracket",
    "pwhl_season_id", "most_recent_pwhl_season",
]

_LG = "pwhl"


def pwhl_season_id(return_as_pandas: bool = False) -> Any:
    """All PWHL seasons with end-year + game-type labels (HockeyTech ``seasons``)."""
    return P.parse_seasons(hockeytech_api(_LG, "modulekit", "seasons", {}), return_as_pandas)


def most_recent_pwhl_season() -> int:
    """Most-recent PWHL season as an end-year integer (max ``season_yr``)."""
    df = pwhl_season_id()
    return int(df["season_yr"].max()) if df.height else 2026


def pwhl_schedule(season=None, season_id=None, return_as_pandas: bool = False) -> Any:
    """PWHL schedule — one row per game (matches fastRhockey ``pwhl_schedule``)."""
    params = {"numberofdaysback": 10000, "numberofdaysahead": 10000, "limit": 10000, "league_id": 1}
    if season is not None or season_id is not None:
        params["season_id"] = resolve_season_id(_LG, season=season, season_id=season_id)
    return P.parse_schedule(hockeytech_api(_LG, "modulekit", "scorebar", params), return_as_pandas)


def pwhl_pbp(game_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL play-by-play — one row per event (superset of fastRhockey ``pwhl_pbp``)."""
    payload = hockeytech_api(_LG, "statviewfeed", "gameCenterPlayByPlay",
                             {"game_id": game_id, "league_id": ""})
    return P.parse_pbp(payload, pbp_style="hockeytech_a", game_id=game_id,
                       return_as_pandas=return_as_pandas)


def pwhl_standings(season=None, season_id=None, return_as_pandas: bool = False) -> Any:
    sid = resolve_season_id(_LG, season=season or most_recent_pwhl_season(), season_id=season_id)
    payload = hockeytech_api(_LG, "statviewfeed", "teams",
                             {"groupTeamsBy": "division", "context": "overall",
                              "special": "false", "league_id": 1, "sort": "points", "season": sid})
    return P.parse_standings(payload, return_as_pandas)


def pwhl_teams(season=None, season_id=None, return_as_pandas: bool = False) -> Any:
    sid = resolve_season_id(_LG, season=season or most_recent_pwhl_season(), season_id=season_id)
    return P.parse_teams(hockeytech_api(_LG, "modulekit", "teamsbyseason", {"season": sid}),
                         return_as_pandas)


def pwhl_team_roster(team_id: int, season=None, season_id=None, return_as_pandas: bool = False) -> Any:
    sid = resolve_season_id(_LG, season=season or most_recent_pwhl_season(), season_id=season_id)
    return P.parse_roster(hockeytech_api(_LG, "modulekit", "roster",
                                         {"team_id": team_id, "season_id": sid}), return_as_pandas)


def pwhl_player_stats(player_id: int, return_as_pandas: bool = False) -> Any:
    return P.parse_player_stats(hockeytech_api(_LG, "modulekit", "player",
                                {"player_id": player_id, "category": "seasonstats"}), return_as_pandas)


def pwhl_leaders(season=None, season_id=None, return_as_pandas: bool = False) -> Any:
    sid = resolve_season_id(_LG, season=season or most_recent_pwhl_season(), season_id=season_id)
    payload = hockeytech_api(_LG, "statviewfeed", "leadersExtended",
                             {"season": sid, "team_id": 0, "playerTypes": "skaters",
                              "skaterStatTypes": "points,goals", "activeOnly": 0})
    return P.parse_leaders(payload, return_as_pandas)


def pwhl_game_summary(game_id: int) -> dict:
    """PWHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars)."""
    return P.parse_game_summary(hockeytech_api(_LG, "gc", "gamesummary", {"game_id": game_id}),
                                game_id=game_id)


# The remaining parity wrappers follow the identical pattern; each calls the
# feed/view from the design's endpoint catalog and its parser:
def pwhl_scorebar(return_as_pandas: bool = False) -> Any:
    return P.parse_scorebar(hockeytech_api(_LG, "modulekit", "scorebar",
                            {"numberofdaysback": 3, "numberofdaysahead": 3, "limit": 100, "league_id": 1}),
                            return_as_pandas)


def pwhl_game_info(game_id: int, return_as_pandas: bool = False) -> Any:
    return P.parse_game_info(hockeytech_api(_LG, "statviewfeed", "gameSummary",
                             {"game_id": game_id}), return_as_pandas)


def pwhl_player_box(game_id: int, return_as_pandas: bool = False) -> Any:
    return P.parse_player_box(hockeytech_api(_LG, "statviewfeed", "gameSummary",
                              {"game_id": game_id}), game_id=game_id, return_as_pandas=return_as_pandas)


def pwhl_player_info(player_id: int, return_as_pandas: bool = False) -> Any:
    return P.parse_player_info(hockeytech_api(_LG, "statviewfeed", "player",
                               {"player_id": player_id}), return_as_pandas)


def pwhl_player_game_log(player_id: int, return_as_pandas: bool = False) -> Any:
    return P.parse_player_game_log(hockeytech_api(_LG, "modulekit", "player",
                                   {"player_id": player_id, "category": "gamebygame"}), return_as_pandas)


def pwhl_player_search(name: str, return_as_pandas: bool = False) -> Any:
    return P.parse_player_search(hockeytech_api(_LG, "modulekit", "searchplayers",
                                 {"search_term": name}), return_as_pandas)


def pwhl_stats(season=None, season_id=None, position: str = "skaters", return_as_pandas: bool = False) -> Any:
    sid = resolve_season_id(_LG, season=season or most_recent_pwhl_season(), season_id=season_id)
    return P.parse_stats(hockeytech_api(_LG, "modulekit", "statviewtype",
                         {"type": position, "season_id": sid}), return_as_pandas)


def pwhl_streaks(return_as_pandas: bool = False) -> Any:
    return P.parse_streaks(hockeytech_api(_LG, "modulekit", "streaks", {"league_id": 1}), return_as_pandas)


def pwhl_transactions(return_as_pandas: bool = False) -> Any:
    return P.parse_transactions(hockeytech_api(_LG, "modulekit", "transactions", {"league_id": 1}),
                                return_as_pandas)


def pwhl_playoff_bracket(season=None, season_id=None, return_as_pandas: bool = False) -> Any:
    sid = resolve_season_id(_LG, season=season or most_recent_pwhl_season(),
                            game_type="playoffs", season_id=season_id)
    return P.parse_playoff_bracket(hockeytech_api(_LG, "modulekit", "brackets",
                                   {"season_id": sid, "league_id": 1}), return_as_pandas)
```

> Add the two small parsers referenced above (`parse_game_info`, `parse_player_box`)
> to `_parsers.py` using the `gameSummary` payload, mirroring `R/pwhl_game_info.R`
> and `R/pwhl_player_box.R`. Until their fixtures are captured, unit-test them with
> a synthetic dict asserting the fastRhockey column contract.

- [ ] **Step 4: Wire into the package `__init__.py`**

```python
# sportsdataverse/pwhl/__init__.py  — add near the other imports
from sportsdataverse.pwhl.pwhl_api import *  # noqa: F401,F403
```

- [ ] **Step 5: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_public_surface.py -q`
Expected: PASS (surface + monkeypatched pbp).

- [ ] **Step 6: Commit**

```bash
git add sportsdataverse/pwhl/pwhl_api.py sportsdataverse/pwhl/__init__.py sportsdataverse/hockeytech/_parsers.py tests/hockeytech/test_public_surface.py
git commit -m "feat(pwhl): live pwhl_*() parity wrappers over the HockeyTech core"
```

---

## Phase A2 — Analytics (source of truth)

### Task A2.1: Shot geometry — distance, angle, scoring chance

**Files:**
- Create: `sportsdataverse/hockeytech/_analytics.py`
- Test: `tests/hockeytech/test_analytics.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/hockeytech/test_analytics.py
from __future__ import annotations

import math
import polars as pl


def test_shot_distance_angle_on_known_point():
    from sportsdataverse.hockeytech._analytics import add_shot_distance_angle

    df = pl.DataFrame({"event": ["shot"], "x_coord": [25.0], "y_coord": [0.0]})
    out = add_shot_distance_angle(df, goal_x=89.0)
    assert "shot_distance" in out.columns and "shot_angle" in out.columns
    # straight on from x=25 -> 64 ft, angle 0
    assert abs(out["shot_distance"][0] - 64.0) < 1e-6
    assert abs(out["shot_angle"][0] - 0.0) < 1e-6


def test_scoring_chance_flags_close_shots():
    from sportsdataverse.hockeytech._analytics import add_shot_distance_angle, scoring_chances

    df = pl.DataFrame({"event": ["shot", "shot"], "x_coord": [80.0, 10.0], "y_coord": [2.0, 2.0]})
    out = scoring_chances(add_shot_distance_angle(df))
    assert "scoring_chance" in out.columns
    assert out["scoring_chance"][0] is True   # 9 ft from net
    assert out["scoring_chance"][1] is False  # ~79 ft from net
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_analytics.py -q`
Expected: FAIL — no `_analytics`.

- [ ] **Step 3: Implement geometry**

```python
# sportsdataverse/hockeytech/_analytics.py
"""Pure HockeyTech analytics: frame(s) -> frame. No network.

Corsi/Fenwick caveat: the HockeyTech feed has no missed-shot event, so shot
attempts = shot + blocked_shot + goal. Both metrics are proxies; every output
carries ``corsi_includes_missed = False``.
"""

from __future__ import annotations

from typing import Any

import polars as pl

_SCORING_CHANCE_FT = 25.0  # home-plate-ish distance threshold


def add_shot_distance_angle(pbp: pl.DataFrame, goal_x: float = 89.0) -> pl.DataFrame:
    """Add ``shot_distance``/``shot_angle`` (feet/degrees) for shot-type events.

    Assumes coordinates already transformed to a standard rink frame (offensive
    net at +goal_x, y=0). Non-shot rows get nulls.
    """
    if pbp.height == 0:
        return pbp.with_columns(shot_distance=pl.lit(None, dtype=pl.Float64),
                                shot_angle=pl.lit(None, dtype=pl.Float64))
    dx = (pl.lit(goal_x) - pl.col("x_coord").abs())
    dy = pl.col("y_coord")
    dist = (dx**2 + dy**2).sqrt()
    angle = (dy.arctan2(dx).abs() * 180.0 / 3.141592653589793)
    is_shot = pl.col("event").is_in(["shot", "blocked_shot", "goal"])
    return pbp.with_columns(
        shot_distance=pl.when(is_shot).then(dist).otherwise(None),
        shot_angle=pl.when(is_shot).then(angle).otherwise(None),
    )


def scoring_chances(pbp: pl.DataFrame, threshold_ft: float = _SCORING_CHANCE_FT) -> pl.DataFrame:
    """Flag ``scoring_chance`` for shot-type events within ``threshold_ft`` of net."""
    if "shot_distance" not in pbp.columns:
        pbp = add_shot_distance_angle(pbp)
    return pbp.with_columns(
        scoring_chance=(pl.col("shot_distance").is_not_null() & (pl.col("shot_distance") <= threshold_ft))
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_analytics.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_analytics.py tests/hockeytech/test_analytics.py
git commit -m "feat(hockeytech): shot distance/angle + scoring-chance flags"
```

---

### Task A2.2: TOI from shifts

**Files:**
- Modify: `sportsdataverse/hockeytech/_analytics.py`
- Test: `tests/hockeytech/test_analytics.py`

- [ ] **Step 1: Add failing test**

```python
# append to tests/hockeytech/test_analytics.py
def test_player_toi_sums_shift_lengths():
    from sportsdataverse.hockeytech._analytics import player_toi

    shifts = pl.DataFrame({
        "player_id": [1, 1, 2],
        "first_name": ["A", "A", "B"], "last_name": ["X", "X", "Y"],
        "period": [1, 1, 1],
        "start_s": [1200, 1100, 1200], "end_s": [1180, 1090, 1150],
    })
    out = player_toi(shifts)
    assert "toi_seconds" in out.columns and "num_shifts" in out.columns
    a = out.filter(pl.col("player_id") == 1)
    assert a["toi_seconds"][0] == 30   # (1200-1180) + (1100-1090)
    assert a["num_shifts"][0] == 2
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_analytics.py::test_player_toi_sums_shift_lengths -q`
Expected: FAIL — no `player_toi`.

- [ ] **Step 3: Implement `player_toi`**

```python
# append to sportsdataverse/hockeytech/_analytics.py

def player_toi(shifts: pl.DataFrame) -> pl.DataFrame:
    """Per-player TOI from a parsed shifts frame (countdown clock => start_s >= end_s)."""
    if shifts.height == 0:
        return pl.DataFrame(schema={"player_id": pl.Int64, "toi_seconds": pl.Int64,
                                    "num_shifts": pl.Int64, "avg_shift_s": pl.Float64})
    per_shift = shifts.with_columns(shift_s=(pl.col("start_s") - pl.col("end_s")))
    return (
        per_shift.group_by("player_id", "first_name", "last_name")
        .agg(
            toi_seconds=pl.col("shift_s").sum(),
            num_shifts=pl.len(),
            avg_shift_s=pl.col("shift_s").mean(),
        )
        .sort("toi_seconds", descending=True)
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_analytics.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_analytics.py tests/hockeytech/test_analytics.py
git commit -m "feat(hockeytech): per-player TOI from shifts"
```

---

### Task A2.3: On-ice reconstruction (countdown-clock interval match)

**Files:**
- Modify: `sportsdataverse/hockeytech/_analytics.py`
- Test: `tests/hockeytech/test_analytics.py`

- [ ] **Step 1: Add failing test**

```python
# append to tests/hockeytech/test_analytics.py
def test_build_on_ice_matches_interval_on_countdown_clock():
    from sportsdataverse.hockeytech._analytics import build_on_ice

    # period 1; event at t=1190 (countdown). Player 1 shift [1200..1180] covers it; player 2 [1100..1090] does not.
    pbp = pl.DataFrame({"event": ["shot"], "period_of_game": [1], "time_s": [1190], "team_id": [10]})
    shifts = pl.DataFrame({
        "player_id": [1, 2], "home": [1, 1], "period": [1, 1],
        "start_s": [1200, 1100], "end_s": [1180, 1090],
    })
    out = build_on_ice(pbp, shifts)
    assert "on_ice_home" in out.columns
    assert out["on_ice_home"][0] == "1"   # only player 1 on ice
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_analytics.py::test_build_on_ice_matches_interval_on_countdown_clock -q`
Expected: FAIL — no `build_on_ice`.

- [ ] **Step 3: Implement `build_on_ice`**

```python
# append to sportsdataverse/hockeytech/_analytics.py

def build_on_ice(pbp: pl.DataFrame, shifts: pl.DataFrame) -> pl.DataFrame:
    """Attach ``on_ice_home``/``on_ice_away`` (comma-joined player_ids) per event.

    Requires ``pbp`` to carry ``period_of_game`` and ``time_s`` (seconds remaining,
    countdown). A player is on ice iff a shift in that period has
    ``start_s >= time_s >= end_s``. ``shifts`` carries ``home`` (1/0).
    """
    if pbp.height == 0 or shifts.height == 0:
        return pbp.with_columns(on_ice_home=pl.lit(None), on_ice_away=pl.lit(None))

    pbp = pbp.with_row_index("_eidx")
    joined = pbp.join(shifts, left_on="period_of_game", right_on="period, how="inner")  # noqa
    on = joined.filter((pl.col("start_s") >= pl.col("time_s")) & (pl.col("time_s") >= pl.col("end_s")))
    agg = (
        on.group_by("_eidx", "home")
        .agg(ids=pl.col("player_id").cast(pl.Utf8).unique().sort().str.concat(","))
        .pivot(values="ids", index="_eidx", on="home")
    )
    rename = {c: ("on_ice_home" if c == "1" else "on_ice_away") for c in agg.columns if c != "_eidx"}
    agg = agg.rename(rename)
    return pbp.join(agg, on="_eidx", how="left").drop("_eidx")
```

> Implementer note: the `right_on="period` line above has an intentional bug
> marker — write it as `right_on="period"`. Confirm the pivot column dtypes
> (`home` may be Int → cast to Utf8 before pivot if needed).

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_analytics.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_analytics.py tests/hockeytech/test_analytics.py
git commit -m "feat(hockeytech): on-ice reconstruction via countdown-clock interval match"
```

---

### Task A2.4: Corsi/Fenwick (team + player) with per-60

**Files:**
- Modify: `sportsdataverse/hockeytech/_analytics.py`
- Test: `tests/hockeytech/test_analytics.py`

- [ ] **Step 1: Add failing test**

```python
# append to tests/hockeytech/test_analytics.py
def test_corsi_fenwick_team_counts_and_flag():
    from sportsdataverse.hockeytech._analytics import corsi_fenwick

    pbp = pl.DataFrame({
        "event": ["shot", "blocked_shot", "goal", "faceoff"],
        "team_id": [10, 10, 20, 10],
    })
    team = corsi_fenwick(pbp)
    assert team.attrs.get("corsi_includes_missed") is False or "corsi_includes_missed" in team.columns
    t10 = team.filter(pl.col("team_id") == 10)
    # team 10: 1 shot + 1 blocked + 0 goal = CF 2 (faceoff ignored)
    assert t10["corsi_for"][0] == 2
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_analytics.py::test_corsi_fenwick_team_counts_and_flag -q`
Expected: FAIL — no `corsi_fenwick`.

- [ ] **Step 3: Implement `corsi_fenwick` (+ `per60`)**

```python
# append to sportsdataverse/hockeytech/_analytics.py

_CORSI_EVENTS = ["shot", "blocked_shot", "goal"]
_FENWICK_EVENTS = ["shot", "goal"]


def corsi_fenwick(pbp: pl.DataFrame) -> pl.DataFrame:
    """Team-level shot-attempt counts. Corsi = shot+blocked+goal; Fenwick excludes
    blocked. Missed shots unavailable => proxies (``corsi_includes_missed`` column).
    """
    teams = [t for t in pbp.get_column("team_id").unique().to_list() if t is not None] if pbp.height else []
    rows = []
    for t in teams:
        cf = pbp.filter(pl.col("event").is_in(_CORSI_EVENTS) & (pl.col("team_id") == t)).height
        ca = pbp.filter(pl.col("event").is_in(_CORSI_EVENTS) & (pl.col("team_id") != t)
                        & pl.col("team_id").is_not_null()).height
        ff = pbp.filter(pl.col("event").is_in(_FENWICK_EVENTS) & (pl.col("team_id") == t)).height
        fa = pbp.filter(pl.col("event").is_in(_FENWICK_EVENTS) & (pl.col("team_id") != t)
                        & pl.col("team_id").is_not_null()).height
        rows.append({
            "team_id": t, "corsi_for": cf, "corsi_against": ca,
            "corsi_for_pct": (cf / (cf + ca)) if (cf + ca) else None,
            "fenwick_for": ff, "fenwick_against": fa,
            "fenwick_for_pct": (ff / (ff + fa)) if (ff + fa) else None,
            "corsi_includes_missed": False,
        })
    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={"team_id": pl.Int64, "corsi_for": pl.Int64, "corsi_against": pl.Int64,
                "corsi_for_pct": pl.Float64, "fenwick_for": pl.Int64, "fenwick_against": pl.Int64,
                "fenwick_for_pct": pl.Float64, "corsi_includes_missed": pl.Boolean})


def per60(value_col: str, toi_seconds_col: str = "toi_seconds") -> pl.Expr:
    """Per-60 rate expression: value / toi_seconds * 3600."""
    return (pl.col(value_col) / pl.col(toi_seconds_col) * 3600).alias(f"{value_col}_per60")
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_analytics.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_analytics.py tests/hockeytech/test_analytics.py
git commit -m "feat(hockeytech): team Corsi/Fenwick proxies + per-60 (missed-shot flag)"
```

---

### Task A2.5: PWHL analytics public functions + enriched `pwhl_pbp`

**Files:**
- Create: `sportsdataverse/pwhl/pwhl_analytics.py`
- Modify: `sportsdataverse/pwhl/pwhl_api.py` (enrich `pwhl_pbp`), `sportsdataverse/pwhl/__init__.py`
- Test: `tests/hockeytech/test_public_surface.py`

- [ ] **Step 1: Add failing test (offline, monkeypatched)**

```python
# append to tests/hockeytech/test_public_surface.py
def test_pwhl_analytics_surface_and_shifts(monkeypatch):
    import sportsdataverse.pwhl as pwhl
    import sportsdataverse.pwhl.pwhl_analytics as an

    for fn in ("pwhl_game_shifts", "pwhl_player_toi", "pwhl_game_corsi"):
        assert hasattr(pwhl, fn), f"missing {fn}"

    monkeypatch.setattr(an, "hockeytech_api",
                        lambda *a, **k: load_fixture("hockeytech", "pwhl_gameshifts_42"))
    sh = an.pwhl_game_shifts(game_id=42)
    assert sh.height > 0 and "start_s" in sh.columns
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_public_surface.py::test_pwhl_analytics_surface_and_shifts -q`
Expected: FAIL — analytics functions missing.

- [ ] **Step 3: Implement `pwhl_analytics.py`**

```python
# sportsdataverse/pwhl/pwhl_analytics.py
"""PWHL on-ice / Corsi / TOI analytics over the HockeyTech feeds.

Corsi/Fenwick are proxies (no missed-shot event in the feed). See the design doc.
"""

from __future__ import annotations

from typing import Any

from sportsdataverse.hockeytech import hockeytech_api
from sportsdataverse.hockeytech import _analytics as A
from sportsdataverse.hockeytech import _parsers as P

__all__ = ["pwhl_game_shifts", "pwhl_player_toi", "pwhl_game_corsi"]

_LG = "pwhl"


def pwhl_game_shifts(game_id: int, return_as_pandas: bool = False) -> Any:
    """All player shifts for a game — one row per stint (incl. countdown-clock seconds)."""
    payload = hockeytech_api(_LG, "modulekit", "gameshifts", {"game_id": game_id})
    return P.parse_shifts(payload, game_id=game_id, return_as_pandas=return_as_pandas)


def pwhl_player_toi(game_id: int, return_as_pandas: bool = False) -> Any:
    """Per-player TOI for a game from the shift tables."""
    shifts = pwhl_game_shifts(game_id)
    out = A.player_toi(shifts)
    return out.to_pandas() if return_as_pandas else out


def pwhl_game_corsi(game_id: int, return_as_pandas: bool = False) -> Any:
    """Team-level Corsi/Fenwick proxies for a game (corsi_includes_missed=False)."""
    from sportsdataverse.pwhl.pwhl_api import pwhl_pbp

    pbp = pwhl_pbp(game_id)
    out = A.corsi_fenwick(pbp)
    return out.to_pandas() if return_as_pandas else out
```

- [ ] **Step 4: Enrich `pwhl_pbp` with geometry + scoring chance**

```python
# in sportsdataverse/pwhl/pwhl_api.py, replace the pwhl_pbp body's return with:
def pwhl_pbp(game_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL play-by-play — superset of fastRhockey ``pwhl_pbp`` (adds shot_distance,
    shot_angle, scoring_chance)."""
    from sportsdataverse.hockeytech import _analytics as A

    payload = hockeytech_api(_LG, "statviewfeed", "gameCenterPlayByPlay",
                             {"game_id": game_id, "league_id": ""})
    df = P.parse_pbp(payload, pbp_style="hockeytech_a", game_id=game_id)
    df = A.scoring_chances(A.add_shot_distance_angle(df))
    return df.to_pandas() if return_as_pandas else df
```

- [ ] **Step 5: Wire into `__init__.py`**

```python
# sportsdataverse/pwhl/__init__.py — add
from sportsdataverse.pwhl.pwhl_analytics import *  # noqa: F401,F403
```

- [ ] **Step 6: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_public_surface.py -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add sportsdataverse/pwhl/pwhl_analytics.py sportsdataverse/pwhl/pwhl_api.py sportsdataverse/pwhl/__init__.py tests/hockeytech/test_public_surface.py
git commit -m "feat(pwhl): on-ice/TOI/Corsi analytics + enriched pwhl_pbp"
```

---

## Phase A3 — AHL / OHL / WHL / QMJHL families

### Task A3.1: PBP dialect b parser

**Files:**
- Modify: `sportsdataverse/hockeytech/_parsers.py`
- Test: `tests/hockeytech/test_parsers.py`

- [ ] **Step 1: Capture a junior fixture, then add a failing test**

Add to `tests/fixtures/hockeytech/_capture.py` a completed-game capture for one junior league (find a `game_id` via `hockeytech_api("ohl","modulekit","scorebar",{...})`), e.g. `ohl_pbp_<id>`. Re-run the capture.

```python
# append to tests/hockeytech/test_parsers.py
def test_parse_pbp_b_dialect_one_row_per_event():
    import glob, os, polars as pl
    from sportsdataverse.hockeytech._parsers import parse_pbp
    # use whichever junior pbp fixture exists
    stems = [os.path.basename(p)[:-5] for p in
             glob.glob("tests/fixtures/hockeytech/*_pbp_*.json")]
    juniors = [s for s in stems if s.split("_")[0] in ("ohl", "whl", "qmjhl", "ahl")]
    assert juniors, "capture at least one junior pbp fixture"
    df = parse_pbp(_load(juniors[0]), pbp_style="hockeytech_b", game_id=1)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    for col in ("game_id", "event", "period_of_game", "player_id"):
        assert col in df.columns
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_parsers.py::test_parse_pbp_b_dialect_one_row_per_event -q`
Expected: FAIL — dialect-b branch not handled (events parse to mostly-null rows or differ).

- [ ] **Step 3: Extend `parse_pbp` for dialect b**

> Implementer reference: dialect b (OHL/WHL/QMJHL, the `pxpverbose`-style flat
> keys observed in the probe — `home_player_id`, `player_id`, `goal_player_id`,
> `goal_scorer`, `blocker_player_id`, `x_location`/`y_location`) differs from the
> nested dialect-a `details{}` shape. Branch on `pbp_style`: for `hockeytech_b`,
> read the flat keys; normalize to the same row schema (`event`, `player_id`,
> `period_of_game`, `time_of_period`, `x_coord`, `y_coord`, `team_id`, `goal`...).

```python
# in parse_pbp, after computing `events`, branch:
#   if pbp_style == "hockeytech_b": rows = _parse_pbp_b(events, game_id)
#   else: rows = _parse_pbp_a(events, game_id)   # the existing logic, extracted
# Implement _parse_pbp_b mapping the flat keys to the shared row schema.
```

Refactor the existing dialect-a body into `_parse_pbp_a(events, game_id)` and add `_parse_pbp_b(events, game_id)` per the fixture's actual keys, both returning `List[Dict]`; `parse_pbp` dispatches and calls `_to_frame`.

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_parsers.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/hockeytech/_parsers.py tests/hockeytech/test_parsers.py tests/fixtures/hockeytech/*.json tests/fixtures/hockeytech/_capture.py
git commit -m "feat(hockeytech): play-by-play dialect b (OHL/WHL/QMJHL)"
```

---

### Task A3.2: League family factory + the four packages

**Files:**
- Create: `sportsdataverse/hockeytech/_family.py`
- Create: `sportsdataverse/ahl/__init__.py`, `sportsdataverse/ahl/ahl_api.py`, `sportsdataverse/ahl/ahl_analytics.py` (and `ohl/`, `whl/`, `qmjhl/`)
- Modify: `sportsdataverse/__init__.py`
- Test: `tests/hockeytech/test_public_surface.py`

- [ ] **Step 1: Add failing test**

```python
# append to tests/hockeytech/test_public_surface.py
import pytest


@pytest.mark.parametrize("lg", ["ahl", "ohl", "whl", "qmjhl"])
def test_junior_family_core_surface(lg):
    mod = __import__(f"sportsdataverse.{lg}", fromlist=["*"])
    for stem in ("schedule", "pbp", "standings", "teams", "team_roster",
                 "player_stats", "leaders", "game_summary", "season_id"):
        assert hasattr(mod, f"{lg}_{stem}"), f"missing {lg}_{stem}"
    assert hasattr(mod, f"most_recent_{lg}_season")
    for stem in ("game_shifts", "player_toi", "game_corsi"):
        assert hasattr(mod, f"{lg}_{stem}"), f"missing analytics {lg}_{stem}"
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/hockeytech/test_public_surface.py -k junior_family -q`
Expected: FAIL — packages don't exist.

- [ ] **Step 3: Implement the family factory**

```python
# sportsdataverse/hockeytech/_family.py
"""Build a league's core + analytics function set from the shared core.

Used by the junior/AHL public modules to avoid copy-pasting 12 near-identical
wrappers per league. PWHL keeps its hand-written ``pwhl_api.py`` for full parity.
"""

from __future__ import annotations

from typing import Any, Callable, Dict

from sportsdataverse.hockeytech import hockeytech_api, resolve_season_id
from sportsdataverse.hockeytech import _analytics as A
from sportsdataverse.hockeytech import _parsers as P
from sportsdataverse.hockeytech._leagues import get_config


def build_family(league: str) -> Dict[str, Callable]:
    """Return {name: fn} for a junior/AHL league's core + analytics surface."""
    cfg = get_config(league)

    def season_id(return_as_pandas: bool = False) -> Any:
        return P.parse_seasons(hockeytech_api(league, "modulekit", "seasons", {}), return_as_pandas)

    def most_recent_season() -> int:
        df = season_id()
        return int(df["season_yr"].max()) if df.height else 0

    def schedule(season=None, season_id_=None, return_as_pandas: bool = False) -> Any:
        params = {"numberofdaysback": 10000, "numberofdaysahead": 10000, "limit": 10000,
                  "league_id": cfg.league_id}
        if season is not None or season_id_ is not None:
            params["season_id"] = resolve_season_id(league, season=season, season_id=season_id_)
        return P.parse_schedule(hockeytech_api(league, "modulekit", "scorebar", params), return_as_pandas)

    def pbp(game_id: int, return_as_pandas: bool = False) -> Any:
        payload = hockeytech_api(league, "statviewfeed", "gameCenterPlayByPlay",
                                 {"game_id": game_id, "league_id": ""})
        df = P.parse_pbp(payload, pbp_style=cfg.pbp_style, game_id=game_id)
        df = A.scoring_chances(A.add_shot_distance_angle(df))
        return df.to_pandas() if return_as_pandas else df

    def standings(season=None, season_id_=None, return_as_pandas: bool = False) -> Any:
        sid = resolve_season_id(league, season=season or most_recent_season(), season_id=season_id_)
        payload = hockeytech_api(league, "statviewfeed", "teams",
                                 {"groupTeamsBy": "division", "context": "overall",
                                  "special": "false", "league_id": cfg.league_id, "sort": "points",
                                  "season": sid})
        return P.parse_standings(payload, return_as_pandas)

    def teams(season=None, season_id_=None, return_as_pandas: bool = False) -> Any:
        sid = resolve_season_id(league, season=season or most_recent_season(), season_id=season_id_)
        return P.parse_teams(hockeytech_api(league, "modulekit", "teamsbyseason", {"season": sid}),
                             return_as_pandas)

    def team_roster(team_id: int, season=None, season_id_=None, return_as_pandas: bool = False) -> Any:
        sid = resolve_season_id(league, season=season or most_recent_season(), season_id=season_id_)
        return P.parse_roster(hockeytech_api(league, "modulekit", "roster",
                              {"team_id": team_id, "season_id": sid}), return_as_pandas)

    def player_stats(player_id: int, return_as_pandas: bool = False) -> Any:
        return P.parse_player_stats(hockeytech_api(league, "modulekit", "player",
                                    {"player_id": player_id, "category": "seasonstats"}), return_as_pandas)

    def leaders(season=None, season_id_=None, return_as_pandas: bool = False) -> Any:
        sid = resolve_season_id(league, season=season or most_recent_season(), season_id=season_id_)
        return P.parse_leaders(hockeytech_api(league, "statviewfeed", "leadersExtended",
                               {"season": sid, "team_id": 0, "playerTypes": "skaters",
                                "skaterStatTypes": "points,goals", "activeOnly": 0}), return_as_pandas)

    def game_summary(game_id: int) -> dict:
        return P.parse_game_summary(hockeytech_api(league, "gc", "gamesummary", {"game_id": game_id}),
                                    game_id=game_id)

    def game_shifts(game_id: int, return_as_pandas: bool = False) -> Any:
        return P.parse_shifts(hockeytech_api(league, "modulekit", "gameshifts", {"game_id": game_id}),
                              game_id=game_id, return_as_pandas=return_as_pandas)

    def player_toi(game_id: int, return_as_pandas: bool = False) -> Any:
        out = A.player_toi(game_shifts(game_id))
        return out.to_pandas() if return_as_pandas else out

    def game_corsi(game_id: int, return_as_pandas: bool = False) -> Any:
        out = A.corsi_fenwick(pbp(game_id))
        return out.to_pandas() if return_as_pandas else out

    return {
        f"{league}_season_id": season_id,
        f"most_recent_{league}_season": most_recent_season,
        f"{league}_schedule": schedule,
        f"{league}_pbp": pbp,
        f"{league}_standings": standings,
        f"{league}_teams": teams,
        f"{league}_team_roster": team_roster,
        f"{league}_player_stats": player_stats,
        f"{league}_leaders": leaders,
        f"{league}_game_summary": game_summary,
        f"{league}_game_shifts": game_shifts,
        f"{league}_player_toi": player_toi,
        f"{league}_game_corsi": game_corsi,
    }
```

- [ ] **Step 4: Create the four league packages (identical except the code)**

```python
# sportsdataverse/ahl/__init__.py
"""sportsdataverse.ahl -- live AHL HockeyTech wrappers (core set + analytics)."""

from __future__ import annotations

from sportsdataverse.hockeytech._family import build_family

_family = build_family("ahl")
globals().update(_family)
__all__ = list(_family)
```

Repeat verbatim for `sportsdataverse/ohl/__init__.py` (`build_family("ohl")`),
`sportsdataverse/whl/__init__.py` (`build_family("whl")`), and
`sportsdataverse/qmjhl/__init__.py` (`build_family("qmjhl")`).

- [ ] **Step 5: Wire into the top-level package**

```python
# sportsdataverse/__init__.py — add after the existing league wildcard imports
from sportsdataverse.ahl import *  # noqa: F401,F403,E402
from sportsdataverse.ohl import *  # noqa: F401,F403,E402
from sportsdataverse.qmjhl import *  # noqa: F401,F403,E402
from sportsdataverse.whl import *  # noqa: F401,F403,E402
```

- [ ] **Step 6: Run to verify it passes**

Run: `python -m pytest tests/hockeytech/test_public_surface.py -q`
Expected: PASS (all four families expose 9 core + 3 analytics + season helpers).

- [ ] **Step 7: Commit**

```bash
git add sportsdataverse/hockeytech/_family.py sportsdataverse/ahl sportsdataverse/ohl sportsdataverse/whl sportsdataverse/qmjhl sportsdataverse/__init__.py tests/hockeytech/test_public_surface.py
git commit -m "feat(hockeytech): AHL/OHL/WHL/QMJHL families via shared core factory"
```

---

### Task A3.3: Live smoke tests (env-gated)

**Files:**
- Modify: `tests/conftest.py`
- Create: `tests/hockeytech/test_live.py`

- [ ] **Step 1: Add the skip helper + a live test**

```python
# tests/conftest.py — add
import os
import pytest


def skip_unless_hockeytech():
    if os.environ.get("HOCKEYTECH_TESTS") != "1":
        pytest.skip("set HOCKEYTECH_TESTS=1 to run live HockeyTech tests")
```

```python
# tests/hockeytech/test_live.py
from __future__ import annotations

import polars as pl
import pytest

from tests.conftest import skip_unless_hockeytech


def test_pwhl_schedule_live_has_games():
    skip_unless_hockeytech()
    from sportsdataverse.pwhl import pwhl_schedule

    df = pwhl_schedule(season=2025)
    if df.height == 0:
        pytest.skip("no rows at test time")
    for col in ("game_id", "home_team", "away_team"):
        assert col in df.columns


@pytest.mark.parametrize("lg", ["ahl", "ohl", "whl", "qmjhl"])
def test_junior_schedule_live(lg):
    skip_unless_hockeytech()
    mod = __import__(f"sportsdataverse.{lg}", fromlist=["*"])
    df = getattr(mod, f"{lg}_schedule")()
    assert isinstance(df, pl.DataFrame)
```

- [ ] **Step 2: Run gated off (should skip) then on (should pass)**

Run: `python -m pytest tests/hockeytech/test_live.py -q` → all skipped.
Run: `HOCKEYTECH_TESTS=1 python -m pytest tests/hockeytech/test_live.py -q` → PASS (or graceful skip on empty).

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py tests/hockeytech/test_live.py
git commit -m "test(hockeytech): env-gated live smoke tests for all five leagues"
```

---

## Phase A4 — Docs, codegen, notebook, full gate

### Task A4.1: Autodoc example args + return schemas

**Files:**
- Modify: `tools/codegen/autodoc_example_args.yaml`
- Create: `tools/codegen/schemas/autodoc/{pwhl,ahl,ohl,whl,qmjhl}/*.yaml` (generated)

- [ ] **Step 1: Add example args**

```yaml
# tools/codegen/autodoc_example_args.yaml — append
pwhl:
  pwhl_schedule: {season: 2025}
  pwhl_pbp: {game_id: 42}
  pwhl_standings: {season: 2025}
  pwhl_teams: {season: 2025}
  pwhl_team_roster: {team_id: 1, season: 2025}
  pwhl_player_stats: {player_id: 27}
  pwhl_leaders: {season: 2025}
  pwhl_game_summary: {game_id: 42}
  pwhl_game_shifts: {game_id: 42}
  pwhl_player_toi: {game_id: 42}
  pwhl_game_corsi: {game_id: 42}
ahl:
  ahl_schedule: {}
  ahl_pbp: {game_id: 1024183}
ohl:
  ohl_schedule: {}
whl:
  whl_schedule: {}
qmjhl:
  qmjhl_schedule: {}
```

- [ ] **Step 2: Generate return schemas (network)**

Run: `HOCKEYTECH_TESTS=1 python tools/codegen/generate.py --autodoc-schemas`
Expected: writes `tools/codegen/schemas/autodoc/pwhl/*.yaml` etc.; prints `wrote pwhl_*`.

- [ ] **Step 3: Commit**

```bash
git add tools/codegen/autodoc_example_args.yaml tools/codegen/schemas/autodoc
git commit -m "docs(hockeytech): autodoc example args + return schemas"
```

---

### Task A4.2: Notebook + R-parity entries + full gate

**Files:**
- Modify: `examples/notebooks/10_pwhl.ipynb` (extend with live + analytics)
- Modify: codegen R-parity inputs (`tools/codegen/r_parity_aliases.yaml`)

- [ ] **Step 1: Add R-parity aliases for the new live functions**

```yaml
# tools/codegen/r_parity_aliases.yaml — append (py_fn: r_fn)
pwhl:
  pwhl_schedule: pwhl_schedule
  pwhl_pbp: pwhl_pbp
  pwhl_standings: pwhl_standings
  pwhl_teams: pwhl_teams
  pwhl_team_roster: pwhl_team_roster
  pwhl_player_stats: pwhl_player_stats
  pwhl_leaders: pwhl_leaders
  pwhl_game_summary: pwhl_game_summary
  pwhl_season_id: pwhl_season_id
  pwhl_game_shifts: pwhl_game_shifts
  pwhl_player_toi: pwhl_player_toi
  pwhl_game_corsi: pwhl_game_corsi
```

- [ ] **Step 2: Extend the PWHL notebook**

Add cells showing `pwhl_schedule(season=2025)`, `pwhl_pbp(game_id=...)`,
`pwhl_game_shifts(...)`, `pwhl_player_toi(...)`, `pwhl_game_corsi(...)`, and one
junior-league example. Keep outputs small.

- [ ] **Step 3: Run the full offline suite + codegen drift gate**

Run: `python -m pytest tests/hockeytech -q`
Expected: PASS.
Run: `python tools/codegen/generate.py --check`
Expected: "all generated files current" (regenerate with `python tools/codegen/generate.py` if it reports drift, then re-stage).

- [ ] **Step 4: Commit**

```bash
git add tools/codegen/r_parity_aliases.yaml examples/notebooks/10_pwhl.ipynb sportsdataverse/parsed
git commit -m "docs(hockeytech): R-parity entries + PWHL notebook live/analytics cells"
```

---

## Self-Review (against the design doc)

- **PWHL parity (19):** Tasks A1.4–A1.9 build all 19 + `most_recent_pwhl_season`. ✓
- **Junior core set (9) × 4 leagues:** Task A3.2 factory + packages. ✓
- **Analytics (shifts/TOI/Corsi/on-ice/scoring-chances + enriched pbp):** A2.1–A2.5 (PWHL) + A3.2 (juniors). ✓
- **snake_case rule:** `_snake_columns` in `_to_frame`, applied by every parser. ✓
- **Missed-shot caveat:** `corsi_includes_missed=False` in `corsi_fenwick` (A2.4). ✓
- **Two PBP dialects:** A1.6 (a), A3.1 (b). ✓
- **Season resolution (end-year + fallback):** A1.4. ✓
- **Error handling (empty frame, no raise):** `_to_frame` returns zero-row frames; `hockeytech_api` returns None on failure (parsers tolerate None). ✓
- **Testing (offline fixtures + env-gated live):** A1.3 fixtures, A3.3 live. ✓
- **Docs/codegen/notebook:** A4.1–A4.2. ✓

**Known follow-ups flagged for the implementer (not gaps in coverage):** the exact nesting of `standings`/`leaders`/`game_summary`/`player_stats` payloads must be confirmed against captured fixtures (the parser accessors are written to the observed shapes but the implementer adjusts accessors, not the asserted column contracts, if a fixture nests differently); the dialect-b key map (A3.1) is filled from the captured junior fixture.
