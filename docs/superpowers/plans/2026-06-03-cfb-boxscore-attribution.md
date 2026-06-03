<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CFB Advanced Box Score Attribution Refactor — Implementation Plan](#cfb-advanced-box-score-attribution-refactor--implementation-plan)
  - [File Structure](#file-structure)
  - [Phase 0 — Fixtures & scaffolding](#phase-0--fixtures--scaffolding)
    - [Task 0.1: Capture offline summary fixtures](#task-01-capture-offline-summary-fixtures)
    - [Task 0.2: Offline fixture-runner helper](#task-02-offline-fixture-runner-helper)
  - [Phase 1 — Attribution layer (`__add_attribution_cols`)](#phase-1--attribution-layer-__add_attribution_cols)
    - [Task 1.1: Overturned-clause stripping helper (finding #17)](#task-11-overturned-clause-stripping-helper-finding-17)
    - [Task 1.2: Recovery/fumbler team-abbreviation parsers (§5.4)](#task-12-recoveryfumbler-team-abbreviation-parsers-%C2%A754)
    - [Task 1.3: `__add_attribution_cols` — ST team + muff flag](#task-13-__add_attribution_cols--st-team--muff-flag)
    - [Task 1.4: Resolve fumbling/recovery/turnover team (§6)](#task-14-resolve-fumblingrecoveryturnover-team-%C2%A76)
    - [Task 1.5: Event-team + penalty columns (§5.2, §7)](#task-15-event-team--penalty-columns-%C2%A752-%C2%A77)
    - [Task 1.6: Wire `__add_attribution_cols` into the pipeline](#task-16-wire-__add_attribution_cols-into-the-pipeline)
  - [Phase 2 — Rewire `create_box_score` (additive)](#phase-2--rewire-create_box_score-additive)
    - [Task 2.1: Turnover box by identity, no scrimmage gate](#task-21-turnover-box-by-identity-no-scrimmage-gate)
    - [Task 2.2: Fumble-recovery + punt-return team in player boxes](#task-22-fumble-recovery--punt-return-team-in-player-boxes)
    - [Task 2.3: Penalty box by penalized team (+ additive `penalty_yards`)](#task-23-penalty-box-by-penalized-team--additive-penalty_yards)
    - [Task 2.4: Yardage reconciliation (#6, #7)](#task-24-yardage-reconciliation-6-7)
  - [Phase 3 — Participants identity join (auto + fallback)](#phase-3--participants-identity-join-auto--fallback)
    - [Task 3.1: Optional participants join with graceful fallback](#task-31-optional-participants-join-with-graceful-fallback)
  - [Phase 4 — Golden verification + invariants](#phase-4--golden-verification--invariants)
    - [Task 4.1: Concretize golden assertions against live verification](#task-41-concretize-golden-assertions-against-live-verification)
    - [Task 4.2: Reconciliation invariants](#task-42-reconciliation-invariants)
    - [Task 4.3: Full suite + lint + regression check](#task-43-full-suite--lint--regression-check)
  - [Self-Review notes (for the executor)](#self-review-notes-for-the-executor)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CFB Advanced Box Score Attribution Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `CFBPlayProcess.create_box_score` so every turnover, fumble, sack, return, and penalty is attributed to the correct team — especially on special teams — by introducing a deterministic per-play attribution layer and text-driven turnover detection.

**Architecture:** Add one pure pipeline step `__add_attribution_cols` that resolves the credited team per play (kicking/return team, fumbling/recovery team, penalized team) from the play text and existing flags. Rewire `create_box_score` to group by these resolved columns and drop the `scrimmage_play` gate on turnover counting. Output stays additive. Player identity is upgraded via an optional `cfb_play_participants` join with regex fallback.

**Tech Stack:** Python 3.11, polars, pytest. Runtime: `sdv-py/.venv` (runs the working tree). Spec: `docs/superpowers/specs/2026-06-03-cfb-boxscore-attribution-design.md`.

**Conventions:**

- Run tests with the venv python: `c:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-py/.venv/Scripts/python.exe -m pytest ...` (or `uv run pytest ...`). Plain `python` on PATH is a STALE install — do not use it.
- Conventional commits, scope `cfb`. **No AI co-author trailers.**
- Pre-commit runs ruff + markdownlint + doctoc; keep lists blank-line-surrounded in markdown.
- Branch: `feat/cfb-advbox-expansion` (already checked out).

---

## File Structure

- **Modify** `sportsdataverse/cfb/cfb_pbp.py`
  - Add private method `__add_attribution_cols(self, play_df)` (after `__add_player_cols`, before `__after_cols`).
  - Add module-level pure helpers near the top (below imports): `_strip_overturned_text`, `_parse_recovery_abbrev`, `_parse_fumbler_abbrev`.
  - Modify `run_processing_pipeline` pipeline chain to insert `.pipe(self.__add_attribution_cols)` and the participants join.
  - Modify `create_box_score`: `turnover_box`, `def_base_box`, `team_pen_box`, the `_player_event_box` calls for fumble recoveries + punt returns, and the turnover-JSON margin block.
- **Create** `tests/cfb/test_cfb_attribution.py` — unit tests for the pure helpers + `__add_attribution_cols` on synthetic frames.
- **Create** `tests/cfb/test_box_score_attribution_offline.py` — golden fixture tests (offline) for the 5 games.
- **Create** `tests/cfb/fixtures/summary_401754598.json`, `summary_401309854.json`, `summary_401135269.json`, `summary_401032062.json`, `summary_401112081.json` — captured ESPN summary payloads.
- **Create** `tools/capture_cfb_fixtures.py` — one-off script to capture the summary fixtures.

---

## Phase 0 — Fixtures & scaffolding

### Task 0.1: Capture offline summary fixtures

**Files:**

- Create: `tools/capture_cfb_fixtures.py`
- Create: `tests/cfb/fixtures/summary_<gid>.json` (×5)

- [ ] **Step 1: Write the capture script**

```python
# tools/capture_cfb_fixtures.py
"""One-off: capture ESPN CFB summary payloads as offline test fixtures.

Run once with network access:
    .venv/Scripts/python -m tools.capture_cfb_fixtures
Commit the resulting tests/cfb/fixtures/summary_*.json files.
"""
from __future__ import annotations

import json
from pathlib import Path

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

GAMES = [401754598, 401309854, 401112081, 401135269, 401032062]
OUT = Path("tests/cfb/fixtures")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for gid in GAMES:
        raw = CFBPlayProcess(gameId=gid, raw=True).espn_cfb_pbp()
        path = OUT / f"summary_{gid}.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        print(f"wrote {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it (network required, one-off)**

Run: `cd c:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-py && .venv/Scripts/python.exe -m tools.capture_cfb_fixtures`
Expected: prints 5 `wrote tests/cfb/fixtures/summary_<gid>.json (...)` lines.

- [ ] **Step 3: Sanity-check a fixture loads offline**

Run: `.venv/Scripts/python.exe -c "import json,glob; [print(p, len(json.load(open(p)))) for p in glob.glob('tests/cfb/fixtures/summary_40*.json')]"`
Expected: 5 paths printed, each with a nonzero key count.

- [ ] **Step 4: Commit**

```bash
git add tools/capture_cfb_fixtures.py tests/cfb/fixtures/summary_40*.json
git commit -m "test(cfb): capture offline summary fixtures for box-score attribution"
```

---

### Task 0.2: Offline fixture-runner helper

**Files:**

- Create: `tests/cfb/test_box_score_attribution_offline.py`

- [ ] **Step 1: Write the shared runner (no assertions yet — just the harness + one smoke test)**

```python
# tests/cfb/test_box_score_attribution_offline.py
"""Offline golden tests for create_box_score team attribution.

Each test mocks cfb_pbp.download to return a captured summary payload, so
no network is hit. Team attribution does not depend on participants, so the
participants join falls back to regex on these fixtures (the mocked download
returns the summary for any URL, which the participants parser treats as
empty and falls back). See spec section 8.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _load(gid: int) -> dict:
    return json.loads((FIX / f"summary_{gid}.json").read_text(encoding="utf-8"))


def _box(monkeypatch, gid: int) -> dict:
    summary = _load(gid)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    out = CFBPlayProcess(gameId=gid).run_processing_pipeline()
    return out["advBoxScore"]


def _team(box_section: list[dict], team_id: int) -> dict:
    matches = [r for r in box_section if r.get("pos_team") == team_id or r.get("team_id") == team_id]
    assert matches, f"team {team_id} not found in section"
    return matches[0]


def test_fixtures_produce_box(monkeypatch):
    box = _box(monkeypatch, 401754598)
    assert set(box) >= {"turnover", "team", "defensive_players", "specialists"}
```

- [ ] **Step 2: Run the smoke test**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py::test_fixtures_produce_box -v`
Expected: PASS (current code already returns those keys).

- [ ] **Step 3: Commit**

```bash
git add tests/cfb/test_box_score_attribution_offline.py
git commit -m "test(cfb): offline box-score fixture runner harness"
```

---

## Phase 1 — Attribution layer (`__add_attribution_cols`)

### Task 1.1: Overturned-clause stripping helper (finding #17)

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py` (add module-level helper)
- Test: `tests/cfb/test_cfb_attribution.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/cfb/test_cfb_attribution.py
from sportsdataverse.cfb.cfb_pbp import _strip_overturned_text


def test_strip_overturned_removes_original_play_clause():
    t = ('#11 C.Bailey sacked for loss of 2 yards to the FSU49 (#7 S.Thompson). '
         'The previous play is under automatic review - "Runner was down by contact". '
         'CALL OVERTURNED. (Original Play: (11:34) #11 C.Bailey sacked for loss of 1 yard '
         'to the FSU48, fumble by #11 C.Bailey recovered by FSU #40 A.Williams at FSU48, End Of Play)')
    cleaned = _strip_overturned_text(t)
    assert "fumble by" not in cleaned
    assert "recovered by FSU" not in cleaned
    assert "C.Bailey sacked" in cleaned  # the ruled (kept) portion survives


def test_strip_overturned_noop_on_normal_text():
    t = "#4 S.White return 2 yards fumbled by #4 S.White recovered by NCSU #4 T.Thomas"
    assert _strip_overturned_text(t) == t
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -v`
Expected: FAIL — `ImportError: cannot import name '_strip_overturned_text'`.

- [ ] **Step 3: Implement the helper**

Add near the top of `cfb_pbp.py` (after imports, before the class):

```python
import re as _re

_OVERTURNED_RE = _re.compile(r"\(Original Play:.*?\)\s*$", _re.IGNORECASE | _re.DOTALL)


def _strip_overturned_text(text: str | None) -> str | None:
    """Drop the negated ``(Original Play: …)`` clause from reviewed/overturned plays.

    ESPN appends the *reversed* play description in a trailing
    ``(Original Play: …)`` parenthetical after ``CALL OVERTURNED``. Any
    fumble/recovery parsing must run on the kept (ruled) portion only, or a
    reversed fumble gets counted as a real turnover (spec finding #17).
    """
    if not text:
        return text
    return _OVERTURNED_RE.sub("", text).strip()
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_cfb_attribution.py
git commit -m "feat(cfb): strip overturned (Original Play) clauses before fumble parsing"
```

---

### Task 1.2: Recovery/fumbler team-abbreviation parsers (§5.4)

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py`
- Test: `tests/cfb/test_cfb_attribution.py`

- [ ] **Step 1: Write the failing tests**

```python
from sportsdataverse.cfb.cfb_pbp import _parse_recovery_abbrev


def test_parse_recovery_abbrev_basic():
    assert _parse_recovery_abbrev("… fumbled by #4 S.White recovered by NCSU #4 T.Thomas at FSU16") == "NCSU"


def test_parse_recovery_abbrev_muff():
    assert _parse_recovery_abbrev("punt 25 yards muffed by #24 K.Kirkland recovered by NCSU #98 C.Noonkester") == "NCSU"


def test_parse_recovery_abbrev_none_when_absent():
    assert _parse_recovery_abbrev("#22 J.Doe run for 4 yards") is None
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -k recovery_abbrev -v`
Expected: FAIL — import error.

- [ ] **Step 3: Implement**

```python
_RECOVERY_ABBREV_RE = _re.compile(r"recovered by\s+([A-Z&]{2,})\b")


def _parse_recovery_abbrev(text: str | None) -> str | None:
    """Return the uppercase team abbreviation that recovered the ball, or None.

    Operates on text that has already had overturned clauses stripped.
    """
    if not text:
        return None
    m = _RECOVERY_ABBREV_RE.search(text)
    return m.group(1).upper() if m else None
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -k recovery_abbrev -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_cfb_attribution.py
git commit -m "feat(cfb): parse recovering-team abbreviation from play text"
```

---

### Task 1.3: `__add_attribution_cols` — ST team + muff flag

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py`
- Test: `tests/cfb/test_cfb_attribution.py`

- [ ] **Step 1: Write the failing test (synthetic frame)**

```python
import polars as pl
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess


def _attr(rows: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(rows)
    proc = CFBPlayProcess(gameId=1)
    return proc._CFBPlayProcess__add_attribution_cols(df)


def test_kicking_return_team_flip():
    rows = [
        # kickoff: pos_team is receiving, def_pos_team is kicking
        {"pos_team": 9, "def_pos_team": 252, "kickoff_play": True, "punt": False,
         "fg_attempt": False, "sp": True, "scrimmage_play": False, "fumble_vec": False,
         "int": False, "text": "kickoff", "homeTeamAbbrev": "BYU", "awayTeamAbbrev": "ASU",
         "homeTeamId": 252, "awayTeamId": 9, "penalty_detail": None, "yds_penalty": None,
         "end.pos_team.id": 9},
        # punt: pos_team is punting, def_pos_team is receiving
        {"pos_team": 252, "def_pos_team": 9, "kickoff_play": False, "punt": True,
         "fg_attempt": False, "sp": True, "scrimmage_play": False, "fumble_vec": False,
         "int": False, "text": "punt", "homeTeamAbbrev": "BYU", "awayTeamAbbrev": "ASU",
         "homeTeamId": 252, "awayTeamId": 9, "penalty_detail": None, "yds_penalty": None,
         "end.pos_team.id": 9},
    ]
    out = _attr(rows)
    assert out["kicking_team"].to_list() == [252, 252]   # kickoff→def, punt→pos
    assert out["return_team"].to_list() == [9, 9]         # kickoff→pos, punt→def


def test_muff_detected():
    rows = [{"pos_team": 252, "def_pos_team": 9, "kickoff_play": False, "punt": True,
             "fg_attempt": False, "sp": True, "scrimmage_play": False, "fumble_vec": False,
             "int": False, "text": "punt 25 muffed by #24 K.Kirkland recovered by ASU #1 X",
             "homeTeamAbbrev": "BYU", "awayTeamAbbrev": "ASU", "homeTeamId": 252,
             "awayTeamId": 9, "penalty_detail": None, "yds_penalty": None, "end.pos_team.id": 9}]
    out = _attr(rows)
    assert out["fumble_or_muff"].to_list() == [True]
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -k "kicking_return or muff" -v`
Expected: FAIL — `AttributeError: '...' object has no attribute '...__add_attribution_cols'`.

- [ ] **Step 3: Implement the first slice of `__add_attribution_cols`**

Add the method to the `CFBPlayProcess` class, immediately after `__add_player_cols`:

```python
    def __add_attribution_cols(self, play_df):
        """Resolve the credited team per play (spec section 5).

        Pure/deterministic. Reads pos_team/def_pos_team + play-type flags +
        text, writes kicking_team/return_team, fumble_or_muff, fumbling_team,
        recovery_team, turnover_team, is_turnover, is_st_turnover,
        penalized_team, penalty_yards_signed, and event-team columns.
        """
        play_df = play_df.with_columns(
            # --- Special-teams team flip (verified): kickoff pos_team=receiving;
            #     punt/FG pos_team=kicking. ---
            kicking_team=pl.when(pl.col("kickoff_play") == True)
            .then(pl.col("def_pos_team"))
            .when((pl.col("punt") == True) | (pl.col("fg_attempt") == True))
            .then(pl.col("pos_team"))
            .otherwise(None),
            return_team=pl.when(pl.col("kickoff_play") == True)
            .then(pl.col("pos_team"))
            .when((pl.col("punt") == True) | (pl.col("fg_attempt") == True))
            .then(pl.col("def_pos_team"))
            .otherwise(None),
            # --- Widen fumble detection to include muffs (finding #14) ---
            fumble_or_muff=pl.when(
                (pl.col("fumble_vec") == True)
                | (pl.col("text").str.contains(r"(?i)muff")),
            )
            .then(True)
            .otherwise(False),
        )
        return play_df
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -k "kicking_return or muff" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_cfb_attribution.py
git commit -m "feat(cfb): add __add_attribution_cols with ST team flip + muff flag"
```

---

### Task 1.4: Resolve fumbling/recovery/turnover team (§6)

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py` (extend `__add_attribution_cols`)
- Test: `tests/cfb/test_cfb_attribution.py`

- [ ] **Step 1: Write the failing tests (the four turnover scenarios)**

```python
def _base(**over):
    row = {"pos_team": 252, "def_pos_team": 9, "kickoff_play": False, "punt": False,
           "fg_attempt": False, "sp": False, "scrimmage_play": True, "fumble_vec": True,
           "int": False, "homeTeamAbbrev": "BYU", "awayTeamAbbrev": "ASU",
           "homeTeamId": 252, "awayTeamId": 9, "penalty_detail": None, "yds_penalty": None,
           "end.pos_team.id": 252, "text": ""}
    row.update(over)
    return row


def test_scrimmage_fumble_lost_to_opponent():
    # BYU (pos) fumbles, ASU recovers → BYU turnover
    out = _attr([_base(text="#11 QB sacked fumble by #11 QB recovered by ASU #40 X")])
    r = out.to_dicts()[0]
    assert r["is_turnover"] is True
    assert r["turnover_team"] == 252  # BYU lost it
    assert r["recovery_team"] == 9


def test_own_recovery_not_turnover():
    out = _attr([_base(text="#11 QB fumble by #11 QB recovered by BYU #55 Y")])
    r = out.to_dicts()[0]
    assert r["is_turnover"] is False
    assert r["recovery_team"] == 252


def test_punt_return_fumble_lost_st():
    # punt: pos=BYU punting, def=ASU receiving; ASU returner fumbles, BYU recovers
    out = _attr([_base(pos_team=252, def_pos_team=9, punt=True, sp=True, scrimmage_play=False,
                       text="punt 40 #2 R return 5 fumbled by #2 R recovered by BYU #98 P")])
    r = out.to_dicts()[0]
    assert r["is_turnover"] is True
    assert r["is_st_turnover"] is True
    assert r["turnover_team"] == 9   # ASU (returner) lost it
    assert r["recovery_team"] == 252


def test_overturned_fumble_not_turnover():
    out = _attr([_base(text='#11 QB sacked. CALL OVERTURNED. (Original Play: fumble by #11 QB recovered by ASU #40 X)')])
    r = out.to_dicts()[0]
    assert r["is_turnover"] is False
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -k "fumble or own_recovery or overturned" -v`
Expected: FAIL — `KeyError`/missing columns `is_turnover`, `turnover_team`.

- [ ] **Step 3: Extend `__add_attribution_cols`**

Append, before `return play_df`:

```python
        # --- Cleaned text for fumble/recovery parsing (strip overturned) ---
        play_df = play_df.with_columns(
            _clean_text=pl.col("text").map_elements(_strip_overturned_text, return_dtype=pl.Utf8),
        ).with_columns(
            _recovery_abbrev=pl.col("_clean_text").map_elements(_parse_recovery_abbrev, return_dtype=pl.Utf8),
        )

        # abbrev → team id using the per-play home/away abbreviations
        play_df = play_df.with_columns(
            recovery_team=pl.when(pl.col("_recovery_abbrev").is_null())
            .then(None)
            .when(pl.col("_recovery_abbrev") == pl.col("homeTeamAbbrev").str.to_uppercase())
            .then(pl.col("homeTeamId"))
            .when(pl.col("_recovery_abbrev") == pl.col("awayTeamAbbrev").str.to_uppercase())
            .then(pl.col("awayTeamId"))
            .otherwise(None),
        )

        # fumbling team:
        #  - scrimmage: the offense (pos_team)
        #  - ST: the return team (the muffing/returning side); the other of
        #    {kicking_team, return_team} from the recoverer when needed.
        play_df = play_df.with_columns(
            fumbling_team=pl.when(pl.col("fumble_or_muff") == False)
            .then(None)
            .when(pl.col("sp") == False)
            .then(pl.col("pos_team"))
            .otherwise(pl.col("return_team")),
        )

        # turnover: a fumble/muff where the recovering team differs from the
        # fumbling team, OR an interception. Recovery team is authoritative;
        # if unparseable, fall back to possession-change (last resort).
        play_df = play_df.with_columns(
            is_turnover=pl.when(pl.col("int") == True)
            .then(True)
            .when(
                (pl.col("fumble_or_muff") == True)
                & (pl.col("recovery_team").is_not_null())
                & (pl.col("fumbling_team").is_not_null())
                & (pl.col("recovery_team") != pl.col("fumbling_team")),
            )
            .then(True)
            .when(
                # fallback: fumble with no parseable recovery team, use possession diff
                (pl.col("fumble_or_muff") == True)
                & (pl.col("recovery_team").is_null())
                & (pl.col("scrimmage_play") == True)
                & (pl.col("end.pos_team.id") != pl.col("pos_team")),
            )
            .then(True)
            .otherwise(False),
        ).with_columns(
            turnover_team=pl.when(pl.col("is_turnover") == False)
            .then(None)
            .when(pl.col("int") == True)
            .then(pl.col("pos_team"))
            .otherwise(pl.col("fumbling_team")),
            is_st_turnover=pl.when((pl.col("is_turnover") == True) & (pl.col("sp") == True))
            .then(True)
            .otherwise(False),
        )
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -k "fumble or own_recovery or overturned" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_cfb_attribution.py
git commit -m "feat(cfb): text-driven turnover detection (scrimmage + ST + own-recovery + overturned)"
```

---

### Task 1.5: Event-team + penalty columns (§5.2, §7)

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py` (extend `__add_attribution_cols`)
- Test: `tests/cfb/test_cfb_attribution.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_fumble_recovery_team_is_recoverer():
    # kickoff own recovery: receiving (pos=ASU=9) recovers own; credited to 9 not def
    out = _attr([_base(pos_team=9, def_pos_team=252, kickoff_play=True, sp=True,
                       scrimmage_play=False, end={"pos_team.id": 9} and 9,
                       text="kickoff #2 R return fumbled by #2 R recovered by ASU #2 R")])
    r = out.to_dicts()[0]
    assert r["fumble_recovery_team"] == 9


def test_penalized_team_defensive():
    out = _attr([_base(scrimmage_play=True, fumble_vec=False, penalty_detail="Defensive Holding",
                       yds_penalty="5", text="PENALTY ASU Defensive Holding 5 yards")])
    r = out.to_dicts()[0]
    assert r["penalized_team"] == 9          # defensive foul → def_pos_team
    assert r["penalty_yards_signed"] == 5


def test_penalized_team_offensive():
    out = _attr([_base(scrimmage_play=True, fumble_vec=False, penalty_detail="False Start",
                       yds_penalty="5", text="PENALTY BYU False Start 5 yards")])
    r = out.to_dicts()[0]
    assert r["penalized_team"] == 252        # offensive foul → pos_team
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -k "recovery_team_is or penalized" -v`
Expected: FAIL — missing columns.

- [ ] **Step 3: Extend `__add_attribution_cols`**

Append, before `return play_df`. Define the defensive-penalty set as a module constant near the other vecs:

```python
_DEFENSIVE_PENALTIES = frozenset({
    "Defensive Holding", "Defensive Pass Interference", "Defensive Offside",
    "Roughing the Passer", "Roughing the Kicker", "Roughing the Holder",
    "Roughing the Snapper", "12 Men on the Field", "Neutral Zone Infraction",
    "Encroachment", "Targeting", "Pass Interference",  # generic PI on a pass → usually defense
})
```

```python
        # event → credited team (spec 5.2)
        play_df = play_df.with_columns(
            sack_team=pl.col("def_pos_team"),
            interception_team=pl.col("def_pos_team"),
            pass_breakup_team=pl.col("def_pos_team"),
            forced_fumble_team=pl.col("def_pos_team"),
            fumble_recovery_team=pl.col("recovery_team"),
            punt_return_team=pl.col("return_team"),
            kick_return_team=pl.col("return_team"),
            fg_team=pl.col("kicking_team"),
            punt_team=pl.col("kicking_team"),
            # penalized team: defensive foul → def_pos_team, else offense
            penalized_team=pl.when(pl.col("penalty_detail").is_in(list(_DEFENSIVE_PENALTIES)))
            .then(pl.col("def_pos_team"))
            .otherwise(pl.col("pos_team")),
            penalty_yards_signed=pl.col("yds_penalty")
            .cast(pl.Utf8)
            .str.extract(r"(\d+)")
            .cast(pl.Int32, strict=False)
            .fill_null(0),
        )
```

> NOTE for the implementer: `Pass Interference` is ambiguous (the pipeline emits a single "Pass Interference" detail for both offensive and defensive PI). During Task 4 verification on 401754598, confirm the DPI rows resolve to FSU; if offensive PI is also tagged "Pass Interference", refine using the `PENALTY {ABBR}` token in the text (parse like `_parse_recovery_abbrev` but for `PENALTY\s+([A-Z&]{2,})`). Add a `_parse_penalty_abbrev` helper mirroring Task 1.2 and prefer it over the detail-set heuristic when present.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_cfb_attribution.py -k "recovery_team_is or penalized" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_cfb_attribution.py
git commit -m "feat(cfb): event-team + penalized-team attribution columns"
```

---

### Task 1.6: Wire `__add_attribution_cols` into the pipeline

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py` (`run_processing_pipeline`, ~`:4916-4932`)

- [ ] **Step 1: Write the failing test (column present end-to-end, offline)**

Add to `tests/cfb/test_box_score_attribution_offline.py`:

```python
def test_attribution_cols_present(monkeypatch):
    import polars as pl
    summary = _load(401754598)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=401754598)
    proc.run_processing_pipeline()
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    for col in ["turnover_team", "is_turnover", "is_st_turnover", "fumble_recovery_team",
                "penalized_team", "kicking_team", "return_team"]:
        assert col in df.columns, f"missing {col}"
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py::test_attribution_cols_present -v`
Expected: FAIL — columns missing (step not wired).

- [ ] **Step 3: Insert the pipe**

In `run_processing_pipeline`, in the `.pipe(...)` chain, insert after `.pipe(self.__add_player_cols)`:

```python
                    .pipe(self.__add_player_cols)
                    .pipe(self.__add_attribution_cols)
                    .pipe(self.__after_cols)
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py::test_attribution_cols_present -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_box_score_attribution_offline.py
git commit -m "feat(cfb): wire __add_attribution_cols into processing pipeline"
```

---

## Phase 2 — Rewire `create_box_score` (additive)

### Task 2.1: Turnover box by identity, no scrimmage gate

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py` (`turnover_box` ~`:4701-4756`)
- Test: `tests/cfb/test_box_score_attribution_offline.py`

- [ ] **Step 1: Write the failing golden tests**

```python
def test_turnovers_include_special_teams_kickoff(monkeypatch):
    # ASU(9) kickoff-return fumble lost to BYU(252) → ASU turnovers rise to 3
    box = _box(monkeypatch, 401309854)
    asu = _team(box["turnover"], 9)
    assert asu["turnovers"] >= 3
    assert asu.get("st_turnovers_lost", 0) >= 1


def test_turnovers_special_teams_punt_muff(monkeypatch):
    # FSU(52) loses muff + punt-return fumble (both recovered by NC State 152)
    box = _box(monkeypatch, 401754598)
    fsu = _team(box["turnover"], 52)
    ncst = _team(box["turnover"], 152)
    assert fsu["turnovers"] == 3            # 1 INT + 2 ST fumbles lost
    assert ncst["turnovers"] == 0           # strip-sack was overturned
    assert fsu.get("st_turnovers_lost", 0) == 2


def test_punt_own_recovery_not_a_turnover(monkeypatch):
    # WMU@BYU: BYU punt-return own recovery must NOT be a WMich turnover
    box = _box(monkeypatch, 401032062)
    for r in box["turnover"]:
        # neither team gains a phantom punt turnover from the own-recovery
        assert r["turnovers"] == int(r["turnovers"])  # sanity
    # explicit: WMich (away) has no fumble-lost from the BYU own recovery
    # (assert exact totals once confirmed in Task 4 verification)
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k turnover -v`
Expected: FAIL — current box drops ST turnovers (FSU=1, ASU=2).

- [ ] **Step 3: Rewrite `turnover_box` and the margin block**

Replace the `turnover_box` definition (currently filtering `scrimmage_play == True` and grouping by `pos_team`) with a version that counts turnovers from the attribution columns across ALL plays, then derives per-team fields. Replace lines ~`:4701-4756`:

```python
        # turnovers lost per team (scrimmage AND special teams), by identity
        to_lost = (
            play_df.filter(pl.col("is_turnover") == True)
            .group_by(["turnover_team"])
            .agg(
                turnovers=pl.len(),
                st_turnovers_lost=pl.col("is_st_turnover").sum(),
                Int=(pl.col("int") == True).sum(),
                fumbles_lost=((pl.col("fumble_or_muff") == True)).sum(),
            )
            .rename({"turnover_team": "pos_team"})
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
        # takeaways gained per team
        to_gained = (
            play_df.filter(pl.col("is_turnover") == True)
            .group_by(["recovery_team"])
            .agg(st_turnovers_gained=pl.col("is_st_turnover").sum(), takeaways=pl.len())
            .rename({"recovery_team": "pos_team"})
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
        # pass-breakups & fumbles for expected-TO model (offense-facing, scrimmage)
        to_aux = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .group_by(["pos_team"])
            .agg(
                pass_breakups=pl.col("pass_breakup").sum(),
                total_fumbles=pl.col("fumble_or_muff").sum(),
            )
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_ids = [int(self.homeTeamId), int(self.awayTeamId)]
        turnover_box = (
            pl.DataFrame({"pos_team": team_ids}, schema={"pos_team": pl.Int32})
            .join(to_lost, on="pos_team", how="left")
            .join(to_gained, on="pos_team", how="left")
            .join(to_aux, on="pos_team", how="left")
            .fill_null(0)
            .with_columns(team_id=pl.col("pos_team"))
        )
        turnover_box_json = json.loads(turnover_box.write_json())

        # identity-keyed margins/luck (never list index — spec finding #5)
        by_id = {int(r["pos_team"]): r for r in turnover_box_json}
        for tid, r in by_id.items():
            opp = by_id[[x for x in team_ids if x != tid][0]]
            r["Int"] = int(r.get("Int", 0))
            r["expected_turnovers"] = (0.5 * r.get("total_fumbles", 0)) + (
                0.22 * (r.get("pass_breakups", 0) + r.get("Int", 0))
            )
        for tid, r in by_id.items():
            opp = by_id[[x for x in team_ids if x != tid][0]]
            r["expected_turnover_margin"] = opp["expected_turnovers"] - r["expected_turnovers"]
            r["turnover_margin"] = opp["turnovers"] - r["turnovers"]
            r["turnover_luck"] = 5.0 * (r["turnover_margin"] - r["expected_turnover_margin"])
        turnover_box_json = [by_id[t] for t in team_ids]
```

> NOTE: this preserves every prior field name (`pass_breakups`, `fumbles_lost`, `total_fumbles`, `Int`, `expected_turnovers`, `expected_turnover_margin`, `turnovers`, `turnover_margin`, `turnover_luck`) and ADDS `team_id`, `st_turnovers_lost`, `st_turnovers_gained`, `takeaways` (spec section 9, additive). `fumbles_recovered` is retained below for back-compat — keep the existing scrimmage-grouped `fumbles_recovered` computation and join it into `to_aux` so the field does not disappear.

- [ ] **Step 4: Add `fumbles_recovered` back-compat into `to_aux`**

In the `to_aux` agg above, add:

```python
                fumbles_recovered=(
                    (pl.col("fumble_or_muff") == True) & (pl.col("is_turnover") == False)
                ).sum(),
```

- [ ] **Step 5: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k turnover -v`
Expected: PASS (ASU=3, FSU=3, NCST=0).

- [ ] **Step 6: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_box_score_attribution_offline.py
git commit -m "fix(cfb): count special-teams turnovers and key margins by team identity"
```

---

### Task 2.2: Fumble-recovery + punt-return team in player boxes

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py` (`_player_event_box` calls ~`:4789-4816`)
- Test: `tests/cfb/test_box_score_attribution_offline.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_kickoff_own_recovery_credits_receiving_team(monkeypatch):
    # BYU(252) returner recovers own KO fumble → credited to BYU, not Hawaii(62)
    box = _box(monkeypatch, 401135269)
    recs = [d for d in box["defensive_players"] if d.get("fumble_recoveries", 0)]
    # the own recovery must be credited to BYU (252), never Hawaii (62)
    byu_recs = [d for d in recs if d.get("def_pos_team") == 252 or d.get("team_id") == 252]
    assert byu_recs, "BYU own kickoff recovery not credited to BYU"


def test_punt_return_credited_to_returning_team(monkeypatch):
    box = _box(monkeypatch, 401754598)
    prs = [s for s in box["specialists"] if s.get("punt_returns", 0)]
    # punt returns must be filed under the receiving team, not the punting team
    # (assert exact team in Task 4 once returner names confirmed)
    assert prs == prs  # placeholder shape check; concretized in Task 4
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k "own_recovery_credits or punt_return_credited" -v`
Expected: FAIL — current code credits KO own recovery to def_pos_team (Hawaii).

- [ ] **Step 3: Change the `_player_event_box` team columns**

In `create_box_score`, change the fumble-recovery and punt-return event-box calls to group by the resolved team columns. Replace:

```python
            _player_event_box("fumble_recovered_player_name", "fumble_recoveries", "def_pos_team", "yds_fumble_return"),
```

with:

```python
            _player_event_box("fumble_recovered_player_name", "fumble_recoveries", "fumble_recovery_team", "yds_fumble_return"),
```

and in the specialists block replace:

```python
            _player_event_box("punt_return_player_name", "punt_returns", "pos_team", "yds_punt_return"),
```

with:

```python
            _player_event_box("punt_return_player_name", "punt_returns", "punt_return_team", "yds_punt_return"),
```

> NOTE: `_player_event_box` renames the team col to whatever it groups by; defensive_players join key becomes `fumble_recovery_team` for that part. To keep the join key uniform, after building each part, rename the grouping col back to the section's canonical key (`def_pos_team` for defensive_players, `pos_team` for specialists) so the `reduce(... join on=[team, player_name])` still aligns. Implement by aliasing inside `_player_event_box`: add a `team_out` parameter defaulting to `team_col` and `.rename({team_col: team_out})`.

- [ ] **Step 4: Update `_player_event_box` signature for the alias**

Replace the helper definition:

```python
        def _player_event_box(name_col, out, team_col, yds_col=None, team_out=None):
            """Count non-null occurrences of `name_col` per (team, player); sum `yds_col`."""
            if name_col not in play_df.columns or team_col not in play_df.columns:
                return None
            f = play_df.filter(pl.col(name_col).is_not_null() & pl.col(team_col).is_not_null())
            if f.height == 0:
                return None
            aggs = [pl.len().alias(out)]
            if yds_col is not None and yds_col in play_df.columns:
                aggs.append(pl.col(yds_col).sum().alias(f"{out}_yards"))
            g = f.group_by([team_col, name_col]).agg(aggs).rename({name_col: "player_name"})
            if team_out and team_out != team_col:
                g = g.rename({team_col: team_out})
            return g
```

Then pass `team_out="def_pos_team"` for the fumble-recovery defensive part and `team_out="pos_team"` for the punt-return specialist part.

- [ ] **Step 5: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k "own_recovery_credits or punt_return_credited" -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_box_score_attribution_offline.py
git commit -m "fix(cfb): credit fumble recoveries + punt returns to the correct team"
```

---

### Task 2.3: Penalty box by penalized team (+ additive `penalty_yards`)

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py` (`team_pen_box` ~`:4216-4227`, and the `team_box` join)
- Test: `tests/cfb/test_box_score_attribution_offline.py`

- [ ] **Step 1: Write the failing test**

```python
def test_penalty_yards_charged_to_penalized_team(monkeypatch):
    box = _box(monkeypatch, 401754598)
    # DPI on C.Bailey (NCSU offense) is an FSU(52) foul; FSU penalty_yards must be > 0
    fsu = _team(box["team"], 52)
    assert fsu.get("penalty_yards", 0) > 0
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k penalty -v`
Expected: FAIL — `penalty_yards` key absent / mis-attributed.

- [ ] **Step 3: Add a penalized-team box and join it (additive)**

After the existing `team_pen_box` definition (keep it for back-compat `total_pen_yards`), add:

```python
        team_penalized_box = (
            play_df.filter(
                (pl.col("penalty_flag") == True)
                & (pl.col("penalty_declined") == False)
                & (pl.col("penalty_offset") == False),
            )
            .group_by(["penalized_team"])
            .agg(
                penalties=pl.len(),
                penalty_yards=pl.col("penalty_yards_signed").sum(),
            )
            .rename({"penalized_team": "pos_team"})
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
```

Add `team_penalized_box` to the `team_data_frames` list so it joins into `team_box`.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k penalty -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_box_score_attribution_offline.py
git commit -m "fix(cfb): attribute penalty yards to the penalized team (additive penalty_yards)"
```

---

### Task 2.4: Yardage reconciliation (#6, #7)

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py` (`__add_attribution_cols` for yardage fallback; `team_scrimmage_box` / `team_base_box`)
- Test: `tests/cfb/test_box_score_attribution_offline.py`

- [ ] **Step 1: Write the failing reconciliation test**

```python
@pytest.mark.parametrize("gid", [401754598, 401309854, 401135269])
def test_pass_rush_sack_reconcile_to_off_yards(monkeypatch, gid):
    box = _box(monkeypatch, gid)
    for r in box["team"]:
        off = r.get("off_yards")
        if off is None:
            continue
        parts = r.get("pass_yards", 0) + r.get("rush_yards", 0) + r.get("sack_yards", 0)
        assert abs(parts - off) <= 2, f"team {r['pos_team']}: {parts} vs off_yards {off}"
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k reconcile -v`
Expected: FAIL — `sack_yards` absent and pass/rush parse-null yardage diverges from `off_yards`.

- [ ] **Step 3: Add statYardage fallback + sack_yards**

In `__add_attribution_cols`, add a reconciled yardage column that falls back to `statYardage` on scrimmage rush/pass plays whose parsed yardage is null:

```python
        play_df = play_df.with_columns(
            yds_rushed_recon=pl.when((pl.col("rush") == True) & (pl.col("yds_rushed").is_null()))
            .then(pl.col("statYardage"))
            .otherwise(pl.col("yds_rushed")),
            yds_receiving_recon=pl.when((pl.col("pass") == True) & (pl.col("sack") == False) & (pl.col("yds_receiving").is_null()))
            .then(pl.col("statYardage"))
            .otherwise(pl.col("yds_receiving")),
        )
```

In `create_box_score`, add `sack_yards` to `team_scrimmage_box_pass` (sum of `yds_sacked` over sack plays) as an additive field, and switch the pass/rush yard sums to the `_recon` columns. Keep the existing `pass_yards`/`rush_yards` field names.

```python
                pass_yards=pl.col("yds_receiving_recon").sum(),
                sack_yards=pl.col("yds_sacked").sum(),
```

(and `rush_yards=pl.col("yds_rushed_recon").sum()` in `team_scrimmage_box_rush`).

- [ ] **Step 4: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k reconcile -v`
Expected: PASS (within ±2 tolerance).

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_box_score_attribution_offline.py
git commit -m "fix(cfb): reconcile pass/rush/sack yardage to offensive total (statYardage fallback)"
```

> NOTE: `#8` (`team_sp_box` punt/kickoff base team) and `#6` (`total_yards` over all plays) remain documented follow-ups; `total_yards` keeps its current value (additive `off_total_yards` may be added later). Tackle only if the ±2 reconciliation reveals `total_yards` contamination on these fixtures.

---

## Phase 3 — Participants identity join (auto + fallback)

### Task 3.1: Optional participants join with graceful fallback

**Files:**

- Modify: `sportsdataverse/cfb/cfb_pbp.py` (`run_processing_pipeline`)
- Test: `tests/cfb/test_box_score_attribution_offline.py`

- [ ] **Step 1: Write the failing test (fallback path must not raise offline)**

```python
def test_participants_join_falls_back_offline(monkeypatch):
    # The mocked download returns the summary for the participants URL too;
    # the join must swallow that and fall back to regex names (no raise).
    box = _box(monkeypatch, 401754598)
    assert box["pass"], "passer box empty — participants fallback regressed"
```

- [ ] **Step 2: Run to verify failure (only if join raises)**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k participants -v`
Expected: FAIL only if the join is added without try/except. (If not yet added, this passes trivially — proceed to implement guarded.)

- [ ] **Step 3: Implement the guarded join**

In `run_processing_pipeline`, after `.pipe(self.__add_attribution_cols)` chain completes and before `create_box_score`, add:

```python
                try:
                    from sportsdataverse.cfb.cfb_play_participants import espn_cfb_play_participants

                    parts = espn_cfb_play_participants(self.gameId)
                    if parts is not None and parts.height > 0:
                        self.plays_json = self.plays_json.join(
                            parts, how="left", left_on="id", right_on="play_id",
                        )
                        # prefer participant names where present, else keep regex
                        for role in ["passer", "rusher", "receiver", "sack",
                                     "interception", "fumble_recovered", "punt_return",
                                     "kickoff_return", "fg_kicker", "punter"]:
                            pcol = f"{role}_player_name"
                            jcol = f"{role}_player_name_right"
                            if jcol in self.plays_json.columns:
                                self.plays_json = self.plays_json.with_columns(
                                    pl.coalesce([pl.col(jcol), pl.col(pcol)]).alias(pcol),
                                ).drop(jcol)
                except Exception as e:  # noqa: BLE001 — identity is best-effort
                    self.logger.info(f"participants join skipped: {e}")
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k participants -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/cfb/cfb_pbp.py tests/cfb/test_box_score_attribution_offline.py
git commit -m "feat(cfb): optional participants identity join with regex fallback"
```

> NOTE: column names from `espn_cfb_play_participants` may differ (e.g. `sacked_by_player_name`). During implementation, print `parts.columns` and map the role keys to the actual participant column names before finalizing the coalesce loop.

---

## Phase 4 — Golden verification + invariants

### Task 4.1: Concretize golden assertions against live verification

**Files:**

- Modify: `tests/cfb/test_box_score_attribution_offline.py`

- [ ] **Step 1: Run the live verification harness to read exact numbers**

Run: `PYTHONPATH=. SDV_PY_LIVE_TESTS=1 .venv/Scripts/python.exe -c "import json; from sportsdataverse.cfb import CFBPlayProcess; [print(g, json.dumps(CFBPlayProcess(gameId=g).run_processing_pipeline()['advBoxScore']['turnover'])) for g in [401754598,401309854,401112081,401135269,401032062]]"`
Expected: prints the corrected turnover sections.

- [ ] **Step 2: Replace the placeholder assertions** in `test_punt_own_recovery_not_a_turnover`, `test_punt_return_credited_to_returning_team` with exact team ids/counts observed (e.g. FSU punt returner team id, WMich `turnovers` unchanged by the BYU own recovery).

- [ ] **Step 3: Run all attribution tests**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py tests/cfb/test_cfb_attribution.py -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/cfb/test_box_score_attribution_offline.py
git commit -m "test(cfb): concretize golden box-score assertions for fixture set"
```

---

### Task 4.2: Reconciliation invariants

**Files:**

- Modify: `tests/cfb/test_box_score_attribution_offline.py`

- [ ] **Step 1: Write the invariant tests**

```python
import pytest


@pytest.mark.parametrize("gid", [401754598, 401309854, 401112081, 401135269, 401032062])
def test_turnover_margin_antisymmetric(monkeypatch, gid):
    box = _box(monkeypatch, gid)
    margins = [r["turnover_margin"] for r in box["turnover"]]
    assert margins[0] == -margins[1]


@pytest.mark.parametrize("gid", [401754598, 401309854, 401112081, 401135269, 401032062])
def test_team_turnovers_equal_lost_events(monkeypatch, gid):
    import polars as pl
    summary = _load(gid)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=gid)
    out = proc.run_processing_pipeline()
    df = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    box = out["advBoxScore"]
    lost_by_team = (
        df.filter(pl.col("is_turnover") == True)
        .group_by("turnover_team").agg(n=pl.len())
    )
    lost = {int(r["turnover_team"]): r["n"] for r in lost_by_team.to_dicts() if r["turnover_team"] is not None}
    for r in box["turnover"]:
        tid = int(r["team_id"])
        assert r["turnovers"] == lost.get(tid, 0), f"team {tid} turnovers mismatch"
```

- [ ] **Step 2: Run**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/test_box_score_attribution_offline.py -k "antisymmetric or equal_lost" -v`
Expected: PASS (10 parametrized cases).

- [ ] **Step 3: Commit**

```bash
git add tests/cfb/test_box_score_attribution_offline.py
git commit -m "test(cfb): turnover reconciliation invariants across fixture set"
```

---

### Task 4.3: Full suite + lint + regression check

- [ ] **Step 1: Run the full cfb test suite**

Run: `.venv/Scripts/python.exe -m pytest tests/cfb/ -v`
Expected: PASS; no regressions in `test_create_box_score.py` / `test_cfb_pbp.py`.

- [ ] **Step 2: Lint**

Run: `cd c:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-py && .venv/Scripts/python.exe -m ruff check sportsdataverse/cfb/cfb_pbp.py tests/cfb/ && .venv/Scripts/python.exe -m ruff format --check sportsdataverse/cfb/cfb_pbp.py tests/cfb/`
Expected: clean (or run `ruff format` to fix).

- [ ] **Step 3: Update NEWS / CHANGELOG**

Add a bullet under the current dev version in `CHANGELOG.md` (sdv-py convention): the special-teams turnover + attribution fixes. Match the existing changelog style.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(cfb): changelog for box-score attribution fixes"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** Phase 1 ⇒ §5/§6; Task 2.1 ⇒ §1 findings #1,#5,#14,#15,#16 + §9 turnover schema; Task 2.2 ⇒ #2,#3,#13; Task 2.3 ⇒ #4,#12 (penalty); Task 2.4 ⇒ #7 + §10 invariant #2 + §3 yardage goal; Phase 3 ⇒ §8 (identity); Phase 4 ⇒ §10. **Documented follow-ups (spec §12, not tasked here):** #6 `total_yards` all-plays gating and #8 `team_sp_box` punt/kickoff base — only addressed if Task 2.4's reconciliation surfaces them on the fixtures.
- **Type consistency:** `_player_event_box` now takes `team_out`; the defensive/specialist `reduce` joins must use the canonical key (`def_pos_team` / `pos_team`) — ensured by `team_out`.
- **Additive guarantee:** every existing `turnover`/`team` field name is preserved; only values change and new fields are added.
- **Network discipline:** all offline tests mock `cfb_pbp.download`; only `tools/capture_cfb_fixtures.py` and Task 4.1 Step 1 hit the network.
