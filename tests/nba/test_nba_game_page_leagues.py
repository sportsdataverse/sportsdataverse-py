"""nba_game_page across leagues (nba / wnba / mbb / wbb) on real ESPN captures.

Fixtures: tests/fixtures/espn/summary_*.json and tests/fixtures/espn/basketball_pbp/*.json.gz
(provenance in tests/fixtures/espn/basketball_pbp/README.md).
"""

from __future__ import annotations

import copy
import gzip
import importlib
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba import nba_game_page as gp

ESPN = Path(__file__).resolve().parents[1] / "fixtures" / "espn"
BB = ESPN / "basketball_pbp"


def _raw(name: str) -> dict:
    if name.endswith(".gz"):
        with gzip.open(BB / name, "rt", encoding="utf-8") as f:
            return json.load(f)
    return json.loads((ESPN / name).read_text(encoding="utf-8"))


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._p = payload

    def json(self) -> dict:
        return self._p


def _final(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> dict:
    """``espn_<lg>_pbp(game_id)`` on the fixture, offline (the fixture is served as the HTTP body)."""
    raw = _raw(name)
    mod = importlib.import_module(f"sportsdataverse.{lg}.{lg}_pbp")
    monkeypatch.setattr(mod, "download", lambda url, **kwargs: _Resp(copy.deepcopy(raw)))
    return getattr(mod, f"espn_{lg}_pbp")(game_id=int(raw["header"]["id"]))


def _frames(
    monkeypatch: pytest.MonkeyPatch, lg: str, name: str
) -> tuple[dict, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """``(final, pbp, team_box, player_box)`` through the league's hoopR-schema converters."""
    final = _final(monkeypatch, lg, name)

    def helper(kind: str):  # noqa: ANN202 - test-local lookup
        return getattr(importlib.import_module(f"sportsdataverse.{lg}.{lg}_{kind}"), f"helper_{lg}_{kind}")

    return final, helper("play_by_play")(final), helper("team_box")(final), helper("player_box")(final)


@pytest.mark.parametrize(
    ("name", "season", "periods", "neutral", "max_period", "plays"),
    [
        ("wnba_250603003.json.gz", 2005, 2, False, 3, 459),
        ("wbb_293170052.json.gz", 2010, 2, False, 2, 351),
        ("wbb_273130043.json.gz", 2008, 2, False, 2, 358),
        ("mbb_401812161.json.gz", 2026, 2, False, 2, 425),
        ("mbb_401830342.json.gz", 2026, 2, True, 4, 426),
        ("summary_mbb.json", 2024, 2, True, 2, 294),
        ("summary_wbb.json", 2024, 4, True, 4, 360),
        ("summary_wnba.json", 2024, 4, False, 5, 418),
    ],
)
def test_capture_provenance(name: str, season: int, periods: int, neutral: bool, max_period: int, plays: int) -> None:
    raw = _raw(name)
    comp = raw["header"]["competitions"][0]
    got = (
        raw["header"]["season"]["year"],
        raw["format"]["regulation"]["periods"],
        comp["neutralSite"],
        max(p["period"]["number"] for p in raw["plays"]),
        len(raw["plays"]),
    )
    assert got == (season, periods, neutral, max_period, plays)


def test_league_id_callers_keep_their_meaning() -> None:
    assert gp.game_minutes(5) == 53 and gp.game_minutes(5, league_id="00") == 53
    assert gp.game_minutes(5, league_id="10") == 45 == gp.game_minutes(5, league="wnba")
    with pytest.raises(ValueError, match="'00'.*'10'"):
        gp.game_minutes(4, league_id="20")
    with pytest.raises(ValueError, match="contradicts"):
        gp.game_minutes(4, league="mbb", league_id="00")
    with pytest.raises(ValueError, match="'nba', 'wnba', 'mbb', 'wbb'"):
        gp.game_minutes(4, league="nhl")


@pytest.mark.parametrize(
    ("league", "season", "fmt", "want"),
    [
        ("nba", 2024, None, 4),
        ("mbb", 2026, None, 2),
        ("mbb", None, 2.0, 2),
        ("wnba", 2005, None, 2),
        ("wnba", 2006, None, 4),
        ("wnba", None, None, 4),
        ("wbb", 2015, None, 2),
        ("wbb", 2016, None, 4),
        ("wbb", 2016, 2.0, 2),  # the game's own format wins over the season cutoff
        ("wnba", 2005, float("nan"), 2),  # a null schedule cell falls back to the cutoff
        ("mbb", 2026, 3.0, 2),  # only 2 or 4 is believed
    ],
)
def test_game_regulation_periods(league: str, season: int | None, fmt: float | None, want: int) -> None:
    assert gp.game_regulation_periods(league, season=season, format_regulation_periods=fmt) == want


def test_game_minutes_count_overtime_past_the_games_own_regulation() -> None:
    assert gp.game_minutes(2, league="mbb") == 40
    assert gp.game_minutes(4, league="mbb") == 50  # 2OT in a halves game (mbb_401830342)
    assert gp.game_minutes(4, league="wbb") == 40 and gp.game_minutes(5, league="wbb") == 45
    assert gp.game_minutes(3, league="wnba", regulation_periods=2) == 45  # 2005 halves, one OT
    assert gp.game_minutes(2, league="wbb", regulation_periods=2) == 40
