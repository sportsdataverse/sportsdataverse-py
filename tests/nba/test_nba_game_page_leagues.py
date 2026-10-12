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
