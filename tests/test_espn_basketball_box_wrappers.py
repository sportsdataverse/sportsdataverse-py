"""hoopR-named per-game wrappers (``espn_{mbb,nba}_{team_box,player_box,game_all}``) offline.

Each wrapper is one ``summary`` request run through the release helpers, so on a committed ESPN
capture (``tests/fixtures/espn/basketball_pbp/README.md``) it must return exactly what the helper
returns on the raw payload, and ``game_all`` must make one request for all three frames.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse import mbb, nba
from sportsdataverse.mbb import mbb_pbp
from sportsdataverse.nba import nba_pbp

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "espn" / "basketball_pbp"

CASES = [
    pytest.param(mbb, mbb_pbp, "mbb", 401830342, id="mbb"),
    pytest.param(nba, nba_pbp, "nba", 401704638, id="nba"),
]


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return copy.deepcopy(self._payload)


def _patch(monkeypatch, module, raw: dict) -> list[str]:
    urls: list[str] = []

    def _download(url, **kwargs):
        urls.append(url)
        return _Resp(raw)

    monkeypatch.setattr(module, "download", _download)
    return urls


def _raw(lg: str, game_id: int) -> dict:
    with gzip.open(FIXTURES / f"{lg}_{game_id}.json.gz", "rt", encoding="utf-8") as f:
        return json.load(f)


@pytest.mark.parametrize(("pkg", "module", "lg", "game_id"), CASES)
def test_box_wrappers_return_the_release_helpers_frames(monkeypatch, pkg, module, lg, game_id) -> None:
    raw = _raw(lg, game_id)
    urls = _patch(monkeypatch, module, raw)

    team = getattr(pkg, f"espn_{lg}_team_box")(game_id)
    player = getattr(pkg, f"espn_{lg}_player_box")(game_id)

    assert_frame_equal(team, getattr(pkg, f"helper_{lg}_team_box")(copy.deepcopy(raw)))
    assert_frame_equal(player, getattr(pkg, f"helper_{lg}_player_box")(copy.deepcopy(raw)))
    assert team.height == 2
    assert player.height == sum(
        len(a) for t in raw["boxscore"]["players"] for s in t["statistics"] for a in [s["athletes"]]
    )
    assert len(urls) == 2 and all(u.endswith(f"/summary?event={game_id}") for u in urls)


@pytest.mark.parametrize(("pkg", "module", "lg", "game_id"), CASES)
def test_game_all_is_one_request_for_plays_and_both_boxes(monkeypatch, pkg, module, lg, game_id) -> None:
    raw = _raw(lg, game_id)
    urls = _patch(monkeypatch, module, raw)
    game = getattr(pkg, f"espn_{lg}_game_all")(game_id)
    assert len(urls) == 1
    assert list(game) == ["Plays", "Team", "Player"]

    # the same frames the one-request-each wrappers return (game_all runs the box helpers before
    # the play-by-play step reshapes the payload's header)
    assert_frame_equal(game["Plays"], getattr(pkg, f"espn_{lg}_pbp")(game_id))
    assert_frame_equal(game["Team"], getattr(pkg, f"espn_{lg}_team_box")(game_id))
    assert_frame_equal(game["Player"], getattr(pkg, f"espn_{lg}_player_box")(game_id))
    assert game["Plays"]["game_id"].unique().to_list() == [game_id]
    assert set(game["Team"]["game_id"].to_list()) == {game_id}


@pytest.mark.parametrize(("pkg", "module", "lg", "game_id"), CASES)
def test_return_as_pandas_keeps_integer_ids(monkeypatch, pkg, module, lg, game_id) -> None:
    _patch(monkeypatch, module, _raw(lg, game_id))
    game = getattr(pkg, f"espn_{lg}_game_all")(game_id, return_as_pandas=True)
    assert all(isinstance(df, pd.DataFrame) for df in game.values())
    assert str(game["Team"]["team_id"].dtype).startswith("int")
    assert isinstance(getattr(pkg, f"espn_{lg}_player_box")(game_id, return_as_pandas=True), pd.DataFrame)
    assert isinstance(getattr(pkg, f"espn_{lg}_team_box")(game_id), pl.DataFrame)
