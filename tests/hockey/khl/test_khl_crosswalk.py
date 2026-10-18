"""Offline tests for the KHL club id <-> team-season id crosswalk, on the committed real captures."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.errors import NoDataError
from sportsdataverse.hockey.khl import khl_club_crosswalk, khl_club_crosswalk_from_frames
from sportsdataverse.hockey.khl import khl_crosswalk_runtime as X
from sportsdataverse.hockey.khl import khl_rest, khl_rest_parsers as P

FIXTURES = Path(__file__).parents[3] / "tests" / "fixtures" / "khl_rest"


def _load(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_game_header_gives_both_ids() -> None:
    xw = khl_club_crosswalk_from_frames(P.parse_khl_game_header(_load("game__header.json")))
    assert xw.schema == {"tournament_id": pl.Int64, "club_id": pl.Int64, "team_season_id": pl.Int64}
    assert xw.rows() == [(1369, 7, 14934), (1369, 71, 14929)]


def test_player_career_and_header_agree_and_dedupe() -> None:
    career = P.parse_khl_player_career(_load("players__stats.json"))
    xw = khl_club_crosswalk_from_frames(P.parse_khl_game_header(_load("game__header.json")), career)
    assert (1369, 7, 14934) in xw.rows() and xw.height == len(set(xw.rows()))


def test_schedule_without_team_season_ids_is_ignored_and_empty_is_typed() -> None:
    xw = khl_club_crosswalk_from_frames(P.parse_khl_schedule(_load("calendar__list.json")))
    assert xw.height == 0 and xw.schema["team_season_id"] == pl.Int64


def test_pandas_in_and_out() -> None:
    header = P.parse_khl_game_header(_load("game__header.json")).to_pandas()
    xw = khl_club_crosswalk_from_frames(header, return_as_pandas=True)
    assert isinstance(xw, pd.DataFrame) and len(xw) == 2


def test_conflicting_sources_raise_rather_than_pick() -> None:
    a = pl.DataFrame({"tournament_id": [1], "club_id": [7], "team_season_id": [100]})
    b = pl.DataFrame({"tournament_id": [1], "club_id": [8], "team_season_id": [100]})
    with pytest.raises(ValueError, match="conflicts"):
        khl_club_crosswalk_from_frames(a, b)


def test_cover_touches_every_club_with_few_games() -> None:
    schedule = P.parse_khl_schedule(_load("calendar__list.json"))
    games = X._cover(schedule)
    rows = schedule.filter(pl.col("game_id").is_in(games))
    touched = set(rows["home_club_id"]) | set(rows["away_club_id"])
    assert touched == set(schedule["home_club_id"]) | set(schedule["away_club_id"])
    assert len(games) <= schedule.height


def test_season_crosswalk_fetches_only_the_covering_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    schedule = P.parse_khl_schedule(_load("calendar__list.json"))
    header = P.parse_khl_game_header(_load("game__header.json"))
    asked = []

    def fake_header(game_id, **kw):
        asked.append((game_id, kw["tournament_id"]))
        return header

    monkeypatch.setattr(khl_rest, "khl_schedule", lambda *a, **k: schedule)
    monkeypatch.setattr(khl_rest, "khl_game_header", fake_header)
    # every header is the same game, so all but its two clubs stay unmapped: that raises, never a short frame
    with pytest.raises(NoDataError, match="no team-season id for club"):
        khl_club_crosswalk(season=2026)
    assert [g for g, _ in asked] == X._cover(schedule) and {t for _, t in asked} == {1369}
    # one real schedule row re-pointed at the header game's two clubs: both get mapped, no raise
    pair = schedule.head(1).with_columns(home_club_id=pl.lit(7, pl.Int64), away_club_id=pl.lit(71, pl.Int64))
    monkeypatch.setattr(khl_rest, "khl_schedule", lambda *a, **k: pair)
    assert khl_club_crosswalk(season=2026).rows() == [(1369, 7, 14934), (1369, 71, 14929)]


def test_season_crosswalk_without_a_schedule_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    empty = P.parse_khl_schedule(_load("calendar__list.json")).head(0)
    monkeypatch.setattr(khl_rest, "khl_schedule", lambda *a, **k: empty)
    with pytest.raises(NoDataError, match="no schedule rows"):
        khl_club_crosswalk(season=2026)
