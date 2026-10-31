"""``espn_mbb_pbp`` / ``espn_nba_pbp`` return the release play-by-play frame, offline.

Payloads are the committed ESPN summary captures in ``tests/fixtures/espn/basketball_pbp/``
(provenance in its README), served as the HTTP body. The release contract is the committed
``load_<lg>_pbp`` returns table (``_returns_schemas.SCHEMAS``, captured from the published
parquets): every column the function emits must be a release column with the release dtype.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from sportsdataverse._returns_schemas import SCHEMAS
from sportsdataverse.mbb import mbb_pbp
from sportsdataverse.nba import nba_pbp

BB = Path(__file__).parent / "fixtures" / "espn" / "basketball_pbp"
MODULES = {"nba": nba_pbp, "mbb": mbb_pbp}
CASES = [("mbb", g) for g in (401830342, 401856600, 401829457, 401812161, 401828345)] + [
    ("nba", g) for g in (401766458, 400829116, 401704638, 260312029)
]
# Columns every season of both releases carries.
CORE = {
    "game_play_number", "id", "sequence_number", "type_id", "type_text", "text", "away_score", "home_score",
    "period_number", "clock_display_value", "scoring_play", "shooting_play", "score_value", "team_id",
    "athlete_id_1", "athlete_id_2", "game_id", "season", "season_type", "home_team_id", "away_team_id",
    "game_spread", "home_team_spread", "game_date", "game_date_time", "coordinate_x", "coordinate_y",
    "coordinate_x_raw", "coordinate_y_raw", "athlete_name_1", "athlete_name_2", "athlete_name_3", "score_source",
}  # fmt: skip
# Release columns a single game does not emit: the MBB season-level win-probability enrichment,
# and payload fields only some games' plays carry (MBB mediaId; NBA type.abbreviation, and
# pointsAttempted / shortDescription / wallclock, which the 2006 and 2016 payloads predate).
NOT_PER_GAME = {
    "mbb": {"pregame_home_prob", "home_win_prob", "media_id"},
    "nba": {"type_abbreviation", "points_attempted", "short_description", "wallclock"},
}


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return copy.deepcopy(self._payload)


def _raw(lg: str, gid: int) -> dict:
    with gzip.open(BB / f"{lg}_{gid}.json.gz", "rt", encoding="utf-8") as f:
        return json.load(f)


def _call(monkeypatch: pytest.MonkeyPatch, lg: str, payload: dict, **kw):
    monkeypatch.setattr(MODULES[lg], "download", lambda url, **kwargs: _Resp(payload))
    return getattr(MODULES[lg], f"espn_{lg}_pbp")(game_id=int(payload["header"]["id"]), **kw)


@pytest.mark.parametrize(("lg", "gid"), CASES)
def test_default_is_the_release_frame(monkeypatch: pytest.MonkeyPatch, lg: str, gid: int) -> None:
    raw = _raw(lg, gid)
    df = _call(monkeypatch, lg, raw)
    release = SCHEMAS[f"loader_schemas:load_{lg}_pbp"]

    assert isinstance(df, pl.DataFrame)
    assert [c for c in df.columns if c not in release] == []
    assert {c: str(df.schema[c]) for c in df.columns if df.schema[c] != release[c]} == {}
    assert CORE <= set(df.columns)
    assert set(release) - set(df.columns) <= NOT_PER_GAME[lg]
    assert df.schema["id"] == pl.Int64 and df.schema["team_id"] == pl.Int32 == df.schema["athlete_id_1"]

    assert df["id"].to_list() == [int(p["id"]) for p in raw["plays"]]
    assert df["game_id"].unique().to_list() == [gid]
    comp = raw["header"]["competitions"][0]
    home = next(c for c in comp["competitors"] if c["homeAway"] == "home")
    away = next(c for c in comp["competitors"] if c["homeAway"] == "away")
    assert df["home_team_id"].unique().to_list() == [int(home["team"]["id"])]
    assert df["away_team_id"].unique().to_list() == [int(away["team"]["id"])]
    last = df.row(-1, named=True)
    if last["score_source"] == "rebuilt":
        assert (last["home_score"], last["away_score"]) == (int(home["score"]), int(away["score"]))

    names = {
        int(a["athlete"]["id"]): a["athlete"]["displayName"]
        for t in raw["boxscore"]["players"]
        for s in t["statistics"]
        for a in s["athletes"]
    }
    named = df.filter(pl.col("athlete_id_1").is_in(list(names)))
    assert named.height > 0
    assert named["athlete_name_1"].to_list() == [names[i] for i in named["athlete_id_1"]]

    # the docstring's shot-chart example runs on the frame
    shots = df.filter(pl.col("shooting_play") == True).select(  # noqa: E712
        "period_number", "clock_display_value", "team_id", "coordinate_x", "coordinate_y", "score_value", "text"
    )
    assert shots.height > 0


@pytest.mark.parametrize("lg", ["mbb", "nba"])
def test_return_parsed_false_keeps_the_game_dict(monkeypatch: pytest.MonkeyPatch, lg: str) -> None:
    gid = next(g for x, g in CASES if x == lg)
    raw = _raw(lg, gid)
    game = _call(monkeypatch, lg, raw, return_parsed=False)
    assert isinstance(game, dict) and game["gameId"] == gid
    assert [p["id"] for p in game["plays"]] == [int(p["id"]) for p in raw["plays"]]
    assert "period.number" in game["plays"][0]
    assert _call(monkeypatch, lg, raw, raw=True)["plays"] == raw["plays"]


@pytest.mark.parametrize("lg", ["mbb", "nba"])
def test_return_as_pandas(monkeypatch: pytest.MonkeyPatch, lg: str) -> None:
    raw = _raw(lg, next(g for x, g in CASES if x == lg))
    pdf = _call(monkeypatch, lg, raw, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    df = _call(monkeypatch, lg, raw)
    assert list(pdf.columns) == df.columns
    # nullable ids stay integer, as load_<lg>_pbp(return_as_pandas=True) returns them (never float)
    assert df["athlete_id_2"].null_count() > 0
    assert str(pdf["athlete_id_2"].dtype) == "int32[pyarrow]"
    assert dict(pdf.dtypes) == dict(df.to_pandas(use_pyarrow_extension_array=True).dtypes)


@pytest.mark.parametrize("lg", ["mbb", "nba"])
@pytest.mark.parametrize("empty", ["ten_plays", "no_pbp_source"])
def test_a_game_without_play_by_play_is_the_release_schema(
    monkeypatch: pytest.MonkeyPatch, lg: str, empty: str
) -> None:
    # hoopR's helper returns NULL on <= 10 plays; the frame is the release's zero-row schema
    raw = _raw(lg, next(g for x, g in CASES if x == lg))
    if empty == "ten_plays":
        raw["plays"] = raw["plays"][:10]
    else:
        raw["header"]["competitions"][0]["playByPlaySource"] = "none"
    df = _call(monkeypatch, lg, raw)
    release = SCHEMAS[f"loader_schemas:load_{lg}_pbp"]
    assert df.height == 0
    assert dict(df.schema) == dict(release)


@pytest.mark.parametrize(("lg", "gid"), CASES)
def test_columns_come_in_the_release_order(monkeypatch: pytest.MonkeyPatch, lg: str, gid: int) -> None:
    # so the frame stacks with load_<lg>_pbp output (pl.concat how="vertical" needs the same order)
    df = _call(monkeypatch, lg, _raw(lg, gid))
    assert df.columns == [c for c in SCHEMAS[f"loader_schemas:load_{lg}_pbp"] if c in df.columns]
