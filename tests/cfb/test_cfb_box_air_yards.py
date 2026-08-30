"""Air-yards aggregates in ``create_box_score`` (passer + receiver lines)."""

import json
from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess, _air_yards_box

FIX = Path(__file__).parent / "fixtures"


def _box(monkeypatch, game_id=401754598):
    summary = json.loads((FIX / f"summary_{game_id}.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=game_id)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    plays = pl.from_dicts(proc.plays_json, infer_schema_length=None)
    return plays, proc.create_box_score(plays)


def test_passer_and_receiver_air_yards_on_real_game(monkeypatch):
    plays, box = _box(monkeypatch)
    passers = pl.from_dicts(box["pass"], infer_schema_length=None)
    receivers = pl.from_dicts(box["receiver"], infer_schema_length=None)
    for df in (passers, receivers):
        assert {"AirYds", "aDOT", "CompAirYds", "YAC", "AirYdsPct"} <= set(df.columns)

    # The fixture has catch spots -> the starting QBs must have populated aDOT.
    top = passers.sort("Att", descending=True).head(2)
    assert top["aDOT"].drop_nulls().len() == 2
    assert (top["AirYdsPct"].drop_nulls() <= 1.0).all()

    # Decomposition: CompAirYds + YAC == receiving yards on completions, per passer.
    comp = plays.filter((pl.col("completion") == True) & (pl.col("yards_after_catch").is_not_null()))
    per_qb = comp.group_by("passer_player_name").agg(
        air=pl.col("air_yards").sum(), yac=pl.col("yards_after_catch").sum(), yds=pl.col("statYardage").sum()
    )
    assert (per_qb["air"] + per_qb["yac"] == per_qb["yds"]).all()
    joined = passers.join(per_qb, on="passer_player_name", how="inner")
    assert joined.height > 0
    assert (joined["YAC"] == joined["yac"]).all()


def test_air_yards_box_is_null_not_zero_without_data():
    """A game whose text carries no catch spot must show blanks, not 0.0."""
    pass_box = pl.DataFrame(
        {
            "pos_team": [1, 1, 1],
            "passer_player_name": ["A", "A", "B"],
            "completion": [True, False, True],
            "air_yards": [None, None, None],
            "yards_after_catch": [None, None, None],
            "yds_receiving": [10.0, 0.0, 5.0],
        },
        schema_overrides={"air_yards": pl.Int64, "yards_after_catch": pl.Int64},
    )
    out = _air_yards_box(pass_box, "passer_player_name")
    for c in ("AirYds", "aDOT", "CompAirYds", "YAC", "AirYdsPct"):
        assert out[c].is_null().all(), c


def test_air_yards_box_zero_air_yard_screen_counts():
    """A screen (air_yards == 0) is data: aDOT is 0.0, not null."""
    pass_box = pl.DataFrame(
        {
            "pos_team": [1, 1],
            "passer_player_name": ["A", "A"],
            "completion": [True, True],
            "air_yards": [0, 10],
            "yards_after_catch": [12, 0],
            "yds_receiving": [12.0, 10.0],
        }
    )
    out = _air_yards_box(pass_box, "passer_player_name")
    row = out.row(0, named=True)
    assert row["AirYds"] == 10 and row["CompAirYds"] == 10 and row["YAC"] == 12
    assert row["aDOT"] == 5.0
    assert abs(row["AirYdsPct"] - round(10 / 22, 2)) < 1e-6  # helper rounds to 2 dp
