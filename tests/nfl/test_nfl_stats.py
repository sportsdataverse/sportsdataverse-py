"""Tests for :func:`sportsdataverse.nfl.build_nfl_player_stats`.

Two layers:

- An offline unit test on a tiny synthetic PBP frame (monkeypatching
  ``load_nfl_pbp`` / ``load_nfl_players``) that locks in the core derivations
  (completions, attempts, passing/receiving yards, targets, 2pt, fantasy).
- A ``@skip_if_no_live`` parity test that aggregates the real SDV-native 2023
  play-by-play and compares the key counting columns against the published
  nflverse ``load_nfl_player_stats`` baseline.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nfl import build_nfl_player_stats
from tests.conftest import skip_if_no_live


def _synthetic_pbp() -> pl.DataFrame:
    """Two-game-week toy PBP with one passer, one rusher, one receiver, one 2pt."""
    base = {
        "game_id": "2023_01_AAA_BBB",
        "season": 2023,
        "week": 1,
        "season_type": "REG",
        "posteam": "AAA",
        "defteam": "BBB",
        "td_team": None,
        "td_player_id": None,
        "td_player_name": None,
        "passer_player_id": None,
        "passer_player_name": None,
        "rusher_player_id": None,
        "rusher_player_name": None,
        "receiver_player_id": None,
        "receiver_player_name": None,
        "down": 1,
        "sp": 0,
        "two_point_attempt": 0,
        "complete_pass": 0,
        "incomplete_pass": 0,
        "interception": 0,
        "sack": 0,
        "fumble": 0,
        "fumble_lost": 0,
        "touchdown": 0,
        "pass_attempt": 0,
        "rush_attempt": 0,
        "qb_kneel": 0,
        "qb_spike": 0,
        "first_down_pass": 0,
        "first_down_rush": 0,
        "air_yards": 0,
        "yards_after_catch": 0,
        "passing_yards": 0,
        "rushing_yards": 0,
        "receiving_yards": 0,
        "yards_gained": 0,
        "epa": 0.0,
        "cpoe": None,
        "play_type": "pass",
        "special_teams_play_type": None,
    }

    def row(**kw):  # type: ignore[no-untyped-def]
        r = dict(base)
        r.update(kw)
        return r

    rows = [
        # QB completes a 20-air-yard pass (15 air + 5 YAC) to WR, first down
        row(
            play_type="pass",
            passer_player_id="QB1",
            passer_player_name="Q.B.",
            receiver_player_id="WR1",
            receiver_player_name="W.R.",
            complete_pass=1,
            pass_attempt=1,
            passing_yards=20,
            receiving_yards=20,
            air_yards=15,
            yards_after_catch=5,
            first_down_pass=1,
            epa=1.2,
            cpoe=3.0,
        ),
        # QB incomplete pass to same WR (a target, not a reception)
        row(
            play_type="pass",
            passer_player_id="QB1",
            passer_player_name="Q.B.",
            receiver_player_id="WR1",
            receiver_player_name="W.R.",
            incomplete_pass=1,
            pass_attempt=1,
            air_yards=10,
            epa=-0.5,
            cpoe=-2.0,
        ),
        # RB run for 7, first down
        row(
            play_type="run",
            rusher_player_id="RB1",
            rusher_player_name="R.B.",
            rush_attempt=1,
            rushing_yards=7,
            yards_gained=7,
            first_down_rush=1,
            epa=0.8,
        ),
        # successful 2pt rush by RB (down null, sp=1)
        row(
            play_type="run",
            down=None,
            two_point_attempt=1,
            sp=1,
            rusher_player_id="RB1",
            rusher_player_name="R.B.",
            rush_attempt=1,
            yards_gained=None,
        ),
    ]
    return pl.DataFrame(rows, infer_schema_length=None)


def _synthetic_players() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "gsis_id": ["QB1", "RB1", "WR1"],
            "display_name": ["Quarter Back", "Running Back", "Wide Receiver"],
            "short_name": ["Q.Back", "R.Back", "W.Rec"],
            "position": ["QB", "RB", "WR"],
            "position_group": ["QB", "RB", "WR"],
            "headshot": ["u1", "u2", "u3"],
        }
    )


def test_build_player_stats_synthetic(monkeypatch):
    """Core derivations on a controlled toy PBP frame."""
    import sportsdataverse.nfl.nfl_loaders as loaders

    monkeypatch.setattr(loaders, "load_nfl_pbp", lambda *a, **k: _synthetic_pbp())
    monkeypatch.setattr(loaders, "load_nfl_players", lambda *a, **k: _synthetic_players())

    df = build_nfl_player_stats([2023], summary_level="week")

    qb = df.filter(pl.col("player_id") == "QB1").to_dicts()[0]
    assert qb["completions"] == 1
    assert qb["attempts"] == 2
    assert qb["passing_yards"] == 20
    assert qb["passing_air_yards"] == 25  # 15 + 10
    assert qb["passing_yards_after_catch"] == 5  # (20 - 15) on completed catch
    assert qb["passing_first_downs"] == 1
    assert qb["position"] == "QB"

    rb = df.filter(pl.col("player_id") == "RB1").to_dicts()[0]
    assert rb["carries"] == 1
    assert rb["rushing_yards"] == 7
    assert rb["rushing_first_downs"] == 1
    assert rb["rushing_2pt_conversions"] == 1  # successful 2pt rush

    wr = df.filter(pl.col("player_id") == "WR1").to_dicts()[0]
    assert wr["targets"] == 2
    assert wr["receptions"] == 1
    assert wr["receiving_yards"] == 20
    assert wr["target_share"] == 1.0  # only receiver on the team-week

    # fantasy: RB = 0.7 (7 rush yds /10) + 2 (2pt) = 2.7
    assert abs(rb["fantasy_points"] - 2.7) < 1e-9


def test_build_player_stats_season_level(monkeypatch):
    """Season summary returns ``games`` and drops week/opponent columns."""
    import sportsdataverse.nfl.nfl_loaders as loaders

    monkeypatch.setattr(loaders, "load_nfl_pbp", lambda *a, **k: _synthetic_pbp())
    monkeypatch.setattr(loaders, "load_nfl_players", lambda *a, **k: _synthetic_players())

    df = build_nfl_player_stats([2023], summary_level="season")
    assert "games" in df.columns
    assert "week" not in df.columns
    assert "opponent_team" not in df.columns
    assert df.filter(pl.col("player_id") == "RB1")["games"][0] == 1


def test_build_player_stats_validates_args():
    import pytest

    with pytest.raises(ValueError):
        build_nfl_player_stats([2023], summary_level="bogus")
    with pytest.raises(ValueError):
        build_nfl_player_stats([2023], season_type="bogus")


@skip_if_no_live
def test_build_player_stats_parity_2023():
    """Parity vs published nflverse player_stats on 2023 REG counting columns."""
    import sportsdataverse.nfl as nfl

    wk = build_nfl_player_stats([2023], summary_level="week", season_type="REG")
    ref = nfl.load_nfl_player_stats().filter((pl.col("season") == 2023) & (pl.col("season_type") == "REG"))

    joined = wk.join(ref, on=["player_id", "week"], how="inner", suffix="_ref")
    # Row overlap should be effectively complete.
    assert joined.height >= int(0.98 * ref.height)

    # Exact-match counting columns.
    for col in ("completions", "attempts", "passing_yards", "passing_tds", "carries", "receptions", "targets"):
        a = joined[col].cast(pl.Float64)
        b = joined[col + "_ref"].cast(pl.Float64)
        exact = (a.round(3) == b.round(3)).sum()
        assert exact >= int(0.99 * joined.height), f"{col}: {exact}/{joined.height} exact"

    # EPA columns: high correlation (epa fallback for absent qb_epa).
    for col in ("passing_epa", "receiving_epa"):
        sub = joined.select([col, col + "_ref"]).drop_nulls()
        corr = sub.select(pl.corr(col, col + "_ref")).item()
        assert corr is not None and corr >= 0.9, f"{col} corr={corr}"
