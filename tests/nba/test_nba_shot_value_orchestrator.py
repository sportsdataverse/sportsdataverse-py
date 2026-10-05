"""Tests for the nba_shot_value orchestrator + lineup variant (offline)."""

import polars as pl

import sportsdataverse.nba.nba_stats as nba_stats
from sportsdataverse.nba.nba_shot_value import nba_shot_value, nba_shot_value_lineups


def _raw_from_frames(frames: "dict[str, pl.DataFrame]") -> dict:
    """Build a stats.nba.com ``resultSets`` envelope from named frames."""
    return {
        "resultSets": [
            {"name": name, "headers": df.columns, "rowSet": [list(r) for r in df.iter_rows()]}
            for name, df in frames.items()
        ]
    }


def test_orchestrator_returns_five_models(monkeypatch, shot_value_corpus):
    raw = _raw_from_frames(
        {"Shot_Chart_Detail": shot_value_corpus["shots"], "LeagueAverages": shot_value_corpus["league_avgs"]}
    )
    monkeypatch.setattr(nba_stats, "nba_stats_shotchartdetail", lambda **kw: raw)
    out = nba_shot_value([201939], "2022-23")
    assert set(out) == {"shots", "talent", "selection", "zones"}
    assert "xpoints" in out["shots"].columns and out["shots"].height > 1000
    assert out["talent"].height > 0 and out["zones"].height > 0


def test_orchestrator_pandas(monkeypatch, shot_value_corpus):
    raw = _raw_from_frames(
        {"Shot_Chart_Detail": shot_value_corpus["shots"], "LeagueAverages": shot_value_corpus["league_avgs"]}
    )
    monkeypatch.setattr(nba_stats, "nba_stats_shotchartdetail", lambda **kw: raw)
    out = nba_shot_value([201939], "2022-23", return_as_pandas=True)
    assert type(out["talent"]).__module__.startswith("pandas")


def test_orchestrator_empty_fetch(monkeypatch):
    monkeypatch.setattr(nba_stats, "nba_stats_shotchartdetail", lambda **kw: {"resultSets": []})
    out = nba_shot_value([1], "2022-23")
    assert out["shots"].height == 0 and out["talent"].height == 0


def test_lineups_scored(monkeypatch, shot_value_corpus):
    lineup_shots = shot_value_corpus["shots"].head(200).with_columns(pl.lit("201939-202691").alias("group_id"))
    raw = _raw_from_frames({"ShotChartLineupDetail": lineup_shots, "LeagueAverages": shot_value_corpus["league_avgs"]})
    monkeypatch.setattr(nba_stats, "nba_stats_shotchartlineupdetail", lambda **kw: raw)
    df = nba_shot_value_lineups("201939-202691", "2022-23", team_id=1610612744)
    assert "xpoints" in df.columns and df.schema["group_id"] == pl.Utf8 and df.height == 200


def test_lineups_empty(monkeypatch):
    monkeypatch.setattr(nba_stats, "nba_stats_shotchartlineupdetail", lambda **kw: {"resultSets": []})
    df = nba_shot_value_lineups("x", "2022-23", team_id=1)
    assert df.height == 0 and "xpoints" in df.columns


def _ptshots_raw(sample: pl.DataFrame) -> dict:
    """A playerdashptshots envelope built from the committed sample (two of its result sets)."""
    cols = {"ClosestDefenderShooting": "CLOSE_DEF_DIST_RANGE", "ShotClockShooting": "SHOT_CLOCK_RANGE"}
    return _raw_from_frames(
        {
            rs: sample.filter(pl.col("result_set") == rs).select(
                pl.col("bucket").alias(col), pl.col("fga").alias("FGA"), pl.col("fgm").alias("FGM")
            )
            for rs, col in cols.items()
        }
    )


def test_context_asks_for_each_players_own_team(monkeypatch, shot_value_corpus):
    # PR #693 review: the wrapper's default TeamID belongs to its default player, so a context call
    # without the shooter's team filtered every other player out (and TeamID "0" is HTTP 500).
    tatum = shot_value_corpus["shots"].filter(pl.col("player_id") == 1628369)  # Boston Celtics
    traded = tatum.head(50).with_columns(pl.lit(1610612737, dtype=pl.Int64).alias("team_id"))  # as if some with Atlanta
    raw = _raw_from_frames(
        {"Shot_Chart_Detail": pl.concat([tatum, traded]), "LeagueAverages": shot_value_corpus["league_avgs"]}
    )
    monkeypatch.setattr(nba_stats, "nba_stats_shotchartdetail", lambda **kw: raw)
    asked = []

    def ptshots(**kw):
        asked.append((kw["player_id"], kw.get("team_id")))
        return _ptshots_raw(shot_value_corpus["ptshots"])

    monkeypatch.setattr(nba_stats, "nba_stats_playerdashptshots", ptshots)
    out = nba_shot_value([1628369], "2022-23", include_context=True)
    assert asked == [("1628369", "1610612737"), ("1628369", "1610612738")]
    assert out["context"].height > 0
