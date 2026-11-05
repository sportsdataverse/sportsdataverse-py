# tools/fit_player_war.py
"""Fit the WAR replacement level and points per win per league, train/holdout disjoint (BB-D26).

The fitted values are frozen into ``sportsdataverse._common.player_war.REPLACEMENT_LEVEL`` and
``PTS_PER_WIN`` with a comment citing this script -- never tuned ad hoc.

* **Replacement level** ``R``: each season, the possession-weighted mean anchored box-BPM rating
  of the players who together played the league's least-used
  :data:`~sportsdataverse._common.player_war.REPLACEMENT_SHARE` of the possessions
  (:func:`~sportsdataverse._common.player_war.replacement_level_by_season`); ``R`` is the mean
  over the TRAIN seasons. The HOLDOUT seasons are scored by how far each season's level lands
  from ``R``.
* **Points per win**: ``wins - games / 2 = margin / pts_per_win``, least squares through the
  origin over the TRAIN team-seasons (BB-D22's qualifier: at least half the most games any team
  played, which keeps the college non-D1 opponents out). The HOLDOUT is scored by the mean
  absolute error in wins of ``games / 2 + margin / pts_per_win``.

Inputs: the player-seasons are the production function's own output (``rating_bpm`` and ``poss``
of :func:`~sportsdataverse.nba.nba_player_war.nba_player_war`, ``rating`` and ``poss`` of
:func:`~sportsdataverse.mbb.mbb_player_war.mbb_player_war`; neither depends on ``R`` or the
points per win), over the network. The team-season records are BB-D22's committed
``tests/fixtures/bb_team_luck/{lg}_season_records.parquet`` (final scores). ``--write`` saves the
player-seasons to ``tests/fixtures/bb_player_war/`` so the test suite re-runs this fit offline.

Usage::

    uv run python tools/fit_player_war.py --league nba --write
    uv run python tools/fit_player_war.py --league wbb
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse._common.player_war import replacement_level_by_season
from sportsdataverse._common.team_luck import QUALIFIER

#: league -> (train, holdout) seasons by END year, disjoint, the holdout after the train. The
#: replacement pool's level drifts up as rosters deepen (NBA about -2.8 per 100 before 2010, about
#: -1.9 since 2014), so the train span is the stretch just before the holdout, not every season:
#: the frozen level describes today's league. College box BPM covers every women's D1 team from
#: 2017 only (about 80 teams in 2014-2016).
SPANS: dict[str, tuple[range, range]] = {
    "nba": (range(2012, 2020), range(2020, 2027)),
    "wnba": (range(2010, 2018), range(2018, 2026)),
    "mbb": (range(2014, 2020), range(2020, 2027)),
    "wbb": (range(2017, 2022), range(2022, 2027)),
}
_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = _ROOT / "tests" / "fixtures" / "bb_player_war"
RECORDS = _ROOT / "tests" / "fixtures" / "bb_team_luck"


def fit_replacement_level(player_seasons: pl.DataFrame) -> float:
    """Mean of the seasons' replacement levels, rounded to 0.01 (the frozen precision)."""
    return round(float(replacement_level_by_season(player_seasons)["replacement_level"].mean()), 2)


def score_replacement_level(player_seasons: pl.DataFrame, level: float) -> tuple[float, float, int]:
    """(mean, max) absolute gap between each season's level and ``level``, and the season count."""
    gap = (replacement_level_by_season(player_seasons)["replacement_level"] - level).abs()
    return float(gap.mean()), float(gap.max()), gap.len()


def _margin_wins(records: pl.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    r = records.filter(QUALIFIER)
    margin = (r["pts"] - r["opp_pts"]).to_numpy().astype(float)
    return margin, r["wins"].to_numpy().astype(float), r["games"].to_numpy().astype(float)


def fit_pts_per_win(records: pl.DataFrame) -> float:
    """Points of season margin per win, least squares through the origin, rounded to 0.01."""
    margin, wins, games = _margin_wins(records)
    slope = float(margin @ (wins - games / 2) / (margin @ margin))
    return round(1.0 / slope, 2)


def score_pts_per_win(records: pl.DataFrame, pts_per_win: float) -> tuple[float, int]:
    """Mean absolute error in wins of ``games / 2 + margin / pts_per_win``, and the team-season count."""
    margin, wins, games = _margin_wins(records)
    return float(np.mean(np.abs(games / 2 + margin / pts_per_win - wins))), len(wins)


def player_seasons(league: str, seasons: range) -> pl.DataFrame:
    """``season``, ``poss`` and ``rating_bpm`` per player-season from the production function (network)."""
    if league in ("nba", "wnba"):
        from sportsdataverse.nba.nba_player_war import nba_player_war

        frames = [nba_player_war(s, league_id="00" if league == "nba" else "10") for s in seasons]
        cols = [pl.col("rating_bpm")]
    else:
        from sportsdataverse.mbb.mbb_player_war import mbb_player_war

        frames = [mbb_player_war(s, league="mens" if league == "mbb" else "womens") for s in seasons]
        cols = [pl.col("rating").alias("rating_bpm")]
    return _compact(pl.concat([f.select("season", "poss", *cols) for f in frames if f.height]))


def _compact(ps: pl.DataFrame) -> pl.DataFrame:
    """The fixture's precision: possessions to 0.1, ratings to 0.001, Float32 (the fit rounds to 0.01)."""
    return (
        ps.select(pl.col("season").cast(pl.Int16), pl.col("poss").round(1), pl.col("rating_bpm").round(3))
        .cast({"poss": pl.Float32, "rating_bpm": pl.Float32})
        .sort("season", "poss", "rating_bpm")
    )


def _in_span(df: pl.DataFrame, span: range) -> pl.DataFrame:
    return df.filter(pl.col("season").is_in(list(span)))


def main(argv: list[str] | None = None) -> int:
    """Fit one league from the network (or the committed fixture with ``--offline``) and print the holdout."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=sorted(SPANS), required=True)
    ap.add_argument("--write", action="store_true", help="save the player-seasons fixture")
    ap.add_argument("--offline", action="store_true", help="read the committed player-seasons fixture")
    args = ap.parse_args(argv)
    train, holdout = SPANS[args.league]
    path = FIXTURES / f"{args.league}_player_seasons.parquet"
    span = range(train.start, holdout.stop)
    ps = _compact(_in_span(pl.read_parquet(path), span)) if args.offline else player_seasons(args.league, span)
    if args.write:
        FIXTURES.mkdir(parents=True, exist_ok=True)
        ps.write_parquet(path, compression="zstd", compression_level=19)
    records = pl.read_parquet(RECORDS / f"{args.league}_season_records.parquet")
    level = fit_replacement_level(_in_span(ps, train))
    mean_gap, max_gap, n = score_replacement_level(_in_span(ps, holdout), level)
    ppw = fit_pts_per_win(_in_span(records, train))
    mae, n_teams = score_pts_per_win(_in_span(records, holdout), ppw)
    print(f"{args.league}: replacement level {level:+.2f} per 100 (train {train.start}-{train.stop - 1})")
    print(
        f"  holdout {holdout.start}-{holdout.stop - 1}: |season level - R| mean {mean_gap:.3f}, max {max_gap:.3f} ({n} seasons)"
    )
    print(f"  pts per win {ppw:.2f}; holdout MAE {mae:.3f} wins ({n_teams} team-seasons)")
    print(replacement_level_by_season(ps))
    return 0


if __name__ == "__main__":
    sys.exit(main())
