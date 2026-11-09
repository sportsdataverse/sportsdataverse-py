"""BB-D7's transition window against Synergy's ``Transition`` play type, every season Synergy covers.

Run (a hoopR-nba-stats-data checkout: its committed ``nba_stats/{possessions,pbp,synergy}/parquet``)::

    uv run python tools/models/nba_transition_window.py <hoopR-nba-stats-data>/nba_stats

For each regular season 2015-16 to 2025-26 (END years 2016-2026) and each window (6, 8, 10 s) it flags
the season's possessions with :func:`~sportsdataverse.nba.nba_possession_flags.add_possession_flags` and
compares the teams' transition possessions with Synergy's: the league share of possessions flagged,
Pearson and Spearman over the 30 teams, and our total over Synergy's. The committed Synergy cube is
player-level, so a team's Synergy count is its players' rows summed (low-minute players are missing,
so it is a floor). 2023-24's player rows hold only 37,388 of the team table's ~46,900 possessions, so
that season is read from the team-level capture in ``tests/fixtures/nba_playtype`` instead (per game).
The numbers behind ``TRANSITION_SECONDS`` are this script's output on 2026-11-06, written beside it
as ``nba_transition_window.csv``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_possession_flags import POSSESSION_FLAGS_SCHEMA, add_possession_flags

SEASONS = range(2016, 2027)
WINDOWS = (6.0, 8.0, 10.0)
TEAM_LEVEL = {2024: Path(__file__).resolve().parents[2] / "tests/fixtures/nba_playtype/synergy_off_team_2024.parquet"}


def synergy(data: Path, season: int) -> tuple[pl.DataFrame, float, bool]:
    """``team_id -> syn`` transition possessions, Synergy's league transition share, and whether ``syn`` is a game's."""
    d = data / "synergy" / "parquet"
    if season in TEAM_LEVEL:
        t = pl.read_parquet(TEAM_LEVEL[season]).filter(pl.col("play_type") == "Transition")
        return t.select("team_id", pl.col("poss").alias("syn")), float(t["freq"].mean()), True
    tr = pl.read_parquet(d / f"regular-season_transition_offensive_totals_{season}.parquet")
    every = pl.concat(
        pl.read_parquet(f, columns=["poss"]) for f in d.glob(f"regular-season_*_offensive_totals_{season}.parquet")
    )
    team = tr.group_by("team_id").agg(pl.col("poss").sum().alias("syn"))
    return team, float(tr["poss"].sum() / every["poss"].sum()), False


def season_rows(data: Path, season: int) -> list[dict[str, float]]:
    poss = pl.read_parquet(data / "possessions" / "parquet" / f"nba_possessions_{season}.parquet")
    pbp = pl.read_parquet(data / "pbp" / "parquet" / f"nba_play_by_play_{season}.parquet")
    poss = poss.drop(*POSSESSION_FLAGS_SCHEMA, strict=False)
    syn, syn_share, per_game = synergy(data, season)
    out = []
    for w in WINDOWS:
        rs = add_possession_flags(poss, pbp, transition_seconds=w).filter(
            (pl.col("game_id").str.slice(2, 1) == "2") & (pl.col("count_as_possession") == True)  # noqa: E712
        )
        flag = pl.col("is_transition")
        ours = rs.group_by(pl.col("offense_team_id").alias("team_id")).agg(
            (flag.sum() / pl.col("game_id").n_unique()).alias("ours"), pl.col("game_id").n_unique().alias("g")
        )
        assert ours.schema["team_id"] == syn.schema["team_id"]
        # a game's possessions on both sides, so a bubble or lockout season's uneven game counts cancel
        m = ours.join(syn, on="team_id", how="inner").with_columns(
            pl.col("syn") if per_game else pl.col("syn") / pl.col("g")
        )
        out.append(
            {
                "season": season,
                "window": w,
                "teams": m.height,
                "share": rs.select(flag.mean()).item(),
                "synergy_share": syn_share,
                "pearson": m.select(pl.corr("ours", "syn")).item(),
                "spearman": m.select(pl.corr("ours", "syn", method="spearman")).item(),
                "ratio": m["ours"].sum() / m["syn"].sum(),
            }
        )
    return out


if __name__ == "__main__":
    data = Path(sys.argv[1])
    res = pl.DataFrame([r for s in SEASONS for r in season_rows(data, s)])
    res.write_csv(Path(__file__).with_suffix(".csv"), float_precision=6)
    with pl.Config(tbl_rows=100, tbl_cols=20):
        print(res)
        print(res.group_by("window").agg(pl.exclude("season", "teams").mean()).sort("window"))
