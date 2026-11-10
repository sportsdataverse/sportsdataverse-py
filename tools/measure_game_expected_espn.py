"""Measure BB-D40's ESPN-located expected points against BB-D9's feed-located totals (WNBA 2025).

Fetches the first final game of every other day of June-August 2025 from ESPN (Site v2 summary),
matches each to its stats.wnba.com game in ``tests/fixtures/bb_game_expected`` by date and the two
teams' (FGA, field-goal points), scores both with the same as-of cells and writes
``tests/fixtures/bb_game_expected/espn_vs_d9_wnba_2025.csv``, the table the ESPN tolerance in
``tests/nba/test_nba_game_expected.py`` is set from. Network: ESPN only.

    uv run python tools/measure_game_expected_espn.py
"""

from __future__ import annotations

import datetime as dt
import json
import urllib.request
from pathlib import Path

import polars as pl

from sportsdataverse.nba import nba_game_page as gp
from sportsdataverse.nba.nba_game_expected import shot_cells, shot_xfg
from sportsdataverse.wnba.wnba_pbp import espn_wnba_pbp

FIX = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "bb_game_expected"
SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?dates="


def history() -> pl.DataFrame:
    """The fixture's 2024-25 WNBA shots, dated by the game logs (BB-D9's source rows)."""
    dates = (
        pl.read_parquet(FIX / "wnba_game_logs_2024_2025.parquet")
        .with_columns(pl.col("game_date").str.slice(0, 10).str.to_date())
        .group_by("season", "game_id")
        .agg(pl.col("game_date").min())
    )
    shots = pl.read_parquet(FIX / "wnba_shots_2024_2025.parquet")
    return shots.join(dates, on=["season", "game_id"]).with_columns(made=pl.col("shot_result") == "Made")


def main() -> None:
    h = history()
    on_court = (pl.col("x_legacy").abs() <= 250) & (pl.col("y_legacy") >= -52.5)
    pts = pl.when(pl.col("made")).then(pl.col("shot_value")).otherwise(0)
    st = (
        h.filter(pl.col("season") == 2025)
        .group_by("game_id", "game_date", "team_id")
        .agg(pl.len().alias("fga"), pts.sum().alias("fg_pts"))
        .group_by("game_id", "game_date")
        .agg(pl.col("fga"), pl.col("fg_pts"))
    )
    rows, day = [], dt.date(2025, 6, 1)
    while day <= dt.date(2025, 8, 31):
        with urllib.request.urlopen(SCOREBOARD + day.strftime("%Y%m%d")) as resp:  # noqa: S310 - fixed ESPN host
            events = json.load(resp)["events"][:1]
        for e in events:
            if e["competitions"][0]["status"]["type"]["name"] != "STATUS_FINAL":
                continue
            final = espn_wnba_pbp(game_id=int(e["id"]), return_parsed=False)
            team = gp.game_page_frames(final, league="wnba")["team_game_expected"]
            key = sorted(zip(team["fga"].to_list(), team["fg_pts"].to_list(), strict=True))
            utc = dt.date.fromisoformat(e["date"][:10])
            match = [
                r
                for r in st.filter(pl.col("game_date").is_in([utc, utc - dt.timedelta(days=1)])).iter_rows(named=True)
                if sorted(zip(r["fga"], r["fg_pts"], strict=True)) == key
            ]
            if len(match) != 1:
                print(f"{e['id']}: no single stats game matches {key}")
                continue
            cells = shot_cells(h, match[0]["game_date"])
            espn = gp.game_page_frames(final, league="wnba", shot_cells=cells)["team_game_expected"]
            g = h.filter(pl.col("game_id") == match[0]["game_id"], on_court)
            d9 = float((shot_xfg(g, cells, league="wnba")["xfg"] * g["shot_value"]).sum())
            rows.append(
                {
                    "espn_game_id": int(e["id"]),
                    "stats_game_id": match[0]["game_id"],
                    "fga": int(espn["fga"].sum()),
                    "espn_xpts": round(float(espn["xfg_pts"].sum()), 6),
                    "d9_xpts": round(d9, 6),
                }
            )
        day += dt.timedelta(days=2)
    pl.DataFrame(rows).write_csv(FIX / "espn_vs_d9_wnba_2025.csv")
    print(f"{len(rows)} games")


if __name__ == "__main__":
    main()
