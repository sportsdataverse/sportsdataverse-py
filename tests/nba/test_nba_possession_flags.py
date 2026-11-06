"""BB-D7: garbage-time and transition flags on the NBA/WNBA possessions table.

Every test runs on committed real releases (``tests/fixtures/bb_possession_flags/README.md``):
the whole 2023-24 NBA regular season, the whole 2025 WNBA season, 30 games of the WNBA's
two-halves 2002 season, 40 games of 1999-00 and the one 2000-26 NBA game whose garbage time
starts in overtime. The transition oracle is stats.nba.com's Synergy ``Transition`` play type
for 2023-24 (``tests/fixtures/nba_playtype/synergy_off_team_2024.parquet``).

Pinned counts and floors are the observed values on those captures. Never lower a floor or
re-pin a count to make a test pass: a rule change that moves them is argued in its PR.
"""

from __future__ import annotations

import pathlib
from typing import Any

import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_flags import (
    POSSESSION_FLAGS_SCHEMA,
    TRANSITION_SECONDS,
    TRANSITION_START_TYPES,
    _running_score,
    add_possession_flags,
)

FIX = pathlib.Path(__file__).resolve().parents[1] / "fixtures"
D = FIX / "bb_possession_flags"
LINEUP = [f"{x}_player_{i}" for x in ("off", "def") for i in range(1, 6)]

#: Observed on the committed captures (2026-11-06, the 8-second window; 60,782 / 10,357 / 798 / 51
#: transition possessions at 10 s): rows, games, transition possessions, garbage-time possessions,
#: games with any garbage time.
OBSERVED = {
    "nba_2024": (242_618, 1_230, 45_615, 8_542, 398),
    "wnba_2025": (48_401, 310, 7_665, 1_357, 92),
    "wnba_2002": (4_405, 30, 611, 159, 15),
    "nba_2021_0022000983": (206, 1, 41, 2, 1),
}


def _load(key: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    lg, yr = key.split("_", 1)
    return pl.read_parquet(D / f"{lg}_possessions_{yr}.parquet"), pl.read_parquet(D / f"{lg}_pbp_{yr}.parquet")


@pytest.fixture(scope="module", params=list(OBSERVED))
def season(request: pytest.FixtureRequest) -> tuple[str, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    poss, pbp = _load(request.param)
    return request.param, poss, pbp, add_possession_flags(poss, pbp)


def _bigballr_garbage(poss: pl.DataFrame, pbp: pl.DataFrame) -> dict[tuple[str, int], bool]:
    """bigballR's garbage-time loop (all_functions.R:1016-1034), written event by event.

    An independent restatement of the college rule for the test: walk each game's events in
    order, keep the running score, test the margin bands and the starters clause on every
    event of a possession, and stay in garbage time once it starts.
    """
    out: dict[tuple[str, int], bool] = {}
    events_by_game = {g[0]: rows for g, rows in pbp.sort("order_index").group_by(["game_id"], maintain_order=True)}
    for (gid,), gp in poss.sort("possession_number").group_by(["game_id"], maintain_order=True):
        prows: list[dict[str, Any]] = gp.to_dicts()
        starters = {prows[0][c] for c in LINEUP}
        p1 = [r["start_seconds_remaining"] for r in prows if r["period"] == 1]
        reg = 2 if max(p1) > 900 else 4
        evs = events_by_game[gid].to_dicts()
        home = away = 0
        scores: dict[int, tuple[int, float]] = {}
        for e in evs:
            for side in ("score_home", "score_away"):
                try:
                    v = int(e[side])
                except (TypeError, ValueError):
                    continue
                if side == "score_home":
                    home = max(home, v)
                else:
                    away = max(away, v)
            scores[e["order_index"]] = (abs(home - away), e["seconds_remaining"])
        garbage = False
        for r in prows:
            n_starters = sum(r[c] in starters for c in LINEUP)
            for oi in range(r["start_order_index"], r["end_order_index"] + 1):
                if oi not in scores or n_starters > 3:
                    continue
                m, secs = scores[oi]
                if r["period"] > reg:
                    hit = m >= 15
                elif r["period"] == reg:
                    hit = (secs <= 600 and m >= 25) or (secs <= 300 and m >= 20) or (secs <= 120 and m >= 15)
                else:
                    hit = False
                garbage = garbage or hit
            out[(gid, r["possession_number"])] = garbage
    return out


def test_existing_columns_and_rows_unchanged(season: tuple[str, pl.DataFrame, pl.DataFrame, pl.DataFrame]) -> None:
    """The flags only append: same rows, same order, same values, three new typed columns."""
    _, poss, _, out = season
    assert out.height == poss.height
    assert out.columns == [*poss.columns, *POSSESSION_FLAGS_SCHEMA]
    assert out.select(poss.columns).equals(poss)
    assert {c: out.schema[c] for c in POSSESSION_FLAGS_SCHEMA} == POSSESSION_FLAGS_SCHEMA
    assert out["is_transition"].null_count() == 0
    assert out["is_garbage_time"].null_count() == 0


def test_observed_counts(season: tuple[str, pl.DataFrame, pl.DataFrame, pl.DataFrame]) -> None:
    """Pinned flag counts: a changed window, band, starters clause or start-type set moves them."""
    key, _, _, out = season
    got = (
        out.height,
        out["game_id"].n_unique(),
        out["is_transition"].sum(),
        out["is_garbage_time"].sum(),
        out.group_by("game_id").agg(pl.col("is_garbage_time").any())["is_garbage_time"].sum(),
    )
    assert got == OBSERVED[key]


def test_garbage_time_matches_the_bigballr_loop(season: tuple[str, pl.DataFrame, pl.DataFrame, pl.DataFrame]) -> None:
    """Every possession's flag equals the event-by-event college rule."""
    _, poss, pbp, out = season
    want = _bigballr_garbage(poss, pbp)
    got = dict(zip(zip(out["game_id"], out["possession_number"]), out["is_garbage_time"]))
    assert got == want


def test_garbage_time_is_sticky_and_late(season: tuple[str, pl.DataFrame, pl.DataFrame, pl.DataFrame]) -> None:
    """Never before the last regulation period; once a game is in garbage time it stays there."""
    key, _, _, out = season
    g = out.sort("game_id", "possession_number")
    last_reg = 2 if key == "wnba_2002" else 4
    assert g.filter(pl.col("is_garbage_time") == True)["period"].min() >= last_reg  # noqa: E712
    after = g.with_columns(
        pl.col("is_garbage_time").cast(pl.Int8).cum_max().over("game_id").cast(pl.Boolean).alias("_seen")
    )
    assert after["is_garbage_time"].equals(after["_seen"], check_names=False)


def test_overtime_and_halves_branches() -> None:
    """2020-21 0022000983 enters garbage time in overtime; the 2002 WNBA halves games in period 2."""
    poss, pbp = _load("nba_2021_0022000983")
    ot = add_possession_flags(poss, pbp).filter(pl.col("is_garbage_time") == True)  # noqa: E712
    assert ot["period"].unique().to_list() == [5]
    poss, pbp = _load("wnba_2002")
    halves = add_possession_flags(poss, pbp).filter(pl.col("is_garbage_time") == True)  # noqa: E712
    assert halves["period"].unique().to_list() == [2]


def test_row_order_does_not_change_the_flags() -> None:
    """A shuffled frame gets the same flag on every possession (the work sorts internally)."""
    poss, pbp = _load("wnba_2025")
    key = ["game_id", "possession_number"]
    base = add_possession_flags(poss, pbp).select(*key, *POSSESSION_FLAGS_SCHEMA)
    shuffled = add_possession_flags(poss.sample(fraction=1.0, shuffle=True, seed=7), pbp.reverse())
    assert shuffled.select(*key, *POSSESSION_FLAGS_SCHEMA).sort(key).equals(base.sort(key))


def test_transition_definition_holds_row_by_row(season: tuple[str, pl.DataFrame, pl.DataFrame, pl.DataFrame]) -> None:
    """Transition = a live start, not the period's first possession, first FGA within the window."""
    _, _, _, out = season
    trans = out.filter(pl.col("is_transition") == True)  # noqa: E712
    assert trans["possession_start_type"].is_in(TRANSITION_START_TYPES).all()
    assert (trans["number_in_period"] > 1).all()
    assert (trans["seconds_to_first_shot"] <= TRANSITION_SECONDS).all()
    assert out["seconds_to_first_shot"].min() >= 0
    missed = out.filter(
        pl.col("possession_start_type").is_in(TRANSITION_START_TYPES)
        & (pl.col("number_in_period") > 1)
        & (pl.col("seconds_to_first_shot") <= TRANSITION_SECONDS)
        & (pl.col("is_transition") == False)  # noqa: E712
    )
    assert missed.is_empty()


def test_seconds_to_first_shot_counts_from_the_change_of_possession() -> None:
    """Pinned values from 2023-24 0022300100 (SAC v LAL), read off the play-by-play by hand.

    Possession 3 (LAL) starts when Fox's layup goes in at 11:40 and Reaves misses at 11:24:
    16 s, though its first row is the miss itself. Possession 6 (SAC) starts on Fox's
    defensive rebound at 10:51 and Sabonis scores at 10:48: 3 s, transition. Possession 1 is
    the opening tip at 12:00 with no field-goal attempt before the turnover: null.
    """
    poss, pbp = _load("nba_2024")
    out = add_possession_flags(poss, pbp).filter(pl.col("game_id") == "0022300100")
    got = {
        r["possession_number"]: (r["seconds_to_first_shot"], r["is_transition"])
        for r in out.filter(pl.col("possession_number").is_in([1, 3, 6])).to_dicts()
    }
    assert got == {1: (None, False), 3: (16.0, False), 6: (3.0, True)}


def test_transition_tracks_synergy_2024() -> None:
    """Team transition possessions per game track Synergy's ``Transition`` play type for 2023-24.

    Observed at the 8-second window: Pearson r 0.892 and Spearman 0.860 over the 30 teams; 18.4
    a game against Synergy's 19.1 (ratio 0.97). At 10 s it was r 0.844, Spearman 0.788 and 24.6
    a game (ratio 1.29), which is why the window is 8 (``TRANSITION_SECONDS``). The level still
    differs a little because Synergy tags a possession by how it was finished (a transition
    possession that ends in a putback or a later half-court set is not ``Transition``), while
    this flag tags every live start whose first shot comes inside the window. The gate is the
    team ordering, with the ratio bounded.

    2023-24 is one of the seasons the window was chosen on (``tools/models/nba_transition_window.csv``
    holds all eleven), so this pins the choice rather than validating it. The ratio band is what
    separates 8 s from 6 s (r 0.909, Spearman 0.892, but a ratio of 0.65).
    """
    poss, pbp = _load("nba_2024")
    out = add_possession_flags(poss, pbp)
    team = (
        out.filter(pl.col("count_as_possession") == True)  # noqa: E712
        .group_by("offense_team_id")
        .agg((pl.col("is_transition").sum() / pl.col("game_id").n_unique()).alias("tr_pg"))
    )
    syn = (
        pl.read_parquet(FIX / "nba_playtype" / "synergy_off_team_2024.parquet")
        .filter(pl.col("play_type") == "Transition")
        .select(pl.col("team_id").alias("offense_team_id"), pl.col("poss").alias("syn_pg"))
    )
    assert team.schema["offense_team_id"] == syn.schema["offense_team_id"]
    m = team.join(syn, on="offense_team_id", how="inner")
    assert m.height == 30
    assert m.select(pl.corr("tr_pg", "syn_pg")).item() >= 0.89
    assert m.select(pl.corr("tr_pg", "syn_pg", method="spearman")).item() >= 0.85
    assert 0.93 <= m["tr_pg"].mean() / m["syn_pg"].mean() <= 1.00


def test_running_score_survives_the_old_zero_filled_feed() -> None:
    """1999-00 pbp writes "0" on every non-scoring row; the running score still ends at the final."""
    pbp = (
        pl.read_parquet(D / "nba_pbp_scores_2000.parquet")
        .sort("game_id", "order_index")
        .with_columns(_running_score("score_home").alias("h"), _running_score("score_away").alias("a"))
    )
    # A game's score never falls: the "0" rows must not read as a 0-0 game mid-quarter.
    assert (pbp.select((pl.col("h") + pl.col("a")).diff().over("game_id").drop_nulls() >= 0).to_series()).all()
    last = pbp.group_by("game_id").agg(pl.col("h").last(), pl.col("a").last())
    sched = pl.read_parquet(D / "nba_schedule_2000.parquet")
    assert last.schema["game_id"] == sched.schema["game_id"]
    m = last.join(sched, on="game_id", how="inner")
    assert m.height == 40
    assert (m["h"] == m["home_pts"]).all() and (m["a"] == m["away_pts"]).all()


def test_empty_and_bad_inputs() -> None:
    poss, pbp = _load("wnba_2025")
    empty = add_possession_flags(poss.head(0), pbp.head(0))
    assert empty.height == 0 and empty.columns == [*poss.columns, *POSSESSION_FLAGS_SCHEMA]
    with pytest.raises(ValueError, match="missing columns"):
        add_possession_flags(poss.drop("off_player_3"), pbp)
    with pytest.raises(ValueError, match="already has"):
        add_possession_flags(add_possession_flags(poss, pbp), pbp)
    with pytest.raises(ValueError, match="game_id dtype"):
        add_possession_flags(poss, pbp.with_columns(pl.col("game_id").cast(pl.Int64)))
    one_game = poss["game_id"][0]
    with pytest.raises(ValueError, match="no events"):
        add_possession_flags(poss, pbp.filter(pl.col("game_id") != one_game))


def test_unknown_lineup_never_passes_the_starters_clause() -> None:
    """A possession with a null on-floor slot cannot count 0 starters and trigger garbage time."""
    poss, pbp = _load("wnba_2025")
    blind = poss.with_columns(pl.lit(None, dtype=pl.Int64).alias("def_player_5"))
    out = add_possession_flags(blind, pbp)
    assert out["is_garbage_time"].sum() == 0
    assert out["is_transition"].sum() == OBSERVED["wnba_2025"][2]
