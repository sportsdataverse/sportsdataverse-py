"""BB-D6: NBA / WNBA game impact box and lineups used, on committed real captures.

Fixtures (``tests/fixtures/bb_game_impact``, README there; none synthetic): 16 games each of the
2025-26 NBA and 2026 WNBA seasons from the ``*_stats_pbp``, ``*_stats_game_lineups``,
``*_stats_possessions`` and ``*_stats_schedules`` releases, plus stats.nba.com's player box
score (``plus_minus_points``), the +/- oracle.

The gates are the values observed on these games; never lower one to make a change pass. The NBA
misses are on-court lineup errors in the source (the 2025-26 lineups are mostly inferred from
substitutions): among player-games whose minutes agree with the box score, all but one match. The
``bb-game-impact`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse._common.game_impact import (
    game_lineups_used,
    game_lineups_used_schema,
    game_player_impact,
    game_player_impact_schema,
)
from sportsdataverse.nba import nba_loaders
from sportsdataverse.nba.nba_game_impact import nba_game_lineups_used, nba_game_player_impact, nba_impact_rows
from sportsdataverse.wnba import wnba_loaders
from sportsdataverse.wnba.wnba_game_impact import wnba_game_player_impact

IMPACT = Path(__file__).resolve().parents[1] / "fixtures" / "bb_game_impact"
LEAGUES = {"nba": (2025, "00", 2880.0), "wnba": (2026, "10", 2400.0)}
#: Observed on the fixture games: (player-games whose +/- equals the box, player-games in the box).
OBSERVED_EXACT = {"nba": (305, 331), "wnba": (303, 309)}
#: The same among player-games whose minutes are within 6 s of the box (the lineup is right).
OBSERVED_RIGHT = {"nba": (296, 297), "wnba": (303, 309)}
#: Games with a possession or action whose lineup lists fewer than five distinct players.
SHORT_LINEUP_GAMES = {"nba": {"0022500559", "0022500632"}, "wnba": set()}
#: Observed impact rows with no box-score row (a player the source lineup puts on the floor wrongly).
OBSERVED_PHANTOMS = {"nba": 0, "wnba": 0}
_SLOTS = [f"{side}_{i}" for side in ("home", "away") for i in range(1, 6)]


def _read(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(IMPACT / f"{lg}_{name}_{LEAGUES[lg][0]}.parquet")


def _rows(lg: str) -> pl.DataFrame:
    return nba_impact_rows(
        _read(lg, "pbp"),
        _read(lg, "game_lineups"),
        _read(lg, "possessions"),
        _read(lg, "schedule"),
        league_id=LEAGUES[lg][1],
    )


def _box(lg: str) -> pl.DataFrame:
    """The stats.nba.com box rows of players who played, with their minutes as a float."""
    mins = pl.col("minutes").str.split(":")
    return (
        _read(lg, "player_box")
        .filter(pl.col("minutes").is_not_null() & (pl.col("minutes") != ""))
        .select(
            "game_id",
            "team_id",
            pl.col("person_id").alias("player_id"),
            pl.col("plus_minus_points").cast(pl.Int64).alias("box_plus_minus"),
            (mins.list.get(0).cast(pl.Float64) + mins.list.get(1).cast(pl.Float64) / 60).alias("box_minutes"),
        )
    )


def _short_games(rows: pl.DataFrame) -> set[str]:
    """Games with a row whose side lists fewer than five distinct players."""
    sides = [pl.concat_list(_SLOTS[:5]), pl.concat_list(_SLOTS[5:])]
    short = pl.any_horizontal(side.list.drop_nulls().list.n_unique() < 5 for side in sides)
    return set(rows.filter(short)["game_id"].unique().to_list())


def _margins(lg: str) -> pl.DataFrame:
    s = _read(lg, "schedule")
    return pl.concat(
        [
            s.select(
                "game_id",
                pl.col("home_team_id").alias("team_id"),
                (pl.col("home_pts") - pl.col("away_pts")).alias("margin"),
            ),
            s.select(
                "game_id",
                pl.col("away_team_id").alias("team_id"),
                (pl.col("away_pts") - pl.col("home_pts")).alias("margin"),
            ),
        ]
    )


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_plus_minus_matches_the_box_score(lg: str) -> None:
    impact = game_player_impact(_rows(lg), player_dtype=pl.Int64())
    box = _box(lg)
    for k in ("game_id", "team_id", "player_id"):
        assert box.schema[k] == impact.schema[k], k
    joined = box.join(impact, on=["game_id", "team_id", "player_id"], how="left")
    phantoms = impact.join(box, on=["game_id", "team_id", "player_id"], how="anti").height
    assert phantoms <= OBSERVED_PHANTOMS[lg]
    exact = int((joined["plus_minus"] == joined["box_plus_minus"]).sum())
    floor, n = OBSERVED_EXACT[lg]
    assert joined.height == n
    assert exact >= floor, f"{lg}: {exact}/{n} player-games match the box +/-, observed {floor}"
    # Where the source lineup is right (minutes within 6 s of the box), the +/- is exact.
    right = joined.filter((pl.col("minutes") - pl.col("box_minutes")).abs() <= 0.1)
    right_floor, right_n = OBSERVED_RIGHT[lg]
    assert right.height == right_n
    assert int((right["plus_minus"] == right["box_plus_minus"]).sum()) >= right_floor


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_team_sums_are_five_times_the_team_totals(lg: str) -> None:
    rows = _rows(lg)
    impact = game_player_impact(rows, player_dtype=pl.Int64())
    team = impact.group_by("game_id", "team_id").agg(pl.col("plus_minus", "poss", "opp_poss", "seconds").sum())
    team = team.join(_margins(lg), on=["game_id", "team_id"], how="inner", validate="1:1")
    poss = (
        _read(lg, "possessions")
        .group_by("game_id", pl.col("offense_team_id").alias("team_id"))
        .len("team_poss")
        .join(
            _read(lg, "possessions")
            .group_by("game_id", pl.col("defense_team_id").alias("team_id"))
            .len("team_opp_poss"),
            on=["game_id", "team_id"],
        )
    )
    team = team.join(poss, on=["game_id", "team_id"], how="inner", validate="1:1")
    assert team.height == 32
    assert _short_games(rows) == SHORT_LINEUP_GAMES[lg]
    clean = team.filter(~pl.col("game_id").is_in(list(SHORT_LINEUP_GAMES[lg])))
    assert clean.height >= 28
    assert (clean["plus_minus"] == 5 * clean["margin"]).all()
    assert (clean["poss"] == 5 * clean["team_poss"]).all()
    assert (clean["opp_poss"] == 5 * clean["team_opp_poss"]).all()
    # The action clock tiles each period: five players' seconds are five times the game length.
    regulation = LEAGUES[lg][2]
    overtimes = (clean["seconds"] / 5 - regulation) / 300
    assert ((overtimes - overtimes.round(0)).abs() < 1e-6).all() and (overtimes >= 0).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_ratings_and_sample_sizes(lg: str) -> None:
    impact = game_player_impact(_rows(lg), player_dtype=pl.Int64())
    assert impact.schema == pl.Schema(game_player_impact_schema(pl.Int64()))
    on = impact.filter(pl.col("poss") > 0)
    assert ((on["off_rtg"] - 100 * on["pts"] / on["poss"]).abs() < 1e-9).all()
    against = impact.filter(pl.col("opp_poss") > 0)
    assert ((against["def_rtg"] - 100 * against["opp_pts"] / against["opp_poss"]).abs() < 1e-9).all()
    both = impact.filter((pl.col("poss") > 0) & (pl.col("opp_poss") > 0))
    assert ((both["net_rtg"] - (both["off_rtg"] - both["def_rtg"])).abs() < 1e-9).all()
    assert (impact["off_rtg_n"] == impact["poss"]).all()
    assert (impact["def_rtg_n"] == impact["opp_poss"]).all()
    assert (impact["net_rtg_n"] == impact["poss"] + impact["opp_poss"]).all()
    assert impact.filter(pl.col("poss") == 0)["off_rtg"].null_count() == impact.filter(pl.col("poss") == 0).height
    assert (impact["plus_minus"] == impact["pts"] - impact["opp_pts"]).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_lineups_used_add_up_to_the_game(lg: str) -> None:
    rows = _rows(lg)
    units = game_lineups_used(rows, player_dtype=pl.Int64())
    assert units.schema == pl.Schema(game_lineups_used_schema(pl.Int64()))
    assert units.select(pl.col(f"player_{i}").null_count() for i in range(1, 6)).sum_horizontal().item() == 0
    first = units.select(pl.concat_list([f"player_{i}" for i in range(1, 6)]).alias("p"))["p"]
    assert (first.list.n_unique() == 5).all() and (first.list.sort() == first).all()
    assert (units["lineup_id"] == first.cast(pl.List(pl.Utf8)).list.join("-")).all()
    players = game_player_impact(rows, player_dtype=pl.Int64()).group_by("game_id", "team_id").agg(pl.col("poss").sum())
    team = (
        units.group_by("game_id", "team_id")
        .agg(pl.col("plus_minus", "poss").sum())
        .join(_margins(lg), on=["game_id", "team_id"])
        .join(players, on=["game_id", "team_id"], suffix="_players")
    )
    clean = team.filter(~pl.col("game_id").is_in(list(SHORT_LINEUP_GAMES[lg])))
    assert clean.height >= 28
    assert (clean["plus_minus"] == clean["margin"]).all()
    assert (5 * clean["poss"] == clean["poss_players"]).all()


def test_public_builders_read_the_start_year(monkeypatch: pytest.MonkeyPatch) -> None:
    def fixture(name: str):  # type: ignore[no-untyped-def]
        def load(seasons: list[int]) -> pl.DataFrame:
            assert list(seasons) == [2025], f"{name}: called with {seasons}"
            return _read("nba", name)

        return load

    for loader, name in (
        ("load_nba_stats_pbp", "pbp"),
        ("load_nba_stats_game_lineups", "game_lineups"),
        ("load_nba_stats_possessions", "possessions"),
        ("load_nba_stats_schedules", "schedule"),
    ):
        monkeypatch.setattr(nba_loaders, loader, fixture(name))
    impact = nba_game_player_impact(2026)
    units = nba_game_lineups_used(2026)
    assert impact.height == game_player_impact(_rows("nba"), player_dtype=pl.Int64()).height
    assert units["game_id"].n_unique() == 16


def test_possessions_need_play_by_play_and_a_side() -> None:
    pbp, poss = _read("nba", "pbp"), _read("nba", "possessions")
    no_pbp, no_side = "0022500013", "0022500136"
    rows = nba_impact_rows(
        pbp.filter(pl.col("game_id") != no_pbp),
        _read("nba", "game_lineups"),
        poss.with_columns(
            pl.when(pl.col("game_id") == no_side).then(0).otherwise(pl.col("offense_team_id")).alias("offense_team_id")
        ),
        _read("nba", "schedule"),
    )
    impact = game_player_impact(rows, player_dtype=pl.Int64())
    units = game_lineups_used(rows, player_dtype=pl.Int64())
    # a game with possessions but no play-by-play publishes nothing (not poss with zero points)
    assert no_pbp not in set(impact["game_id"]) and no_pbp not in set(units["game_id"])
    # possessions whose offense is neither team are dropped, never credited to the away side
    game = impact.filter(pl.col("game_id") == no_side)
    assert game.height > 0 and game["poss"].sum() == 0 and game["opp_poss"].sum() == 0
    assert impact.filter(pl.col("game_id") != no_side)["poss"].sum() > 0


def test_wnba_builder_reads_the_calendar_year(monkeypatch: pytest.MonkeyPatch) -> None:
    def fixture(name: str):  # type: ignore[no-untyped-def]
        def load(seasons: list[int]) -> pl.DataFrame:
            assert list(seasons) == [2026], f"{name}: called with {seasons}"
            return _read("wnba", name)

        return load

    for loader, name in (
        ("load_wnba_stats_pbp", "pbp"),
        ("load_wnba_stats_game_lineups", "game_lineups"),
        ("load_wnba_stats_possessions", "possessions"),
        ("load_wnba_stats_schedules", "schedule"),
    ):
        monkeypatch.setattr(wnba_loaders, loader, fixture(name))
    impact = wnba_game_player_impact(2026)
    assert_frame_equal(impact, game_player_impact(_rows("wnba"), player_dtype=pl.Int64()))


def test_empty_input_and_bad_league() -> None:
    empty = nba_impact_rows(pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
    assert game_player_impact(empty, player_dtype=pl.Int64()).schema == pl.Schema(game_player_impact_schema(pl.Int64()))
    assert game_lineups_used(empty, player_dtype=pl.Int64()).height == 0
    with pytest.raises(ValueError, match="league_id"):
        nba_impact_rows(pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), league_id="20")
