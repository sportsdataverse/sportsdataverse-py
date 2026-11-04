"""BB-D6: college game impact box and lineups used, on committed real captures.

Fixtures (``tests/fixtures/bb_game_impact``, README there; none synthetic): 12 D1-vs-D1 games
each of the 2024-25 ``ncaa_{mbb,wbb}_pbp`` and ``ncaa_{mbb,wbb}_possessions`` releases (plus one
men's D1-vs-non-D1 game), and the same pipeline's team box (final points). Neither the ESPN nor
the NCAA college box score has a +/-, so the oracles are the final margin, the possession count
and the game clock. Some games have stretches where the source lineup lists fewer than five
players; the identities hold on the rest. The gates are the values observed on these games;
never lower one to make a change pass.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.game_impact import game_lineups_used, game_player_impact, game_player_impact_schema
from sportsdataverse.mbb import mbb_loaders
from sportsdataverse.mbb.mbb_game_impact import mbb_game_player_impact, ncaa_impact_rows
from sportsdataverse.wbb import wbb_loaders
from sportsdataverse.wbb.wbb_game_impact import wbb_game_player_impact

IMPACT = Path(__file__).resolve().parents[1] / "fixtures" / "bb_game_impact"
LEAGUE = {"mbb": "mens", "wbb": "womens"}
SHORT_LINEUP_GAMES = {"mbb": {"5722009", "5723822", "5737410", "5738251"}, "wbb": {"5722076", "5723697", "5737290"}}
#: The men's D1-vs-non-D1 contest: the D1 home side (ESPN 151) is its only team with rows.
NON_D1 = ("5722009", 151)
_SLOTS = [f"{side}_{i}" for side in ("home", "away") for i in range(1, 6)]


def _read(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(IMPACT / f"ncaa_{lg}_{name}_2025.parquet")


def _rows(lg: str) -> pl.DataFrame:
    return ncaa_impact_rows(_read(lg, "pbp"), _read(lg, "possessions"), league=LEAGUE[lg])


def _short_games(rows: pl.DataFrame) -> set[str]:
    sides = [pl.concat_list(_SLOTS[:5]), pl.concat_list(_SLOTS[5:])]
    short = pl.any_horizontal(side.list.drop_nulls().list.n_unique() < 5 for side in sides)
    return set(rows.filter(short)["game_id"].unique().to_list())


def _margins(lg: str) -> pl.DataFrame:
    """Each D1 team's final margin from the NCAA team box (a non-D1 opponent has a null id)."""
    box = _read(lg, "team_box").select(
        pl.col("contest_id").alias("game_id"), pl.col("team_espn_team_id").cast(pl.Int64).alias("team_id"), "pts"
    )
    opp = box.rename({"team_id": "opp_team_id", "pts": "opp_pts"})
    return (
        box.join(opp, on="game_id")
        .filter(pl.col("team_id").ne_missing(pl.col("opp_team_id")) & pl.col("team_id").is_not_null())
        .select("game_id", "team_id", (pl.col("pts") - pl.col("opp_pts")).cast(pl.Int64).alias("margin"))
    )


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_team_sums_are_five_times_the_team_totals(lg: str) -> None:
    rows = _rows(lg)
    impact = game_player_impact(rows, player_dtype=pl.Utf8())
    assert impact.schema == pl.Schema(game_player_impact_schema(pl.Utf8()))
    team = impact.group_by("game_id", "team_id").agg(pl.col("plus_minus", "poss", "seconds").sum())
    margins = _margins(lg)
    assert team.schema["team_id"] == margins.schema["team_id"]
    team = team.join(margins, on=["game_id", "team_id"], how="inner", validate="1:1")
    poss = _read(lg, "possessions").select(
        pl.col("contest_id").alias("game_id"),
        pl.when(pl.col("poss_team") == pl.col("home"))
        .then(pl.col("home_espn_team_id"))
        .otherwise(pl.col("away_espn_team_id"))
        .cast(pl.Int64)
        .alias("team_id"),
    )
    team = team.join(poss.group_by("game_id", "team_id").len("team_poss"), on=["game_id", "team_id"], validate="1:1")
    assert team.height == {"mbb": 25, "wbb": 24}[lg]
    assert _short_games(rows) == SHORT_LINEUP_GAMES[lg]
    clean = team.filter(~pl.col("game_id").is_in(list(SHORT_LINEUP_GAMES[lg])))
    assert clean.height == {"mbb": 18, "wbb": 18}[lg]
    assert (clean["plus_minus"] == 5 * clean["margin"]).all()
    assert (clean["poss"] == 5 * clean["team_poss"]).all()
    # Each period's clock is whole: five players' seconds are 2400 plus five minutes per overtime.
    overtimes = (clean["seconds"] / 5 - 2400) / 300
    assert ((overtimes - overtimes.round(0)).abs() < 1e-6).all() and (overtimes >= 0).all()


def test_non_d1_opponent_gets_no_rows_but_its_points_count() -> None:
    impact = game_player_impact(_rows("mbb"), player_dtype=pl.Utf8())
    game, d1 = NON_D1
    rows = impact.filter(pl.col("game_id") == game)
    assert set(rows["team_id"].unique().to_list()) == {d1}
    assert rows["opp_team_id"].null_count() == rows.height
    margin = _margins("mbb").filter((pl.col("game_id") == game) & (pl.col("team_id") == d1))["margin"].item()
    assert rows["plus_minus"].sum() == 5 * margin


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_lineups_used_are_five_man_units_that_add_up(lg: str) -> None:
    units = game_lineups_used(_rows(lg), player_dtype=pl.Utf8())
    assert units["player_5"].null_count() == 0
    team = (
        units.group_by("game_id", "team_id")
        .agg(pl.col("plus_minus").sum())
        .join(_margins(lg), on=["game_id", "team_id"])
    )
    clean = team.filter(~pl.col("game_id").is_in(list(SHORT_LINEUP_GAMES[lg])))
    assert clean.height == 18 and (clean["plus_minus"] == clean["margin"]).all()


def test_free_throws_take_the_whole_lineup_at_the_foul() -> None:
    pbp = _read("mbb", "pbp")
    rows = _rows("mbb")
    raw = ncaa_impact_rows(pbp.with_columns(pl.lit(None, pl.Utf8).alias("foul_class")), _read("mbb", "possessions"))
    with_rule = game_player_impact(rows, player_dtype=pl.Utf8())
    without = game_player_impact(raw, player_dtype=pl.Utf8())
    moved = with_rule.join(without, on=["game_id", "team_id", "player_id"], suffix="_raw").filter(
        pl.col("plus_minus") != pl.col("plus_minus_raw")
    )
    assert moved.height > 0
    # Only the points move: every player's clock is the same with or without the rule.
    keys = ["game_id", "team_id", "player_id"]
    clock = with_rule.join(without, on=keys, how="full", suffix="_raw", coalesce=True)
    assert ((clock["seconds"].fill_null(0) - clock["seconds_raw"].fill_null(0)).abs() < 1e-9).all()
    # Every free throw carries its foul row's ten slots exactly, nulls included.
    # The event rows come first, in the builder's (contest, release order) order.
    in_order = pbp.with_row_index("_o").sort("contest_id", "_o")
    ev = rows.head(pbp.height).with_columns(in_order.select("event_type", "foul_class"))
    foul_row = pl.when(pl.col("foul_class").is_not_null()).then(pl.col("_i")).forward_fill().over("game_id")
    ev = ev.with_row_index("_i").with_columns(foul_row.alias("_f"))
    fts = ev.filter((pl.col("event_type") == "Free Throw") & pl.col("_f").is_not_null())
    at = ev.select(pl.col("_i").alias("_f"), *[pl.col(c).alias(f"f_{c}") for c in _SLOTS])
    pair = fts.join(at, on="_f")
    assert pair.height == fts.height > 0
    for c in _SLOTS:
        assert pair[c].eq_missing(pair[f"f_{c}"]).all(), c


def test_possessions_need_events_and_a_side() -> None:
    pbp, poss = _read("mbb", "pbp"), _read("mbb", "possessions")
    no_pbp, no_side = "5722385", "5726007"
    rows = ncaa_impact_rows(
        pbp.filter(pl.col("contest_id") != no_pbp),
        poss.with_columns(
            pl.when(pl.col("contest_id") == no_side)
            .then(pl.lit("nobody"))
            .otherwise(pl.col("poss_team"))
            .alias("poss_team")
        ),
    )
    impact = game_player_impact(rows, player_dtype=pl.Utf8())
    assert no_pbp not in set(impact["game_id"])
    game = impact.filter(pl.col("game_id") == no_side)
    assert game.height > 0 and game["poss"].sum() == 0 and game["opp_poss"].sum() == 0


def test_public_builders_read_their_league(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mbb_loaders, "load_ncaa_mbb_pbp", lambda seasons: _read("mbb", "pbp"))
    monkeypatch.setattr(mbb_loaders, "load_ncaa_mbb_possessions", lambda seasons: _read("mbb", "possessions"))
    monkeypatch.setattr(wbb_loaders, "load_ncaa_wbb_pbp", lambda seasons: _read("wbb", "pbp"))
    monkeypatch.setattr(wbb_loaders, "load_ncaa_wbb_possessions", lambda seasons: _read("wbb", "possessions"))
    assert mbb_game_player_impact(2025).height == game_player_impact(_rows("mbb"), player_dtype=pl.Utf8()).height
    assert wbb_game_player_impact(2025).height == game_player_impact(_rows("wbb"), player_dtype=pl.Utf8()).height
    with pytest.raises(ValueError, match="league"):
        mbb_game_player_impact(2025, league="coed")
