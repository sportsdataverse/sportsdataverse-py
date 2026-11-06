"""College EPV and EPV added (BB-D24) on real ``ncaa_{mbb,wbb}_pbp`` games of the 2025 holdout season.

The fixtures (``tests/fixtures/bb_epv/{mbb,wbb}_pbp_2025.parquet`` and the hand-cut
``mbb_pbp_2025_quirks.parquet``, README beside them) are release rows, filtered and with columns
dropped only. Expected values are read off the release (its scores, its possession stamps), never
written by hand.
"""

from __future__ import annotations

import pathlib
from functools import lru_cache

import numpy as np
import polars as pl
import pytest

from sportsdataverse.mbb import mbb_epv
from sportsdataverse.mbb.mbb_epv import ncaa_epv_inputs
from sportsdataverse.nba.nba_epv import (
    EPV_FEATURES,
    START_TYPES,
    add_epv,
    epv_level,
    epv_lineup_games,
    epv_player_games,
    epv_states,
    epv_team_games,
    load_epv_model,
)

D = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "bb_epv"
HOLDOUT = 2025
LEAGUES = {"mbb": "mens", "wbb": "womens"}
KEY = ["game_id", "possession_number"]
SHOTS = ("made_shot", "missed_shot", "free_throw")


@lru_cache(maxsize=3)
def _release(name: str) -> pl.DataFrame:
    return pl.read_parquet(D / f"{name}.parquet")


@lru_cache(maxsize=3)
def _inputs(name: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    return ncaa_epv_inputs(_release(name))


@lru_cache(maxsize=3)
def _states(name: str) -> pl.DataFrame:
    poss, pbp, _, _ = _inputs(name)
    return epv_states(poss, pbp)


@lru_cache(maxsize=3)
def _plays(name: str) -> pl.DataFrame:
    lg = name.split("_")[0]
    return add_epv(_states(name), league_id=lg, game_order=_inputs(name)[3].select("game_id", "game_order"))


def _fixture(lg: str) -> str:
    return f"{lg}_pbp_{HOLDOUT}"


def _finals(name: str) -> pl.DataFrame:
    """Each game's points, both teams, from the release's last scores.

    A wrong basket counts too: it is the offense's two, put in by a defender.
    """
    return (
        _release(name)
        .group_by(pl.col("contest_id").cast(pl.Utf8).alias("game_id"))
        .agg((pl.col("home_score").max() + pl.col("away_score").max()).alias("final"))
    )


@pytest.mark.parametrize("name", [_fixture("mbb"), _fixture("wbb"), f"mbb_pbp_{HOLDOUT}_quirks"])
def test_labels_hold_every_point_of_the_game(name: str) -> None:
    """Each possession's points are its offense's, and a game's possessions hold its final score.

    The quirks file has a game whose ``home_score`` is the ``away`` team's, with a wrong basket
    (5722807), and one with the other side's free throws stamped on a possession (5722014).
    """
    starts = _states(name).filter(pl.col("step") == 0)
    got = starts.group_by("game_id").agg(pl.col("remaining_points").sum().alias("labels"))
    j = got.join(_finals(name), on="game_id")
    assert j.height == _release(name)["contest_id"].n_unique()
    assert (j["labels"] == j["final"]).all(), j.filter(pl.col("labels") != pl.col("final"))


def test_the_score_is_summed_from_the_baskets() -> None:
    """In 5722807 the release's ``home_score`` is the away team's; summed from the baskets the score is the home team's."""
    _, pbp, _, _ = _inputs(f"mbb_pbp_{HOLDOUT}_quirks")
    fin = pbp.group_by("game_id").agg(
        pl.col("score_home").max(), pl.col("score_away").max(), pl.col("home_score").max(), pl.col("away_score").max()
    )
    swapped = fin.filter(pl.col("game_id") == "5722807").row(0, named=True)
    assert (swapped["score_home"], swapped["score_away"]) == (swapped["away_score"], swapped["home_score"])
    assert swapped["score_home"] != swapped["home_score"]
    kept = fin.filter(pl.col("game_id") == "5722014").row(0, named=True)
    assert (kept["score_home"], kept["score_away"]) == (kept["home_score"], kept["away_score"])


@pytest.mark.parametrize("name", [_fixture("mbb"), _fixture("wbb"), f"mbb_pbp_{HOLDOUT}_quirks"])
def test_no_possession_holds_the_other_sides_shots(name: str) -> None:
    """Every shot and free throw is the possession's offense's (the release stamps a few on the other side's)."""
    poss, pbp, _, _ = _inputs(name)
    shots = pbp.filter(pl.col("event_type").is_in(SHOTS)).join(poss.select(*KEY, "offense_team_id"), on=KEY, how="left")
    assert shots.height > 0
    assert (shots["team_id"] == shots["offense_team_id"]).all()
    # the release's own stamps disagree on some: the split is what fixes them
    if name.endswith("quirks"):
        assert (shots["_poss_side"] != shots["team_id"]).any()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_possessions_are_consecutive_and_the_engine_reads_them(lg: str) -> None:
    poss, pbp, _, _ = _inputs(_fixture(lg))
    p = poss.sort(KEY)
    gaps = p.with_columns(pl.col("possession_number").diff().over("game_id").alias("d"))["d"].drop_nulls()
    assert (gaps == 1).all()
    assert (p["start_order_index"] <= p["end_order_index"]).all()
    # contiguous: each possession starts on the row after the previous one ends
    nxt = p.with_columns(pl.col("start_order_index").shift(-1).over("game_id").alias("n")).drop_nulls("n")
    assert (nxt["n"] == nxt["end_order_index"] + 1).all()
    assert set(p["possession_start_type"].unique()) <= set(START_TYPES)


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_a_steal_opens_the_next_possession(lg: str) -> None:
    """The steal row trailing a turnover belongs to the next possession, which starts live; the stealer is credited."""
    poss, pbp, _, _ = _inputs(_fixture(lg))
    steals = pbp.filter(pl.col("description").str.starts_with("STEAL"))
    j = steals.join(poss.select(*KEY, "start_order_index", "possession_start_type", "number_in_period"), on=KEY)
    # the steal comes before the possession's first live event, so the possession starts as a live-ball turnover
    first = pbp.filter(pl.col("event_type").is_in(["made_shot", "missed_shot", "free_throw", "rebound", "turnover"]))
    first = first.group_by(KEY).agg(pl.col("order_index").min().alias("_first"))
    lead = j.join(first, on=KEY).filter((pl.col("order_index") < pl.col("_first")) & (pl.col("number_in_period") > 1))
    assert lead.height > 0.9 * steals.height
    assert (lead["possession_start_type"].is_in(["OffLiveBallTurnover", "OffTimeout"])).all()
    plays = _plays(_fixture(lg)).sort("game_id", "order_index")
    prev = plays.with_columns(
        pl.col("event_type").shift(1).over("game_id").alias("_pt"),
        pl.col("epv_added").shift(1).over("game_id").alias("_pa"),
    ).filter(pl.col("description").str.starts_with("STEAL") & (pl.col("_pt") == "turnover"))
    assert prev.height > 0.9 * steals.height
    own = epv_player_games(_plays(_fixture(lg)), _inputs(_fixture(lg))[2])
    assert own["own_actions"].sum() > 0
    # a steal gives the stealer the negative of the turnover it ended: turnovers cost the offense
    assert prev["_pa"].mean() < 0


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_epv_added_telescopes_and_teams_reconcile(lg: str) -> None:
    p = _plays(_fixture(lg))
    r = p.group_by(KEY).agg(
        (
            pl.col("epv_added").sum()
            - pl.col("points").sum()
            + pl.col("epv_start").first()
            - pl.col("epv_after").filter(pl.col("is_last")).first()
        ).abs()
    )
    assert r["epv_added"].max() < 1e-9
    t = epv_team_games(p)
    assert t.height == 2 * p["game_id"].n_unique()
    assert t.group_by("game_id").agg(pl.col("net_epv").sum().abs())["net_epv"].max() < 1e-9


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_on_court_is_five_times_the_team(lg: str) -> None:
    """With the release's ten on every row, the players' on-court EPV sums to five times their team's."""
    p = _plays(_fixture(lg))
    tens = _inputs(_fixture(lg))[2]
    full = tens.filter(pl.all_horizontal(pl.exclude("game_id", "action_number").is_not_null()))
    games = (
        full.group_by("game_id")
        .len()
        .join(tens.group_by("game_id").len(), on="game_id")
        .filter(pl.col("len") == pl.col("len_right"))["game_id"]
    )
    pg = epv_player_games(p, tens).filter(pl.col("game_id").is_in(games.implode()))
    tg = epv_team_games(p).filter(pl.col("game_id").is_in(games.implode()))
    on = (
        pg.group_by("game_id", "team_id")
        .agg(pl.col("off_epv_on").sum())
        .join(tg.select("game_id", "team_id", "off_epv"), on=["game_id", "team_id"])
    )
    assert on.height > 0
    assert np.allclose(on["off_epv_on"].to_numpy(), 5 * on["off_epv"].to_numpy())
    assert epv_lineup_games(p, tens).height > 0


def test_the_level_is_as_of_each_game() -> None:
    """A game's level reads only the games of earlier dates: dropping later games changes no earlier one."""
    name = _fixture("mbb")
    booster = load_epv_model("mbb")
    order = _inputs(name)[3].select("game_id", "game_order")
    s = _states(name)
    full = epv_level(s, booster, order)
    dates = sorted(order["game_order"].unique().to_list())
    early = order.filter(pl.col("game_order") <= dates[len(dates) // 2])["game_id"]
    part = epv_level(s.filter(pl.col("game_id").is_in(early.implode())), booster, order)
    j = part.join(full, on="game_id", suffix="_full")
    assert j.height == early.len()
    assert np.allclose(j["level_scale"].to_numpy(), j["level_scale_full"].to_numpy())
    first = order.filter(pl.col("game_order") == dates[0])["game_id"]
    assert (full.filter(pl.col("game_id").is_in(first.implode()))["level_scale"] == 1.0).all()
    # later games move off the training level
    assert full["level_scale"].n_unique() > 1


def test_the_level_scales_every_value() -> None:
    """``epv`` is the booster's prediction times the game's level, and so is the average start the end term uses."""
    name = _fixture("mbb")
    booster = load_epv_model("mbb")
    order = _inputs(name)[3].select("game_id", "game_order")
    s = _states(name).filter(pl.col("step") > 0)
    raw = booster.inplace_predict(s.select(pl.col(EPV_FEATURES).cast(pl.Float64)).to_numpy())
    lv = s.select("game_id").join(
        epv_level(_states(name), booster, order), on="game_id", how="left", maintain_order="left"
    )
    p = _plays(name).sort("game_id", "possession_number", "step")
    assert np.allclose(p["epv"].to_numpy(), raw * lv["level_scale"].to_numpy())


def test_the_level_is_the_shrunk_points_per_possession_of_earlier_dates() -> None:
    """``level_scale`` against a hand walk over the release's own possessions, date by date."""
    name = _fixture("mbb")
    booster = load_epv_model("mbb")
    ppp, k = float(booster.attr("level_ppp")), float(booster.attr("level_k"))
    poss, _, _, games = _inputs(name)
    starts = _states(name).filter(pl.col("step") == 0)
    per_game = {
        g: (float(p), n)
        for g, p, n in starts.group_by("game_id").agg(pl.col("remaining_points").sum(), pl.len()).iter_rows()
    }
    date = dict(games.select("game_id", "game_order").iter_rows())
    want = {}
    for g in per_game:
        before = [h for h in per_game if date[h] < date[g]]
        pts, n = sum(per_game[h][0] for h in before), sum(per_game[h][1] for h in before)
        want[g] = (k * ppp + pts) / (k + n) / ppp
    got = dict(epv_level(_states(name), booster, games.select("game_id", "game_order")).iter_rows())
    assert got.keys() == want.keys()
    assert all(abs(got[g] - want[g]) < 1e-12 for g in want)
    assert max(want.values()) != min(want.values())


def test_a_wrong_basket_ends_its_possession() -> None:
    """5722807's wrong-basket two stays the last event of its possession; the next starts as after a basket."""
    poss, pbp, _, _ = _inputs(f"mbb_pbp_{HOLDOUT}_quirks")
    wb = (
        pbp.filter(pl.col("event_type_right").str.starts_with("Wrong Basket"))
        if "event_type_right" in pbp.columns
        else None
    )
    rel = _release(f"mbb_pbp_{HOLDOUT}_quirks").filter(pl.col("contest_id") == "5722807")
    row = rel.with_row_index("order_index").filter(pl.col("event_type").str.starts_with("Wrong Basket"))
    assert row.height == 1
    oi = int(row["order_index"][0])
    g = poss.filter(pl.col("game_id") == "5722807").sort("possession_number")
    mine = g.filter(pl.col("start_order_index") <= oi).tail(1)
    assert mine["end_order_index"][0] == oi
    nxt = g.filter(pl.col("possession_number") == mine["possession_number"][0] + 1)
    assert nxt["possession_start_type"][0] == "OffMadeShot"


def test_the_level_refuses_an_undated_game_and_a_booster_without_one() -> None:
    name = _fixture("mbb")
    order = _inputs(name)[3].select("game_id", "game_order")
    with pytest.raises(ValueError, match="no game_order"):
        epv_level(_states(name), load_epv_model("mbb"), order.head(3))
    with pytest.raises(ValueError, match="dtype|is "):
        epv_level(_states(name), load_epv_model("mbb"), order.with_columns(pl.col("game_id").cast(pl.Int64)))
    two = pl.concat([order, order.head(1).with_columns(pl.col("game_order") + pl.duration(days=1))])
    with pytest.raises(ValueError, match="two orders"):
        epv_level(_states(name), load_epv_model("mbb"), two)
    assert epv_level(_states(name), load_epv_model("mbb"), pl.concat([order, order.head(1)])).height == order.height
    with pytest.raises(ValueError, match="level_ppp"):
        epv_level(_states(name), load_epv_model("00"))
    assert load_epv_model("00").attr("level_ppp") is None  # the NBA booster is served without a level


#: Observed on the fixture with the shipped boosters: skill vs the training label mean (the level
#: bias was +0.004 men, -0.039 women, on 20 games over the season's first 17-18 dates).
OBSERVED_FIT = {"mbb": 0.267, "wbb": 0.273}


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_the_booster_is_out_of_sample_and_calibrated(lg: str) -> None:
    """The fixture season is the holdout; on it EPV beats the training mean and is level within 3 game-clustered SE."""
    booster = load_epv_model(lg)
    train = {int(x) for x in booster.attr("train_seasons").split(",")}
    held = {int(booster.attr("holdout_season")), int(booster.attr("confirmation_season"))}
    assert not train & held and int(booster.attr("holdout_season")) == HOLDOUT
    assert float(booster.attr("level_k")) > 0 and float(booster.attr("level_ppp")) > 0
    p = _plays(_fixture(lg))
    y = p["remaining_points"].to_numpy()
    const = float(booster.attr("train_label_mean"))
    skill = 1 - np.mean((p["epv"].to_numpy() - y) ** 2) / np.mean((const - y) ** 2)
    assert skill > OBSERVED_FIT[lg] - 0.02
    # level within three standard errors of a game-clustered mean
    g = (
        p.with_columns((pl.col("epv") - pl.col("remaining_points")).alias("e"))
        .group_by("game_id")
        .agg(pl.col("e").sum(), pl.len().alias("n"))
    )
    e, n = g["e"].to_numpy(), g["n"].to_numpy()
    bias = e.sum() / n.sum()
    se = np.sqrt(np.sum((e - bias * n) ** 2) / (len(n) * (len(n) - 1))) / n.mean()
    assert abs(bias) < 3 * se


def test_inputs_are_checked() -> None:
    with pytest.raises(ValueError, match="missing columns"):
        ncaa_epv_inputs(_release(_fixture("mbb")).drop("poss_num"))
    with pytest.raises(ValueError, match="league must be"):
        mbb_epv.mbb_epv_team_games(HOLDOUT, league="girls")
    first = min(int(x) for x in load_epv_model("wbb").attr("train_seasons").split(","))
    with pytest.raises(ValueError, match=f"served from the {first} season"):
        mbb_epv.mbb_epv_plays(first - 1, league="womens")


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_public_tables_carry_espn_teams_and_ncaa_players(lg: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """The loaders read the season asked; teams are ESPN ids, players NCAA ids (String); non-D1 teams get no row."""
    seen: list[list[int]] = []

    def fake(seasons: list[int]) -> pl.DataFrame:
        seen.append(list(seasons))
        return _release(_fixture(lg))

    if lg == "wbb":
        from sportsdataverse.wbb import wbb_epv, wbb_loaders

        monkeypatch.setattr(wbb_loaders, "load_ncaa_wbb_pbp", fake)
        team, player, plays = (
            wbb_epv.wbb_epv_team_games(HOLDOUT),
            wbb_epv.wbb_epv_player_games(HOLDOUT),
            wbb_epv.wbb_epv_plays(HOLDOUT),
        )
        units = wbb_epv.wbb_epv_lineup_games(HOLDOUT, return_as_pandas=True)
    else:
        from sportsdataverse.mbb import mbb_loaders

        monkeypatch.setattr(mbb_loaders, "load_ncaa_mbb_pbp", fake)
        team, player, plays = (
            mbb_epv.mbb_epv_team_games(HOLDOUT),
            mbb_epv.mbb_epv_player_games(HOLDOUT),
            mbb_epv.mbb_epv_plays(HOLDOUT),
        )
        units = mbb_epv.mbb_epv_lineup_games(HOLDOUT, return_as_pandas=True)
    assert all(s == [HOLDOUT] for s in seen)
    games = _inputs(_fixture(lg))[3]
    espn = set(games["home_espn_team_id"].drop_nulls()) | set(games["away_espn_team_id"].drop_nulls())
    for t in (team, player):
        assert t["team_id"].null_count() == 0 and set(t["team_id"]) <= espn
        assert t.schema["team_id"] == pl.Int64
    assert player.schema["player_id"] == pl.Utf8
    assert set(player["player_id"]) <= set(_release(_fixture(lg))["player_1_id"].drop_nulls()) | set(
        pl.concat([_release(_fixture(lg))[f"{s}_{i}_player_id"] for s in ("home", "away") for i in range(1, 6)])
        .drop_nulls()
        .to_list()
    )
    non_d1 = games.filter(pl.col("home_espn_team_id").is_null() | pl.col("away_espn_team_id").is_null())
    assert non_d1.height > 0  # the fixture has non-D1 opponents: one team row each, not two
    assert team.height == 2 * games.height - non_d1.height
    assert plays.schema["person_id"] == pl.Utf8 and plays.height == _plays(_fixture(lg)).height
    # the league routes to its own booster: the women's plays are not the men's booster's
    plays = plays.sort("game_id", "order_index")
    assert np.allclose(plays["epv"].to_numpy(), _plays(_fixture(lg)).sort("game_id", "order_index")["epv"].to_numpy())
    other = add_epv(
        _states(_fixture(lg)),
        league_id="wbb" if lg == "mbb" else "mbb",
        game_order=_inputs(_fixture(lg))[3].select("game_id", "game_order"),
    ).sort("game_id", "order_index")
    assert not np.allclose(plays["epv"].to_numpy(), other["epv"].to_numpy())
    assert len(units) > 0
