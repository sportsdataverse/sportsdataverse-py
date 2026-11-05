"""BB-D24: expected possession value (EPV) and EPV added on NBA play-by-play.

Every test runs on committed real releases (``tests/fixtures/bb_epv/README.md``): the first 40
regular-season games of the 2024-25 NBA, the holdout season of the bundled booster (it never saw
them in training). Floors are the observed values on these captures less stated headroom; never
lower one to make a test pass.
"""

from __future__ import annotations

import pathlib
from functools import lru_cache

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.nba import nba_epv
from sportsdataverse.nba.nba_epv import (
    EPV_FEATURES,
    START_TYPES,
    add_epv,
    epv_lineup_games,
    epv_player_games,
    epv_states,
    epv_team_games,
    load_epv_model,
)
from sportsdataverse.nba.nba_possession_flags import TRANSITION_SECONDS, add_possession_flags

D = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "bb_epv"
LEAGUE = {"nba": "00"}
#: The booster's holdout season: the fixture's.
HOLDOUT = 2025
KEY = ["game_id", "possession_number"]
SLOTS = [f"{s}_player_{i}" for s in ("off", "def") for i in range(1, 6)]


@lru_cache(maxsize=2)
def _raw(lg: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    return pl.read_parquet(D / f"{lg}_possessions_{HOLDOUT}.parquet"), pl.read_parquet(
        D / f"{lg}_pbp_{HOLDOUT}.parquet"
    )


@lru_cache(maxsize=2)
def _states(lg: str) -> pl.DataFrame:
    return epv_states(*_raw(lg))


@lru_cache(maxsize=2)
def _plays(lg: str) -> pl.DataFrame:
    return add_epv(_states(lg), league_id=LEAGUE[lg])


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_labels_sum_to_the_engine_points(lg: str) -> None:
    """Each possession's event points sum to the possessions release's ``points``; the start's label is all of it."""
    poss, _ = _raw(lg)
    s = _states(lg)
    got = s.group_by(KEY).agg(pl.col("points").sum(), pl.col("remaining_points").filter(pl.col("step") == 0).first())
    j = got.join(poss.select(*KEY, pl.col("points").alias("engine")), on=KEY)
    assert j.height == poss.height
    assert (j["points"] == j["engine"]).all()
    assert (j["remaining_points"] == j["engine"]).all()


def _oracle_margins(pbp: pl.DataFrame) -> dict[tuple[str, int], int]:
    """Home lead before each event: the feed's last non-blank, non-zero score carried forward, event by event."""
    out: dict[tuple[str, int], int] = {}
    for (gid,), g in pbp.sort("game_id", "order_index").group_by("game_id", maintain_order=True):
        h = a = 0
        for oi, sh, sa in g.select("order_index", "score_home", "score_away").iter_rows():
            out[(gid, oi)] = h - a
            h = int(sh) if sh not in (None, "", "0") else h
            a = int(sa) if sa not in (None, "", "0") else a
    return out


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_start_margin_is_the_lead_before_the_first_event(lg: str) -> None:
    """``start_margin`` is the offense's lead before the possession's first event, never after its own basket.

    The oracle walks the raw feed. A possession whose first event is its own made shot (common
    after a defensive rebound, which closes the previous possession) is where a margin read
    after the event goes wrong, so those are checked on their own.
    """
    poss, pbp = _raw(lg)
    oracle = _oracle_margins(pbp)
    s = _states(lg).filter(pl.col("step") == 0)
    first = poss.select(*KEY, "start_order_index")
    rows = s.join(first, on=KEY).select(
        "game_id", "start_order_index", "start_margin", "offense_team_id", "home_team_id"
    )
    want = pl.Series([float(oracle[(g, oi)] * (1 if off == home else -1)) for g, oi, _, off, home in rows.iter_rows()])
    off = rows.filter(rows["start_margin"] != want)
    # The running score is the feed's cumulative max (BB-D7's rule, which survives the pre-2019 "0"
    # rows), so a score a replay review lowers reads high until it is passed again. That is the only
    # source of a miss: each one sits in a game whose feed score falls somewhere.
    falls = (
        pbp.sort("game_id", "order_index")
        .with_columns(pl.col("score_home", "score_away").cast(pl.Int64, strict=False))
        .filter(pl.col("score_home") > 0)
        .group_by("game_id")
        .agg(((pl.col("score_home").diff() < 0) | (pl.col("score_away").diff() < 0)).any().alias("falls"))
    )
    assert off.join(falls.filter("falls"), on="game_id", how="anti").height == 0
    assert off.height <= 0.01 * rows.height  # observed 2 of 8,012 possessions (one reviewed basket)
    scored_first = (
        _states(lg).filter((pl.col("step") == 1) & (pl.col("points") > 0)).select(*KEY).join(s, on=KEY).height
    )
    assert scored_first > 500  # observed 2,200+: the case is well covered


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_transition_state_is_known_as_of_each_event(lg: str) -> None:
    """``transition`` 2 only once the first shot is taken; 1 only before it, on a live start, inside the window."""
    poss, pbp = _raw(lg)
    flags = (
        add_possession_flags(poss, pbp)
        .sort(KEY)
        .select(
            *KEY,
            "is_transition",
            "possession_start_type",
            "number_in_period",
            # the change of possession: the previous possession's last clock
            pl.col("end_seconds_remaining").shift(1).over("game_id").alias("_ball_clock"),
        )
    )
    s = _states(lg).filter(pl.col("step") > 0).join(flags, on=KEY)
    first = (
        s.filter(pl.col("event_type").is_in(["made_shot", "missed_shot"]))
        .group_by(KEY)
        .agg(pl.col("order_index").min().alias("_first"))
    )
    s = s.join(first, on=KEY, how="left").with_columns(
        (pl.col("order_index") >= pl.col("_first")).fill_null(False).alias("_seen")
    )
    before, after = s.filter(~pl.col("_seen")), s.filter(pl.col("_seen"))
    assert (before["transition"] != 2).all()
    assert ((after["transition"] == 2) == after["is_transition"]).all()
    window = before.filter(pl.col("transition") == 1)
    assert window.height > 0
    assert window["possession_start_type"].is_in(["OffMadeShot", "OffMissedShot", "OffLiveBallTurnover"]).all()
    assert (window["number_in_period"] > 1).all()
    assert ((window["_ball_clock"] - window["seconds_remaining"]) <= TRANSITION_SECONDS).all()
    shut = before.filter(
        pl.col("possession_start_type").is_in(["OffMadeShot", "OffMissedShot", "OffLiveBallTurnover"])
        & (pl.col("number_in_period") > 1)
        & ((pl.col("_ball_clock") - pl.col("seconds_remaining")) > TRANSITION_SECONDS)
    )
    assert shut.height > 0 and (shut["transition"] == 0).all()  # the window closes after 10 s


def test_features_do_not_see_later_events() -> None:
    """Cutting the feed after an event leaves every state up to it unchanged (no look-ahead).

    For each of a game's possessions with a shot, the pbp is cut just before the possession's
    last event and the possessions after it are dropped; the states of the events kept must
    equal the full run's. A transition flag read before the first shot, or a margin read after
    the start, changes under the cut.
    """
    poss, pbp = _raw("nba")
    gid = poss["game_id"].min()
    gp, gb = poss.filter(pl.col("game_id") == gid), pbp.filter(pl.col("game_id") == gid)
    full = _states("nba").filter(pl.col("game_id") == gid)
    checked = 0
    for row in gp.filter(pl.col("end_order_index") - pl.col("start_order_index") >= 2).iter_rows(named=True):
        cut = row["end_order_index"] - 1
        p = gp.filter(pl.col("possession_number") <= row["possession_number"]).with_columns(
            pl.when(pl.col("possession_number") == row["possession_number"])
            .then(cut)
            .otherwise(pl.col("end_order_index"))
            .alias("end_order_index")
        )
        part = epv_states(p, gb.filter(pl.col("order_index") <= cut))
        mine = pl.col("possession_number") == row["possession_number"]
        a = part.filter(mine).select("step", *EPV_FEATURES)
        b = full.filter(mine & (pl.col("step") <= a["step"].max())).select("step", *EPV_FEATURES)
        assert a.equals(b), row["possession_number"]
        checked += 1
    assert checked > 50  # observed in game 0022400001


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_epv_added_telescopes_over_each_possession(lg: str) -> None:
    """``epv_before`` is the previous state's EPV, ``epv_after`` this one's, so a possession sums to its ends."""
    p = _plays(lg)
    inner = p.filter(~pl.col("is_last"))
    assert (inner["epv_after"] == inner["epv"]).all()
    prev = p.sort("game_id", "possession_number", "step").with_columns(
        pl.col("epv").shift(1).over(KEY).fill_null(pl.col("epv_start")).alias("_prev")
    )
    assert np.allclose(prev["epv_before"].to_numpy(), prev["_prev"].to_numpy())
    r = p.group_by(KEY).agg(
        (
            pl.col("epv_added").sum()
            - pl.col("points").sum()
            + pl.col("epv_start").first()
            - pl.col("epv_after").filter(pl.col("is_last")).first()
        ).abs()
    )
    assert r["epv_added"].max() < 1e-9


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_a_possession_end_prices_the_next_start(lg: str) -> None:
    """An ending play gives up the next start over an average one; a period's last gives up nothing."""
    p = _plays(lg)
    mean = float(load_epv_model(LEAGUE[lg]).attr("start_epv_mean"))
    nxt = p.select("game_id", (pl.col("possession_number") - 1).alias("possession_number"), "epv_start").unique()
    last = p.filter(pl.col("is_last")).join(nxt.rename({"epv_start": "_next"}), on=KEY, how="left")
    inside = last.filter(pl.col("next_in_period") & pl.col("_next").is_not_null())
    assert np.allclose(inside["epv_after"].to_numpy(), mean - inside["_next"].to_numpy())
    assert (last.filter(~pl.col("next_in_period"))["epv_after"] == 0).all()
    starts = _states(lg).filter(pl.col("step") == 0).select(*KEY, pl.col("start_type").alias("_next_type"))
    by = (
        inside.join(starts.with_columns(pl.col("possession_number") - 1), on=KEY)
        .group_by("_next_type")
        .agg(pl.col("epv_after").mean())
    )
    after = dict(by.iter_rows())
    # observed: a live-ball turnover hands over the best start, a dead ball a worse one
    assert after[START_TYPES.index("OffLiveBallTurnover")] < after[START_TYPES.index("OffDeadball")]


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_states_order_as_the_game_does(lg: str) -> None:
    """The fitted EPV orders states the way the holdout season does (observed on these games)."""
    s = _plays(lg)
    starts = add_epv(_states(lg), league_id=LEAGUE[lg]).select(*KEY, "epv_start").unique()
    st = _states(lg).filter(pl.col("step") == 0).join(starts, on=KEY)
    by_type = dict(st.group_by("start_type").agg(pl.col("epv_start").mean()).iter_rows())
    assert by_type[START_TYPES.index("OffLiveBallTurnover")] > by_type[START_TYPES.index("OffDeadball")]
    late = st.filter(pl.col("seconds_remaining") <= 2)["epv_start"].mean()
    early = st.filter(pl.col("seconds_remaining") >= 30)["epv_start"].mean()
    assert late < early - 0.3  # observed a 0.88 gap: a heave is worth little
    ft = s.filter(pl.col("last_event") == nba_epv.LAST_EVENTS.index("free_throw"))
    one_left = ft.filter(pl.col("ft_pending") == 1)["epv"].mean() - ft.filter(pl.col("ft_pending") == 0)["epv"].mean()
    assert 0.6 < one_left < 0.9  # observed 0.70: a free throw still to come is worth about a make rate
    oreb = s.filter(pl.col("last_event") == nba_epv.LAST_EVENTS.index("off_rebound"))["epv"].mean()
    miss = s.filter(pl.col("last_event") == nba_epv.LAST_EVENTS.index("missed_shot"))["epv"].mean()
    assert oreb > miss + 0.5  # observed 0.91


#: Observed on the holdout fixture with the shipped booster: (skill vs the training label mean,
#: |mean(epv) - mean(label)|). The floor keeps 0.02 of skill and allows 0.03 points of bias (40
#: games, so a season's drift plus sampling).
OBSERVED_FIT = {"nba": (0.262, 0.014)}


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_holdout_fit_and_calibration(lg: str) -> None:
    """Squared error on the points label beats the training mean; calibrated in the large; seasons disjoint."""
    booster = load_epv_model(LEAGUE[lg])
    train = {int(x) for x in booster.attr("train_seasons").split(",")}
    held = {int(booster.attr("holdout_season")), int(booster.attr("confirmation_season"))}
    assert not train & held and len(held) == 2
    assert int(booster.attr("holdout_season")) == HOLDOUT  # the fixture's season is out of sample
    s = _states(lg)
    pred = booster.inplace_predict(s.select(pl.col(EPV_FEATURES).cast(pl.Float64)).to_numpy())
    y = s["remaining_points"].to_numpy()
    const = float(booster.attr("train_label_mean"))
    skill = 1 - np.mean((pred - y) ** 2) / np.mean((const - y) ** 2)
    floor, _ = OBSERVED_FIT[lg]
    assert skill > floor - 0.02
    assert abs(float(np.mean(pred) - np.mean(y))) < 0.03


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_team_games_reconcile_with_the_plays(lg: str) -> None:
    p = _plays(lg)
    t = epv_team_games(p)
    want = p.group_by("game_id", "offense_team_id").agg(pl.col("epv_added").sum().alias("w"))
    j = t.join(want, left_on=["game_id", "team_id"], right_on=["game_id", "offense_team_id"])
    assert j.height == t.height == 2 * p["game_id"].n_unique()
    assert np.allclose(j["off_epv"].to_numpy(), j["w"].to_numpy())
    both = t.group_by("game_id").agg(pl.col("net_epv").sum().abs(), pl.col("off_epv").sum(), pl.col("def_epv").sum())
    assert both["net_epv"].max() < 1e-9
    assert np.allclose(both["off_epv"].to_numpy(), both["def_epv"].to_numpy())


HOME = [f"home_player_{i}" for i in range(1, 6)]
AWAY = [f"away_player_{i}" for i in range(1, 6)]


@lru_cache(maxsize=2)
def _tens(lg: str) -> pl.DataFrame:
    return pl.read_parquet(D / f"{lg}_game_lineups_{HOLDOUT}.parquet")


def _complete_games(p: pl.DataFrame) -> pl.DataFrame:
    """The games whose every play has a known possession lineup (the on-court fallback)."""
    gaps = p.filter(pl.any_horizontal(pl.col(SLOTS).is_null())).select("game_id").unique()
    return p.select("game_id").unique().join(gaps, on="game_id", how="anti")


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_on_court_is_five_times_the_team(lg: str) -> None:
    """Every player on the floor carries the play: per team-game, the players sum to five times the team."""
    p = _plays(lg)
    games = _complete_games(p)
    assert games.height >= 0.9 * p["game_id"].n_unique()  # observed 40 of 40
    p = p.join(games, on="game_id", how="semi")
    pg = epv_player_games(p, _tens(lg)).group_by("game_id", "team_id").agg(pl.col("off_epv_on", "def_epv_on").sum())
    tg = epv_team_games(p).join(pg, on=["game_id", "team_id"])
    assert np.allclose(tg["off_epv_on"].to_numpy(), 5 * tg["off_epv"].to_numpy())
    assert np.allclose(tg["def_epv_on"].to_numpy(), 5 * tg["def_epv"].to_numpy())
    lu = epv_lineup_games(p, _tens(lg)).group_by("game_id", "team_id").agg(pl.col("off_epv").sum().alias("lu"))
    tl = epv_team_games(p).join(lu, on=["game_id", "team_id"])
    assert np.allclose(tl["lu"].to_numpy(), tl["off_epv"].to_numpy())


def _with_action_tens(lg: str) -> pl.DataFrame:
    """The plays with the game lineups release's ten at each action (home and away lists)."""
    tens = _tens(lg).select(
        "game_id",
        "action_number",
        pl.concat_list(HOME).alias("_home"),
        pl.concat_list(AWAY).alias("_away"),
    )
    return _plays(lg).join(tens, on=["game_id", "action_number"], how="left").sort("game_id", "order_index")


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_on_court_reads_the_action_lineup(lg: str) -> None:
    """With the game lineups release, a player subbed in mid-possession is on the floor from that action."""
    p = _with_action_tens(lg)
    own = pl.concat_list(SLOTS)
    moved = p.filter(
        pl.col("_home").is_not_null()
        & ((pl.col("_home").list.concat("_away").list.sort()) != own.list.sort())
        & (pl.col("event_type") != "free_throw")
    )
    assert moved.height > 100  # observed 2,652 plays after a mid-possession substitution
    row = moved.row(0, named=True)
    new = next(x for x in row["_home"] + row["_away"] if x not in [row[c] for c in SLOTS])
    pg = epv_player_games(_plays(lg), _tens(lg))
    with_tens = pg.filter((pl.col("game_id") == row["game_id"]) & (pl.col("player_id") == new))["epv_on"]
    without = epv_player_games(_plays(lg)).filter((pl.col("game_id") == row["game_id"]) & (pl.col("player_id") == new))[
        "epv_on"
    ]
    assert with_tens.len() == 1 and (without.len() == 0 or not np.isclose(with_tens[0], without[0]))


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_free_throws_land_on_the_foul_lineup(lg: str) -> None:
    """A free throw counts for the ten on the floor at the foul, not at the shot (BB-D6's rule).

    The same plays are rolled up twice, once as they are and once with the fouls relabelled so the
    rule cannot find them; the players subbed in or out between a foul and its free throws are the
    ones whose on-court EPV +/- moves, by exactly those free throws' EPV added.
    """
    p = _with_action_tens(lg).filter(pl.col("_home").is_not_null())
    ten = pl.col("_home").list.concat("_away")
    at_foul = pl.when(pl.col("event_type") == "foul").then(ten).forward_fill().over("game_id")
    ft = p.with_columns(at_foul.alias("_foul"), ten.alias("_own")).filter(
        (pl.col("event_type") == "free_throw")
        & pl.col("_foul").is_not_null()
        & (pl.col("_own").list.sort() != pl.col("_foul").list.sort())
    )
    assert ft.height > 10  # observed 408 free throws after a substitution
    row = ft.row(0, named=True)
    came_in = next(x for x in row["_own"] if x not in row["_foul"])
    went_out = next(x for x in row["_foul"] if x not in row["_own"])
    plays = _plays(lg)
    unruled = plays.with_columns(
        pl.when(pl.col("event_type") == "foul")
        .then(pl.lit("foul_hidden"))
        .otherwise(pl.col("event_type"))
        .alias("event_type")
    )
    keys = ["game_id", "player_id"]
    a = epv_player_games(plays, _tens(lg)).select(*keys, "epv_on")
    b = epv_player_games(unruled, _tens(lg)).select(*keys, pl.col("epv_on").alias("_unruled"))
    j = a.join(b, on=keys, how="full", coalesce=True).fill_null(0.0).filter(pl.col("game_id") == row["game_id"])
    moved = dict(zip(j["player_id"].to_list(), (j["epv_on"] - j["_unruled"]).to_list()))
    assert not np.isclose(moved[came_in], 0.0) and not np.isclose(moved[went_out], 0.0)
    assert np.isclose(sum(moved.values()), 0.0)  # the free throws move between players, never vanish


def test_own_actions_credit_the_actor() -> None:
    """Shooters get the play's EPV added, defensive rebounders its negative, stealers the turnover's negative."""
    p = _plays("nba").sort("game_id", "order_index")
    own = nba_epv._own_credit(p)
    assert own.height > 10_000
    p = p.with_columns(pl.col("epv_added").shift(1).over("game_id").alias("_prev"))
    shot = p.filter((pl.col("event_type") == "made_shot") & (pl.col("person_id") > 0)).row(0, named=True)
    dreb = p.filter(
        (pl.col("event_type") == "rebound")
        & (pl.col("team_id") == pl.col("defense_team_id"))
        & (pl.col("person_id") != pl.col("team_id"))
    ).row(0, named=True)
    steal = p.filter(pl.col("description").str.contains("STEAL")).row(0, named=True)
    for ev, want in (
        (shot, shot["epv_added"]),
        (dreb, -dreb["epv_added"]),
        (steal, -steal["_prev"]),
    ):
        got = own.filter((pl.col("game_id") == ev["game_id"]) & (pl.col("player_id") == ev["person_id"]))["_credit"]
        assert np.isclose(got, want).any()
    pg = epv_player_games(p.drop("_prev"))
    assert (pg["own_actions"] >= 0).all() and pg["player_id"].dtype == pl.Int64


@pytest.mark.parametrize("lg", list(LEAGUE))
def test_events_carry_the_possession_lineup(lg: str) -> None:
    """Each state carries its possession's starting ten; the pbp's own slots (the NBA's) are not read."""
    poss, _ = _raw(lg)
    s = _states(lg).filter(pl.col("step") > 0).select(*KEY, *SLOTS)
    j = s.join(poss.select(*KEY, *[pl.col(c).alias(f"_{c}") for c in SLOTS]), on=KEY)
    assert all((j[c] == j[f"_{c}"]).fill_null(True).all() for c in SLOTS)


def test_inputs_are_checked() -> None:
    poss, pbp = _raw("nba")
    with pytest.raises(ValueError, match="missing columns"):
        epv_states(poss.drop("possession_start_type"), pbp)
    with pytest.raises(ValueError, match="league_id"):
        load_epv_model("10")  # the WNBA booster is not shipped
    first = min(int(x) for x in load_epv_model().attr("train_seasons").split(","))
    with pytest.raises(ValueError, match=f"served from the {first} season"):
        nba_epv.nba_epv_plays(first - 1)

    class _NoAttr:
        def attr(self, _: str) -> None:
            return None

    with pytest.raises(ValueError, match="start_epv_mean"):
        add_epv(_states("nba"), booster=_NoAttr())  # type: ignore[arg-type]
    empty = add_epv(_states("nba").head(0))
    assert empty.height == 0 and "epv_added" in empty.columns
    with pytest.raises(ValueError, match="no first event"):
        first_events = poss.select("game_id", pl.col("start_order_index").alias("order_index"))
        epv_states(poss, pbp.join(first_events.head(1), on=["game_id", "order_index"], how="anti"))
    with pytest.raises(pl.exceptions.InvalidOperationError):
        epv_states(poss.with_columns(pl.lit("OffSomethingNew").alias("possession_start_type")), pbp)


def test_the_booster_reads_the_features_in_order() -> None:
    """``inplace_predict`` on an array never checks names, so the loader does."""
    booster = load_epv_model()
    assert list(booster.feature_names or []) == EPV_FEATURES
    swapped = booster.copy()
    swapped.feature_names = [*EPV_FEATURES[1:], EPV_FEATURES[0]]
    with pytest.raises(ValueError, match="reads"):
        nba_epv._check_features(swapped)
    with pytest.raises(ValueError, match="reads"):
        add_epv(_states("nba"), booster=swapped)


def test_shot_zones_are_coded_and_matter() -> None:
    """Every zone occurs; a rim miss leaves more EPV than a missed three (more putbacks)."""
    s = _plays("nba")
    counts = dict(
        s.filter(pl.col("event_type").is_in(["made_shot", "missed_shot"])).group_by("shot_zone").len().iter_rows()
    )
    # observed on the 40 games: rim 1,829, short 1,684, mid 582, three 3,063 field-goal attempts
    assert all(counts.get(z, 0) > 300 for z in (1, 2, 3, 4)) and 0 not in counts
    miss = s.filter(pl.col("event_type") == "missed_shot").group_by("shot_zone").agg(pl.col("epv").mean())
    by = dict(miss.iter_rows())
    assert by[1] > by[4] + 0.1  # observed a 0.20 gap
    threes = s.filter(pl.col("event_type").is_in(["made_shot", "missed_shot"]) & (pl.col("shot_zone") == 4))
    assert threes["description"].str.contains("3PT").mean() > 0.99


def test_the_transition_window_closes_at_a_dead_ball() -> None:
    """A timeout, foul, violation, substitution or team rebound sets the defense: the window shuts."""
    s = _states("nba").filter(pl.col("step") > 0)
    dead = pl.col("event_type").is_in(["timeout", "foul", "violation", "jump_ball", "substitution", "replay"]) | (
        (pl.col("event_type") == "rebound") & (pl.col("person_id") == pl.col("team_id"))
    )
    s = s.with_columns((dead.cast(pl.Int32).cum_max().over(KEY) > 0).alias("_stopped"))
    assert s.filter(pl.col("_stopped") & (pl.col("transition") == 1)).height == 0
    assert s.filter(pl.col("transition") == 1).height > 500  # observed 1,084: the window still opens
    assert s.filter(pl.col("transition") == 2).height > 2000  # observed 4,209 transition-possession states


def test_team_rebounds_are_dead_balls_of_the_right_side() -> None:
    """The feed files a team rebound under team_id 0 with the team in person_id; it is read as that team's."""
    s = _states("nba").filter(pl.col("event_type") == "rebound")
    team = s.filter(pl.col("person_id") == pl.col("team_id"))
    assert team.height > 300  # observed 681 team rebounds in the 40 games
    codes = {e: nba_epv.LAST_EVENTS.index(e) for e in ("off_team_rebound", "def_rebound", "free_throw")}
    off = team.filter(pl.col("team_id") == pl.col("offense_team_id"))
    assert off.height > 50 and off["last_event"].is_in([codes["off_team_rebound"], codes["free_throw"]]).all()
    assert (team.filter(pl.col("team_id") == pl.col("defense_team_id"))["last_event"] == codes["def_rebound"]).all()
    between = s.filter(pl.col("ft_pending") > 0)
    assert between.height > 50 and (between["last_event"] == codes["free_throw"]).all()  # the trip goes on


def _same(a: pl.DataFrame, b: pl.DataFrame) -> None:
    """Equal up to row order and float summation order (group sums run in parallel)."""
    keys = [c for c in a.columns if a.schema[c] != pl.Float64]
    assert_frame_equal(a.sort(keys), b.sort(keys), check_exact=False)


def test_season_loaders_take_the_start_year(monkeypatch: pytest.MonkeyPatch) -> None:
    """The NBA loaders are keyed by the start year: season 2025 reads the 2024 asset."""
    from sportsdataverse.nba import nba_loaders

    seen: dict[str, list[int]] = {}

    def fake(which: str):  # type: ignore[no-untyped-def]
        def f(seasons: list[int]) -> pl.DataFrame:
            seen[which] = list(seasons)
            poss, pbp = _raw("nba")
            return {"pbp": pbp, "poss": poss, "tens": _tens("nba")}[which]

        return f

    monkeypatch.setattr(nba_loaders, "load_nba_stats_pbp", fake("pbp"))
    monkeypatch.setattr(nba_loaders, "load_nba_stats_possessions", fake("poss"))
    monkeypatch.setattr(nba_loaders, "load_nba_stats_game_lineups", fake("tens"))
    t = nba_epv.nba_epv_team_games(HOLDOUT)
    assert seen["pbp"] == [HOLDOUT - 1] and seen["poss"] == [HOLDOUT - 1] and "tens" not in seen
    assert t.height == 80
    pg = nba_epv.nba_epv_player_games(HOLDOUT)
    assert seen["tens"] == [HOLDOUT - 1]
    _same(pg, epv_player_games(_plays("nba"), _tens("nba")))
    _same(nba_epv.nba_epv_lineup_games(HOLDOUT), epv_lineup_games(_plays("nba"), _tens("nba")))
    assert nba_epv.nba_epv_plays(HOLDOUT, return_as_pandas=True).shape[0] == _plays("nba").height
