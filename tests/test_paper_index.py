"""Paper Index port: parity with Game on Paper's trainer and module on real games.

M-LUCK evidence, in three layers (fixture provenance in tests/fixtures/paper_index/README.md):

* the trainer's 24 real holdout games (12 per league) replayed from their inputs
  through the port's scoring, to GOP's own tolerance (5e-4, the 4-decimal rounding
  of the shipped weights);
* the same games' released pbp rows through the port end to end, against GOP's
  ``paper_index.compute`` on the same rows (and, for the NFL, whose rows are the
  ones the fit read, against the trainer's inputs too);
* the deserved-wins roll-up on a hand-computed case and on those games.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from importlib.resources import files
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.paper_index import (
    DESERVED_WINS_SCHEMA,
    GAMES_SCHEMA,
    HOLDOUT_SEASONS,
    LEAGUE_PTS_PER_OPP,
    MARGINS,
    PBP_COLUMNS,
    TRAIN_SEASONS,
    WEIGHTS,
    _score,
    _team_inputs,
    deserved_wins,
    paper_index_game,
    paper_index_games,
)

FIX = Path(__file__).parent / "fixtures" / "paper_index"
LEAGUES = ("cfb", "nfl")
ORACLE = {lg: json.loads((FIX / name).read_text()) for lg, name in
          (("cfb", "paper_index_oracle.json"), ("nfl", "paper_index_oracle_nfl.json"))}  # fmt: skip
GOP = {lg: json.loads((FIX / f"gop_compute_{lg}.json").read_text())["games"] for lg in LEAGUES}
SHARE_TOL = 5e-4  # GOP test_oracle_real_holdout_games: 4-decimal weights
EXACT = 1e-9  # same weights, same rows: GOP's train/serve parity_check tolerance

# GOP's camelCase input names -> the port's columns
INPUT_NAMES = {
    "successRate": "success_rate",
    "explosiveRate": "explosive_rate",
    "explosivenessEpa": "explosiveness_epa",
    "oppConversion": "opp_conversion",
    "ptsPerOpp": "pts_per_opp",
    "avgStartEp": "avg_start_ep",
    "havocAllowedRate": "havoc_allowed_rate",
    "turnoversCommitted": "turnovers_committed",
    "avgStartYardsToEndzone": "avg_start_yards_to_endzone",
}


@pytest.fixture(scope="module", params=LEAGUES)
def league(request) -> str:
    return request.param


def _pbp(league: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"oracle_games_pbp_{league}.parquet")


def _replay(league: str) -> pl.DataFrame:
    """The trainer's fixture inputs, home side beside away, through the port's scoring."""
    rows = [
        {
            **{INPUT_NAMES[k]: v for k, v in g["home"].items()},
            **{f"{INPUT_NAMES[k]}_opp": v for k, v in g["away"].items()},
            "expected": g["expectedHomeShare"],
        }
        for g in ORACLE[league]["games"]
    ]
    return _score(pl.DataFrame(rows), league)


# ------------------------------------------------------------ lineage


def test_shipped_constants_are_the_trainers(league):
    """Never-lower/lineage: WEIGHTS are the fixture's fitted weights at 4 decimals, the
    no-opportunity neutral is the trainer's, and the honest spans are the fit's split."""
    fx = ORACLE[league]
    prov = fx["provenance"]
    assert prov["league"] == league and prov["script"] == "python/tools/fit_paper_index.py"
    assert set(WEIGHTS[league]) == set(fx["weights"]) == set(MARGINS)
    for k, v in fx["weights"].items():
        assert WEIGHTS[league][k] == round(v, 4), (league, k, v)
        assert WEIGHTS[league][k] > 0 or k in prov["margins_pinned_to_zero"]
    assert LEAGUE_PTS_PER_OPP[league] == prov["league_pts_per_opp"]
    first, last = (int(s) for s in prov["train_seasons"].split("-"))
    assert TRAIN_SEASONS[league] == (first, last)
    assert HOLDOUT_SEASONS[league] == (min(prov["holdout_seasons"]), max(prov["holdout_seasons"]))
    assert TRAIN_SEASONS[league][1] < HOLDOUT_SEASONS[league][0]  # disjoint, holdout after train


def test_bundled_curve_is_the_fitted_curve(league):
    """The weights were fitted against one field-position curve; the bundled parquet must
    be that file (the fixture inputs carry avgStartEp precomputed and would not notice)."""
    want = ORACLE[league]["provenance"]["fp_curve"]
    raw = (files(f"sportsdataverse.{league}") / "models" / f"{league}_field_position_ep.parquet").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == want["sha256"]


# ------------------------------------------------------------ oracle replay


def test_oracle_holdout_games_replay(league):
    """The port reproduces the trainer's share for its 12 real holdout games."""
    out = _replay(league)
    assert out.height == 12
    err = (out["paper_share"] - out["expected"]).abs()
    assert err.max() < SHARE_TOL, (league, err.max())
    assert ((out["paper_share"] + out["opp_share"] - 1.0).abs() < 1e-12).all()
    # the fixture spans the range, so a sign flip on any margin cannot hide
    assert out["expected"].min() < 0.15 and out["expected"].max() > 0.85


def test_replay_fails_on_an_edited_weight(league, monkeypatch):
    """The replay can go red: one weight off by 5% moves some share past the tolerance."""
    edited = {**WEIGHTS[league], "havoc": WEIGHTS[league]["havoc"] * 1.05}
    monkeypatch.setitem(WEIGHTS, league, edited)
    out = _replay(league)
    assert (out["paper_share"] - out["expected"]).abs().max() > SHARE_TOL


# ------------------------------------------------------------ real rows


def test_real_rows_match_gop_compute(league):
    """Released pbp rows -> the port, against GOP's paper_index.compute on the same rows:
    every input, margin and share, per game and in the batch."""
    pbp = _pbp(league)
    batch = paper_index_games(pbp, league)
    assert batch["game_id"].n_unique() == 12 and batch.height == 24
    inputs = _team_inputs(pbp, league, ["game_id", "pos_team_id"])
    for g in GOP[league]:
        gid = int(g["gameId"])
        one = paper_index_game(pbp.filter(pl.col("game_id") == gid), g["homeId"], g["awayId"], league)
        assert one is not None
        assert abs(one["home_share"] - g["homeShare"]) < EXACT, (league, gid)
        assert abs(one["away_share"] - (1.0 - g["homeShare"])) < EXACT
        assert set(one["margins"]) == set(g["margins"]) == set(MARGINS)
        for m, v in g["margins"].items():
            assert abs(one["margins"][m] - v) < EXACT, (league, gid, m)
        home = batch.filter((pl.col("game_id") == gid) & (pl.col("team_id") == g["homeId"])).row(0, named=True)
        assert abs(home["paper_share"] - g["homeShare"]) < EXACT
        for m, v in g["margins"].items():
            assert abs(home[f"{m}_margin"] - v) < EXACT
        for side in ("home", "away"):
            got = inputs.filter((pl.col("game_id") == gid) & (pl.col("pos_team_id") == g[f"{side}Id"]))
            for k, v in g[side].items():
                assert abs(got[INPUT_NAMES[k]][0] - v) < EXACT, (league, gid, side, k)


def test_nfl_real_rows_reproduce_the_trainer():
    """The NFL rows are the ones the fit read: the port's inputs equal the trainer's
    aggregation, and its shares the trainer's -- end to end, pbp to share."""
    pbp = _pbp("nfl")
    # the field-goal rule is exercised: made FGs on non-scrimmage rows flagged scoring_opp
    fg_rows = pbp.filter((pl.col("fg_made") == True) & (pl.col("scoring_opp") == True)  # noqa: E712
                         & (pl.col("scrimmage_play") == False))  # noqa: E712  # fmt: skip
    assert fg_rows.height >= 10
    inputs = _team_inputs(pbp, "nfl", ["game_id", "pos_team_id"])
    batch = paper_index_games(pbp, "nfl")
    for g, gop in zip(ORACLE["nfl"]["games"], GOP["nfl"]):
        gid = int(g["gameId"])
        for side in ("home", "away"):
            got = inputs.filter((pl.col("game_id") == gid) & (pl.col("pos_team_id") == gop[f"{side}Id"]))
            for k, v in g[side].items():
                assert abs(got[INPUT_NAMES[k]][0] - v) < EXACT, (gid, side, k)
        share = batch.filter((pl.col("game_id") == gid) & (pl.col("team_id") == gop["homeId"]))["paper_share"][0]
        assert abs(share - g["expectedHomeShare"]) < SHARE_TOL


def test_batch_rows_are_two_mirrored_team_views(league):
    pbp = _pbp(league)
    out = paper_index_games(pbp, league)
    ids = {"game_id": pbp.schema["game_id"], "team_id": pbp.schema["pos_team_id"]}
    assert out.schema == pl.Schema({**ids, **GAMES_SCHEMA})
    assert out.select("game_id", "team_id").is_unique().all()
    assert out.equals(out.sort("game_id", "team_id"))
    by_game = out.group_by("game_id").agg(
        share_sum=pl.col("paper_share").sum(),
        won=pl.col("won").sum(),
        **{m: pl.col(f"{m}_margin").sum() for m in MARGINS},
    )
    assert (by_game["won"] == 1).all()
    assert ((by_game["share_sum"] - 1.0).abs() < 1e-12).all()
    for m in MARGINS:  # each margin is the other row's, negated
        assert (by_game[m].abs() < 1e-12).all(), m
    assert ((out["paper_share"] + out["opp_share"] - 1.0).abs() < 1e-12).all()
    # won agrees with the final score
    finals = {int(g["gameId"]): g for g in ORACLE[league]["games"]}
    home_ids = {int(g["gameId"]): g["homeId"] for g in GOP[league]}
    for r in out.iter_rows(named=True):
        g = finals[r["game_id"]]
        home_won = g["homeScore"] > g["awayScore"]
        assert r["won"] == (home_won if r["team_id"] == home_ids[r["game_id"]] else not home_won)


def test_string_ids_stay_strings():
    pbp = _pbp("cfb").with_columns(
        pl.col(c).cast(pl.Utf8) for c in ("game_id", "pos_team_id", "homeTeamId", "awayTeamId")
    )
    out = paper_index_games(pbp, "cfb")
    assert out.schema["game_id"] == pl.Utf8 and out.schema["team_id"] == pl.Utf8
    assert out.height == 24
    ref = paper_index_games(_pbp("cfb"), "cfb")
    back = out.with_columns(pl.col("game_id", "team_id").cast(pl.Int64))
    joined = ref.join(back, on=["game_id", "team_id"], how="inner", suffix="_str")
    assert joined.height == 24
    assert (joined["paper_share"] - joined["paper_share_str"]).abs().max() < 1e-12


def test_row_order_does_not_matter(league):
    """Drive starts are taken by play order, so shuffled rows score the same games."""
    pbp = _pbp(league)
    shuffled = pbp.sample(fraction=1.0, shuffle=True, seed=7)
    assert not shuffled.equals(pbp)
    ref = paper_index_games(pbp, league)
    got = paper_index_games(shuffled, league)
    assert got.select("game_id", "team_id").equals(ref.select("game_id", "team_id"))
    assert (got["paper_share"] - ref["paper_share"]).abs().max() < 1e-12
    g = GOP[league][0]
    one = paper_index_game(shuffled.filter(pl.col("game_id") == int(g["gameId"])), g["homeId"], g["awayId"], league)
    assert abs(one["home_share"] - g["homeShare"]) < EXACT


def test_ids_never_through_float():
    pbp = _pbp("cfb")
    with pytest.raises(TypeError, match="never float"):
        paper_index_games(pbp.with_columns(pl.col("pos_team_id").cast(pl.Float64)), "cfb")
    with pytest.raises(TypeError, match="never float"):
        paper_index_game(pbp.with_columns(pl.col("pos_team_id").cast(pl.Float64)), 1, 2, "cfb")
    with pytest.raises(TypeError, match="homeTeamId"):
        paper_index_games(pbp.with_columns(pl.col("homeTeamId").cast(pl.Utf8)), "cfb")


def test_population_filters():
    """Incomplete games, ties, a side under the snap floor and (NFL) the Pro Bowl drop out;
    a side AT the floor stays."""
    pbp = _pbp("nfl")
    gids = pbp["game_id"].unique(maintain_order=True).to_list()
    last = pl.col("game_play_number") == pl.col("game_play_number").max().over("game_id")
    g_inc, g_tie, g_snap, g_pro, g_floor = gids[:5]
    away = pbp.filter(pl.col("game_id") == g_snap)["awayTeamId"][0]
    away_floor = pbp.filter(pl.col("game_id") == g_floor)["awayTeamId"][0]
    home_pro, away_pro = pbp.filter(pl.col("game_id") == g_pro).select("homeTeamId", "awayTeamId").row(0)
    snap_n = pl.int_range(pl.len()).over("game_id", "pos_team_id", "scrimmage_play")
    edited = (
        pbp.with_columns(
            status_type_completed=pl.when((pl.col("game_id") == g_inc) & last).then(False)
            .otherwise(pl.col("status_type_completed")),
            awayScore=pl.when((pl.col("game_id") == g_tie) & last).then(pl.col("homeScore"))
            .otherwise(pl.col("awayScore")),
        )
        .filter(~((pl.col("game_id") == g_snap) & (pl.col("pos_team_id") == away)
                  & (pl.col("scrimmage_play") == True) & (snap_n >= 19)))  # noqa: E712
        .filter(~((pl.col("game_id") == g_floor) & (pl.col("pos_team_id") == away_floor)
                  & (pl.col("scrimmage_play") == True) & (snap_n >= 20)))  # noqa: E712
        .with_columns(
            pl.when(pl.col("game_id") == g_pro).then(
                pl.col(c).replace({home_pro: 31, away_pro: 32})
            ).otherwise(pl.col(c)).alias(c)
            for c in ("homeTeamId", "awayTeamId", "pos_team_id")
        )
    )  # fmt: skip
    snaps = edited.filter(pl.col("scrimmage_play") == True).group_by("game_id", "pos_team_id").len()  # noqa: E712
    assert snaps.filter((pl.col("game_id") == g_snap) & (pl.col("pos_team_id") == away))["len"].to_list() == [19]
    assert snaps.filter((pl.col("game_id") == g_floor) & (pl.col("pos_team_id") == away_floor))["len"].to_list() == [20]
    kept = set(paper_index_games(edited, "nfl")["game_id"].unique().to_list())
    assert kept == set(gids) - {g_inc, g_tie, g_snap, g_pro}
    assert g_floor in kept
    # the Pro Bowl rule is NFL-only; the snap floor and ties are not
    kept_cfb_rules = set(paper_index_games(edited, "cfb")["game_id"].unique().to_list())
    assert g_pro in kept_cfb_rules and g_snap not in kept_cfb_rules and g_tie not in kept_cfb_rules


def test_empty_frames_carry_the_schema(league):
    pbp = _pbp(league)
    out = paper_index_games(pbp.head(0), league)
    assert out.height == 0
    assert out.schema == pl.Schema({"game_id": pl.Int64, "team_id": pl.Int64, **GAMES_SCHEMA})
    dw = deserved_wins(out)
    assert dw.height == 0
    assert dw.schema == pl.Schema({"season": pl.Int64, "team_id": pl.Int64,
                                   **{k: v for k, v in DESERVED_WINS_SCHEMA.items() if k != "season"}})  # fmt: skip


def test_bad_input_is_refused():
    pbp = _pbp("cfb")
    with pytest.raises(ValueError, match="league"):
        paper_index_games(pbp, "ufl")  # no fit of its own: no index, not another league's weights
    with pytest.raises(ValueError, match="missing columns"):
        paper_index_games(pbp.drop("drive.id"), "cfb")
    # fg_made is read only where the fit counts made field goals (the NFL)
    no_fg, ref = paper_index_games(pbp.drop("fg_made"), "cfb"), paper_index_games(pbp, "cfb")
    assert no_fg.select("game_id", "team_id").equals(ref.select("game_id", "team_id"))
    assert (no_fg["paper_share"] - ref["paper_share"]).abs().max() < 1e-12
    with pytest.raises(ValueError, match="fg_made"):
        paper_index_games(_pbp("nfl").drop("fg_made"), "nfl")
    with pytest.raises(ValueError, match="more than one game_id"):
        paper_index_game(pbp, 1, 2, "cfb")


def test_game_is_symmetric_and_fails_open():
    pbp = _pbp("cfb")
    g = GOP["cfb"][3]
    rows = pbp.filter(pl.col("game_id") == int(g["gameId"]))
    fwd = paper_index_game(rows, g["homeId"], g["awayId"], "cfb")
    rev = paper_index_game(rows, str(g["awayId"]), str(g["homeId"]), "cfb")  # ids as text work too
    assert abs(fwd["home_share"] - rev["away_share"]) < 1e-15
    assert all(abs(fwd["margins"][m] + rev["margins"][m]) < 1e-12 for m in MARGINS)
    assert paper_index_game(rows, 999999, g["awayId"], "cfb") is None  # a team with no snaps
    # same plays, other league: another fit, another share
    assert paper_index_game(rows, g["homeId"], g["awayId"], "nfl")["home_share"] != fwd["home_share"]


def test_no_opportunity_side_gets_the_neutral_fills(league):
    """A side with no scoring-opportunity trip finishes neutrally: 0.5 conversion and the
    league's train-season points per opportunity (no fixture side lacks a trip)."""
    pbp = _pbp(league)
    g = GOP[league][0]
    rows = pbp.filter(pl.col("game_id") == int(g["gameId"]))
    dry = rows.with_columns(
        scoring_opp=pl.when(pl.col("pos_team_id") == g["awayId"]).then(False).otherwise(pl.col("scoring_opp"))
    )
    got = _team_inputs(dry, league, ["pos_team_id"])
    away = got.filter(pl.col("pos_team_id") == g["awayId"]).row(0, named=True)
    home = got.filter(pl.col("pos_team_id") == g["homeId"]).row(0, named=True)
    assert away["opp_conversion"] == 0.5
    assert away["pts_per_opp"] == LEAGUE_PTS_PER_OPP[league]
    assert abs(home["pts_per_opp"] - g["home"]["ptsPerOpp"]) < EXACT  # the other side is untouched


def test_feed_loss_warns():
    """Losing more than 2% of decided games to missing inputs warns (the trainer's join floor)."""
    pbp = _pbp("nfl")
    gid = pbp["game_id"][0]
    broken = pbp.with_columns(
        pl.when(pl.col("game_id") == gid).then(None).otherwise(pl.col("drive.id")).alias("drive.id")
    )
    with pytest.warns(UserWarning, match="inputs computable for 11 of 12"):
        out = paper_index_games(broken, "nfl")
    assert gid not in out["game_id"].to_list()


# ------------------------------------------------------------ deserved wins


def test_deserved_wins_hand_computed():
    games = pl.DataFrame(
        {
            "season": [2024, 2024, 2024, 2024, 2025],
            "team_id": ["A", "A", "A", "B", "A"],
            "won": [True, True, False, False, True],
            "paper_share": [0.8, 0.3, 0.6, 0.25, 1.0],
        }
    )
    out = deserved_wins(games)
    a = out.filter((pl.col("season") == 2024) & (pl.col("team_id") == "A")).row(0, named=True)
    assert (a["games"], a["wins"]) == (3, 2)
    assert a["deserved_wins"] == pytest.approx(1.7, abs=1e-12)
    assert a["luck_wins"] == pytest.approx(0.3, abs=1e-12)
    # var = .8*.2 + .3*.7 + .6*.4 = 0.61
    assert a["luck_z"] == pytest.approx(0.3 / math.sqrt(0.61), abs=1e-12)
    b = out.filter(pl.col("team_id") == "B").row(0, named=True)
    assert (b["games"], b["wins"], b["luck_wins"]) == (1, 0, -0.25)
    assert b["luck_z"] == pytest.approx(-0.25 / math.sqrt(0.25 * 0.75), abs=1e-12)
    # every share exactly 1: zero variance, no z
    assert out.filter(pl.col("season") == 2025)["luck_z"].to_list() == [None]
    assert out.select("season", "team_id").rows() == [(2024, "A"), (2024, "B"), (2025, "A")]


def test_deserved_wins_on_the_oracle_games(league):
    games = paper_index_games(_pbp(league), league)
    out = deserved_wins(games)
    assert out["games"].sum() == games.height == 24
    assert out["wins"].sum() == 12  # one winner per game
    assert abs(out["deserved_wins"].sum() - 12.0) < 1e-9  # the shares of a game sum to 1
    assert abs(out["luck_wins"].sum()) < 1e-9
    per_team = games.group_by("season", "team_id").agg(pl.col("paper_share").sum().alias("p"))
    joined = out.join(per_team, on=["season", "team_id"])
    assert joined.height == out.height
    assert ((joined["deserved_wins"] - joined["p"]).abs() < 1e-12).all()


# ------------------------------------------------------------ full seasons (local files)


def _local_seasons(league: str, seasons) -> dict[int, pl.DataFrame]:
    """Released espn_{league}_pbp seasons from SDV_PY_ESPN_{LEAGUE}_PBP_DIR, or skip.

    Local files only: the release is republished in place, so these run against a
    pinned local copy (on the droplet: cfbfastR-cfb-data/cfb/pbp/parquet and
    nfl-data/out/espn_nfl/pbp)."""
    var = f"SDV_PY_ESPN_{league.upper()}_PBP_DIR"
    if not os.environ.get(var):
        pytest.skip(f"set {var} to a dir of espn_{league}_pbp play_by_play_{{season}}.parquet")
    paths = {int(s): Path(os.environ[var]) / f"play_by_play_{s}.parquet" for s in seasons}
    if not all(p.exists() for p in paths.values()):
        pytest.skip(f"missing espn_{league}_pbp seasons under {os.environ[var]}")
    return {s: pl.read_parquet(p, columns=list(PBP_COLUMNS)) for s, p in paths.items()}


def test_nfl_population_is_the_fits():
    """On the NFL seasons the fit read, the port scores exactly the fit's games per season
    (the trainer's per-season counts are in the fixture provenance)."""
    seasons = ORACLE["nfl"]["provenance"]["seasons"]
    for s, pbp in _local_seasons("nfl", seasons).items():
        games = paper_index_games(pbp, "nfl")
        assert games["game_id"].n_unique() == seasons[str(s)]["games"], s


def test_holdout_seasons_clear_the_fit_gates(league):
    """The holdout seasons through the port: pooled Brier under the fit's never-lower gate
    (GATES in fit_paper_index.py: cfb 0.09, nfl 0.13), and per season the luck_z of teams
    with 8+ games centred near 0 with spread near 1. Bounds from the values observed
    2026-10-03 (never lower them to pass): Brier cfb 0.0682 (rebuilt pbp), nfl 0.1174
    (= the fit's); |mean luck_z| <= 0.045; sd cfb 1.04/1.11, nfl 0.85-1.28."""
    prov = ORACLE[league]["provenance"]
    games = pl.concat(
        [paper_index_games(pbp, league) for pbp in _local_seasons(league, prov["holdout_seasons"]).values()]
    )
    brier = ((games["paper_share"] - games["won"].cast(pl.Float64)) ** 2).mean()
    assert brier < prov["gates"]["league"]["brier"], brier
    if league == "nfl":  # the fit's own pbp: its holdout Brier, reproduced
        assert round(brier, 4) == prov["holdout_brier"]
    luck = deserved_wins(games).filter(pl.col("games") >= 8)
    for s, z in luck.group_by("season").agg(pl.col("luck_z")).iter_rows():
        zs = pl.Series(z)
        assert abs(zs.mean()) < 0.15 and 0.8 < zs.std() < 1.35, (league, s, zs.mean(), zs.std())
