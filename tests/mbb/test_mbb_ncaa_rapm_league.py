"""Tests for the league-wide NCAA RAPM solver (Path B).

``aggregate_stints`` collapses id-resolved possessions (the output of
``mbb_ncaa_rapm_input.resolve_possessions``) into matchup stints;
``solve_rapm_league`` runs one weighted sparse joint O/D ridge over them.

The solver tests assert IDENTIFIABLE quantities (within-lineup differences,
orderings, sign conventions, weighting equivalences) rather than absolute
coefficient values -- under a ridge penalty absolute values are shrunk, but
the difference between two players who swap into an otherwise identical
lineup is pinned by the data. The external correctness contract (Torvik
team-aggregate Spearman) runs on real seasons in the gate phase, not here.
"""

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_rapm_league import (
    STINT_SCHEMA,
    aggregate_stints,
    solve_rapm_league,
    team_aggregate,
)

# Ten id slots in resolve_possessions' output naming: "<slot>_id".
_SLOT_IDS = [f"{side}_{i}_id" for side in ("home", "away") for i in range(1, 6)]


def _poss(rows):
    """Build a minimal id-resolved possessions frame.

    Each row: (home, away, poss_team, pts, home_ids[5], away_ids[5]).
    """
    out = {
        "home": [],
        "away": [],
        "poss_team": [],
        "pts": [],
        **{c: [] for c in _SLOT_IDS},
    }
    for home, away, poss_team, pts, home_ids, away_ids in rows:
        out["home"].append(home)
        out["away"].append(away)
        out["poss_team"].append(poss_team)
        out["pts"].append(pts)
        for i in range(5):
            out[f"home_{i + 1}_id"].append(home_ids[i])
            out[f"away_{i + 1}_id"].append(away_ids[i])
    return pl.DataFrame(
        out,
        schema_overrides={c: pl.Utf8 for c in _SLOT_IDS},
    )


_H = ["h1", "h2", "h3", "h4", "h5"]
_A = ["a1", "a2", "a3", "a4", "a5"]


class TestAggregateStints:
    def test_collapses_identical_matchups(self):
        d = _poss(
            [
                ("Duke", "Iowa", "Duke", 2, _H, _A),
                ("Duke", "Iowa", "Duke", 0, _H, _A),
            ]
        )
        s = aggregate_stints(d)
        assert s.height == 1
        assert s["n_poss"][0] == 2
        assert s["pts"][0] == 2

    def test_offense_is_the_poss_team_side(self):
        d = _poss([("Duke", "Iowa", "Iowa", 3, _H, _A)])
        s = aggregate_stints(d)
        assert sorted(s["off_ids"][0].to_list()) == _A
        assert sorted(s["def_ids"][0].to_list()) == _H
        assert s["is_home_offense"][0] is False
        assert s["off_team"][0] == "Iowa"
        assert s["def_team"][0] == "Duke"

    def test_slot_order_does_not_split_a_lineup(self):
        d = _poss(
            [
                ("Duke", "Iowa", "Duke", 2, _H, _A),
                ("Duke", "Iowa", "Duke", 1, list(reversed(_H)), list(reversed(_A))),
            ]
        )
        s = aggregate_stints(d)
        assert s.height == 1
        assert s["n_poss"][0] == 2

    def test_home_and_away_offense_are_separate_stints(self):
        d = _poss(
            [
                ("Duke", "Iowa", "Duke", 2, _H, _A),
                ("Duke", "Iowa", "Iowa", 2, _H, _A),
            ]
        )
        s = aggregate_stints(d)
        assert s.height == 2
        assert set(s["is_home_offense"].to_list()) == {True, False}

    def test_null_slot_id_drops_the_possession(self):
        broken = _H[:4] + [None]
        d = _poss(
            [
                ("Duke", "Iowa", "Duke", 2, broken, _A),
                ("Duke", "Iowa", "Duke", 1, _H, _A),
            ]
        )
        s = aggregate_stints(d)
        assert s["n_poss"].sum() == 1

    def test_poss_team_matching_neither_side_is_dropped(self):
        d = _poss([("Duke", "Iowa", "Kansas", 2, _H, _A)])
        s = aggregate_stints(d)
        assert s.height == 0

    def test_empty_input_returns_documented_schema(self):
        s = aggregate_stints(_poss([]))
        assert s.height == 0
        assert dict(s.schema) == dict(STINT_SCHEMA)

    def test_ids_stay_utf8_lists(self):
        d = _poss([("Duke", "Iowa", "Duke", 2, _H, _A)])
        s = aggregate_stints(d)
        assert s.schema["off_ids"] == pl.List(pl.Utf8)
        assert s.schema["def_ids"] == pl.List(pl.Utf8)

    def test_missing_required_column_raises(self):
        with pytest.raises(ValueError, match="poss_team"):
            aggregate_stints(pl.DataFrame({"home": ["Duke"]}))


def _balanced_stints(ppp_starter=1.0, ppp_sub=1.0, n=200):
    """Two Duke lineups (h5 vs h6 swapped) against the same Iowa five.

    Every stint is away-offense-free and home-offense-only, so the ONLY
    contrast in the design is h5-vs-h6; their orapm difference is pinned by
    ``ppp_starter - ppp_sub`` (per 100: x100) while everything shared shrinks
    identically.
    """
    starters = _H
    subbed = _H[:4] + ["h6"]
    rows = []
    for _ in range(n):
        rows.append(("Duke", "Iowa", "Duke", None, starters, _A))
        rows.append(("Duke", "Iowa", "Duke", None, subbed, _A))
    d = _poss([(h, a, p, 0, hi, ai) for h, a, p, _, hi, ai in rows])
    s = aggregate_stints(d)
    # overwrite pts to the generative model: pts = ppp * n_poss
    return s.with_columns(
        pl.when(pl.col("off_ids").list.contains("h6"))
        .then(pl.col("n_poss") * ppp_sub)
        .otherwise(pl.col("n_poss") * ppp_starter)
        .cast(pl.Int64)
        .alias("pts")
    )


class TestSolveRapmLeague:
    def test_recovers_a_known_within_lineup_contrast(self):
        # h5's lineups score 2.0 ppp, h6's 1.0 ppp -> orapm(h5) - orapm(h6)
        # approaches 100 per 100 possessions as lambda -> 0.
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0)
        players, _info = solve_rapm_league(s, ridge_lambda=1e-6)
        o = dict(zip(players["player_id"].to_list(), players["orapm"].to_list()))
        assert o["h5"] > o["h6"]
        assert abs((o["h5"] - o["h6"]) - 100.0) < 1.0

    def test_defense_sign_convention_positive_is_good(self):
        # Same offense, two defenses: the one allowing fewer points must get
        # the HIGHER drapm.
        good_def = _A
        bad_def = _A[:4] + ["a6"]
        rows = []
        for _ in range(200):
            rows.append(("Duke", "Iowa", "Duke", 1, _H, good_def))
            rows.append(("Duke", "Iowa", "Duke", 2, _H, bad_def))
        s = aggregate_stints(_poss(rows))
        players, _info = solve_rapm_league(s, ridge_lambda=1e-6)
        d = dict(zip(players["player_id"].to_list(), players["drapm"].to_list()))
        assert d["a5"] > d["a6"]
        assert abs((d["a5"] - d["a6"]) - 100.0) < 1.0

    def test_weighting_equivalence_duplicated_stint_equals_doubled_n_poss(self):
        s1 = _balanced_stints(ppp_starter=1.5, ppp_sub=0.5, n=50)
        # doubling every stint's weight...
        s2 = s1.with_columns((pl.col("n_poss") * 2).alias("n_poss"), (pl.col("pts") * 2).alias("pts"))
        p1, _ = solve_rapm_league(s1, ridge_lambda=100.0)
        p2, _ = solve_rapm_league(s2, ridge_lambda=200.0)
        # lambda scales with total weight for exact equivalence
        j = p1.join(p2, on="player_id", suffix="_2")
        assert (j["orapm"] - j["orapm_2"]).abs().max() < 1e-6

    def test_larger_lambda_shrinks_coefficients(self):
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0)
        loose, _ = solve_rapm_league(s, ridge_lambda=1.0)
        tight, _ = solve_rapm_league(s, ridge_lambda=1e5)
        assert tight["orapm"].abs().max() < loose["orapm"].abs().max()

    def test_hca_column_absorbs_a_home_offense_boost(self):
        # identical matchup, home offense scores more -> hca > 0.
        rows = []
        for _ in range(200):
            rows.append(("Duke", "Iowa", "Duke", 2, _H, _A))
            rows.append(("Duke", "Iowa", "Iowa", 1, _H, _A))
        s = aggregate_stints(_poss(rows))
        _players, info = solve_rapm_league(s, ridge_lambda=1e-6)
        assert info["hca"] > 0

    def test_info_reports_scale_and_size(self):
        s = _balanced_stints(ppp_starter=1.0, ppp_sub=1.0, n=10)
        _players, info = solve_rapm_league(s, ridge_lambda=100.0)
        assert info["n_poss"] == s["n_poss"].sum()
        assert info["n_stints"] == s.height
        assert abs(info["intercept"] - 100.0) < 1e-9  # 1.0 ppp -> 100 per 100
        assert info["ridge_lambda"] == 100.0

    def test_possession_exposure_columns(self):
        s = _balanced_stints(n=10)
        players, _ = solve_rapm_league(s, ridge_lambda=100.0)
        row = players.filter(pl.col("player_id") == "a1")
        # a1 defends every possession and never attacks
        assert row["def_poss"][0] == s["n_poss"].sum()
        assert row["off_poss"][0] == 0

    def test_empty_stints_return_empty_frame(self):
        players, info = solve_rapm_league(aggregate_stints(_poss([])), ridge_lambda=100.0)
        assert players.height == 0
        assert set(players.columns) == {
            "player_id",
            "orapm",
            "drapm",
            "rapm_net",
            "off_poss",
            "def_poss",
        }
        assert info["n_stints"] == 0

    def test_net_is_o_plus_d(self):
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0, n=50)
        players, _ = solve_rapm_league(s, ridge_lambda=50.0)
        assert (players["rapm_net"] - (players["orapm"] + players["drapm"])).abs().max() < 1e-12

    def test_return_as_pandas(self):
        import pandas as pd

        s = _balanced_stints(n=5)
        players, _ = solve_rapm_league(s, ridge_lambda=100.0, return_as_pandas=True)
        assert isinstance(players, pd.DataFrame)


def _players(**net):
    """players frame with orapm = value, drapm = 0 for each id."""
    ids = sorted(net)
    return pl.DataFrame(
        {
            "player_id": ids,
            "orapm": [float(net[p]) for p in ids],
            "drapm": [0.0] * len(ids),
            "rapm_net": [float(net[p]) for p in ids],
            "off_poss": [0] * len(ids),
            "def_poss": [0] * len(ids),
        }
    )


class TestTeamAggregate:
    def test_weighted_mean_of_on_floor_sums(self):
        # Duke offense: stint A (sum orapm 5.0, 30 poss), stint B (sum 0.0,
        # 10 poss) -> team_orapm = (5*30 + 0*10) / 40 = 3.75.
        rows = [("Duke", "Iowa", "Duke", 0, _H, _A)] * 30 + [("Duke", "Iowa", "Duke", 0, _H[:4] + ["h6"], _A)] * 10
        s = aggregate_stints(_poss(rows))
        players = _players(h1=1, h2=1, h3=1, h4=1, h5=1, h6=-4)
        t = team_aggregate(s, players)
        duke = t.filter(pl.col("team") == "Duke")
        assert abs(duke["team_orapm"][0] - 3.75) < 1e-12

    def test_defense_side_uses_drapm(self):
        rows = [("Duke", "Iowa", "Duke", 0, _H, _A)] * 10
        s = aggregate_stints(_poss(rows))
        players = _players(h1=0, h2=0, h3=0, h4=0, h5=0)
        players = pl.concat(
            [
                players,
                pl.DataFrame(
                    {
                        "player_id": _A,
                        "orapm": [0.0] * 5,
                        "drapm": [2.0] * 5,
                        "rapm_net": [2.0] * 5,
                        "off_poss": [0] * 5,
                        "def_poss": [0] * 5,
                    }
                ),
            ]
        )
        t = team_aggregate(s, players)
        iowa = t.filter(pl.col("team") == "Iowa")
        assert abs(iowa["team_drapm"][0] - 10.0) < 1e-12
        assert abs(iowa["team_net"][0] - 10.0) < 1e-12

    def test_player_missing_from_ratings_counts_zero(self):
        rows = [("Duke", "Iowa", "Duke", 0, _H, _A)] * 5
        s = aggregate_stints(_poss(rows))
        players = _players(h1=5)  # h2..h5, a1..a5 absent
        t = team_aggregate(s, players)
        duke = t.filter(pl.col("team") == "Duke")
        assert abs(duke["team_orapm"][0] - 5.0) < 1e-12

    def test_empty_stints_return_documented_schema(self):
        t = team_aggregate(aggregate_stints(_poss([])), _players())
        assert t.height == 0
        assert set(t.columns) == {
            "team",
            "team_orapm",
            "team_drapm",
            "team_net",
            "off_poss",
            "def_poss",
        }
