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

import numpy as np
import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_rapm_league import (
    STINT_SCHEMA,
    aggregate_stints,
    possession_deciles,
    solve_rapm_league,
    split_half_se_check,
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
            "orapm_se",
            "drapm_se",
            "rapm_net_se",
            "orapm_se_sampling",
            "drapm_se_sampling",
            "rapm_net_se_sampling",
        }
        assert info["n_stints"] == 0

    def test_nonconverged_solve_raises_not_returns_partial(self, monkeypatch):
        # lsqr can hit iter_lim (istop=7) and hand back a partial iterate with
        # no error; that must never flow silently into the gate.
        import scipy.sparse.linalg as sla

        real = sla.lsqr

        def fake(*a, **k):
            out = list(real(*a, **k))
            out[1] = 7  # istop: iteration limit reached
            return tuple(out)

        monkeypatch.setattr("scipy.sparse.linalg.lsqr", fake)
        s = _balanced_stints(n=5)
        with pytest.raises(RuntimeError, match="istop"):
            solve_rapm_league(s, ridge_lambda=100.0)

    def test_info_reports_solver_diagnostics(self):
        s = _balanced_stints(n=5)
        _players_frame, info = solve_rapm_league(s, ridge_lambda=100.0)
        # 0 = x=0 is the exact solution (this fixture has zero centered
        # signal), 1/2 = converged.
        assert info["lsqr_istop"] in (0, 1, 2)
        assert info["lsqr_itn"] >= 0

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

    def test_non_utf8_player_id_raises_not_silently_zeroes(self):
        # An Int64-keyed players frame would left-join to all-null -> every
        # coefficient fill_null(0.0) -> all-zero team ratings with no error.
        s = aggregate_stints(_poss([("Duke", "Iowa", "Duke", 2, _H, _A)]))
        bad = _players(h1=1).with_columns(pl.lit(1).alias("player_id"))
        with pytest.raises(TypeError, match="player_id"):
            team_aggregate(s, bad)

    def test_zero_overlap_between_stints_and_players_raises(self):
        s = aggregate_stints(_poss([("Duke", "Iowa", "Duke", 2, _H, _A)]))
        strangers = _players(x1=1, x2=2)
        with pytest.raises(ValueError, match="overlap"):
            team_aggregate(s, strangers)

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


# ---------------------------------------------------------------------------
# Posterior + sampling standard errors, and the two SE validations
# ---------------------------------------------------------------------------


_EXPOSURE = np.array([1.0, 1.0, 0.9, 0.9, 0.8, 0.5, 0.2])  # h1..h7 / a1..a7 selection weights


def _noisy_resolved(n_poss=4000, n_games=20, seed=0, h7_boost=0.25):
    """Id-resolved possessions with a ``contest_id`` and Poisson scoring noise.

    Duke (home) vs Iowa (away), each side fielding a random five of seven
    drawn with graded exposure weights (h1 ~ every possession, h7 rarely), so
    NO two players are perfectly collinear -- the always-together five of the
    deterministic fixtures above would pin the starters' SEs at the prior and
    invert every "more minutes, smaller SE" comparison. Duke's h7 lifts the
    offence by ``h7_boost`` ppp, so the design carries signal AND noise (the
    deterministic fixtures have near-zero residuals, where a residual-variance
    SE is degenerate).
    """
    rng = np.random.default_rng(seed)
    p = _EXPOSURE / _EXPOSURE.sum()
    hs, as_ = [f"h{i}" for i in range(1, 8)], [f"a{i}" for i in range(1, 8)]
    rows, contest = [], []
    for k in range(n_poss):
        duke = sorted(hs[i] for i in rng.choice(7, 5, replace=False, p=p))
        iowa = sorted(as_[i] for i in rng.choice(7, 5, replace=False, p=p))
        poss_team = "Duke" if k % 2 == 0 else "Iowa"
        ppp = 1.0 + (h7_boost if (poss_team == "Duke" and "h7" in duke) else 0.0)
        rows.append(("Duke", "Iowa", poss_team, int(min(rng.poisson(ppp), 3)), duke, iowa))
        contest.append(str(1000 + k % n_games))
    return _poss(rows).with_columns(pl.Series("contest_id", contest, dtype=pl.Utf8))


def _dense_system(s, lam):
    """Hand-built dense normal equations for a stint frame -- the formula the engine must match."""
    players = sorted(set(s["off_ids"].explode().to_list()) | set(s["def_ids"].explode().to_list()))
    idx = {p: i for i, p in enumerate(players)}
    n_p = len(players)
    x = np.zeros((s.height, 2 * n_p + 1))
    rows = zip(s["off_ids"].to_list(), s["def_ids"].to_list(), s["is_home_offense"].to_list())
    for r, (off, dfn, home) in enumerate(rows):
        for p in off:
            x[r, idx[p]] = 1.0
        for p in dfn:
            x[r, n_p + idx[p]] = -1.0
        x[r, 2 * n_p] = 1.0 if home else -1.0
    w = s["n_poss"].to_numpy().astype(float)
    y = 100.0 * s["pts"].to_numpy() / w
    yc = y - np.average(y, weights=w)
    gram = x.T @ (w[:, None] * x)
    m = np.linalg.inv(gram + lam * np.eye(2 * n_p + 1))
    return players, n_p, x, w, yc, gram, m


_SE6 = (
    "orapm_se",
    "drapm_se",
    "rapm_net_se",
    "orapm_se_sampling",
    "drapm_se_sampling",
    "rapm_net_se_sampling",
)


class TestPosteriorSe:
    def test_matches_the_dense_formulas(self):
        """posterior = sigma2*M, sampling = sigma2*(M - lam*M^2) = sigma2*M G M; sigma2 on n - df_eff dof."""
        s = aggregate_stints(_noisy_resolved(n_poss=2000, seed=1))
        lam = 50.0
        players, info = solve_rapm_league(s, ridge_lambda=lam)
        ids, n_p, x, w, yc, gram, m = _dense_system(s, lam)
        assert players["player_id"].to_list() == ids
        beta_exact = m @ (x.T @ (w * yc))
        assert np.abs(players["orapm"].to_numpy() - beta_exact[:n_p]).max() < 1e-3
        assert np.abs(players["drapm"].to_numpy() - beta_exact[n_p : 2 * n_p]).max() < 1e-3
        assert info["solve_max_abs_dev"] < 1e-3
        beta = np.concatenate([players["orapm"].to_numpy(), players["drapm"].to_numpy(), [info["hca"]]])
        resid = yc - x @ beta
        df_eff = np.trace(m @ gram)
        sigma2 = float(w @ resid**2) / (s.height - df_eff)
        assert abs(info["df_eff"] - df_eff) < 1e-8
        assert abs(info["sigma2"] / sigma2 - 1.0) < 1e-10
        i = np.arange(n_p)
        sandwich = m @ gram @ m
        assert np.allclose(sandwich, m - lam * m @ m)  # the identity the engine relies on
        for cov, sfx in ((m, ""), (sandwich, "_sampling")):
            d = np.diag(cov)
            assert np.allclose(players[f"orapm_se{sfx}"].to_numpy() ** 2, sigma2 * d[:n_p], rtol=1e-9)
            assert np.allclose(
                players[f"drapm_se{sfx}"].to_numpy() ** 2,
                sigma2 * d[n_p : 2 * n_p],
                rtol=1e-9,
            )
            net_var = sigma2 * (d[i] + d[n_p + i] + 2.0 * cov[i, n_p + i])
            assert np.allclose(players[f"rapm_net_se{sfx}"].to_numpy() ** 2, net_var, rtol=1e-9)
        assert abs(info["hca_se"] ** 2 - sigma2 * m[2 * n_p, 2 * n_p]) < 1e-9

    def test_sampling_se_never_exceeds_posterior_se(self):
        """M G M <= M in the PSD order, so the shrunk estimate's repeatability is at most the posterior SD."""
        players, _ = solve_rapm_league(aggregate_stints(_noisy_resolved(n_poss=1500, seed=5)), ridge_lambda=50.0)
        for c in ("orapm", "drapm", "rapm_net"):
            assert (players[f"{c}_se_sampling"] <= players[f"{c}_se"] + 1e-12).all()
            assert (players[f"{c}_se"] > 0).all()

    def test_more_possessions_tighter_posterior_se(self):
        small, _ = solve_rapm_league(aggregate_stints(_noisy_resolved(n_poss=500, seed=2)), ridge_lambda=50.0)
        big, _ = solve_rapm_league(aggregate_stints(_noisy_resolved(n_poss=8000, seed=2)), ridge_lambda=50.0)
        j = small.join(big, on="player_id", suffix="_big")
        assert j.height == 14
        assert (j["rapm_net_se_big"] < j["rapm_net_se"]).all()

    def test_possession_deciles_bins_and_columns(self):
        """Rank-based equal-count bins over off+def possessions; medians of every SE column.

        Whether the SE actually FALLS with playing time is a data property (a
        player who never sits is confounded with his team's total under the
        fixed-five constraint, so the top deciles flatten) -- the producer
        gates it on real seasons; here only the table contract is tested.
        """
        s = aggregate_stints(_noisy_resolved(n_poss=3000, seed=3))
        players, _ = solve_rapm_league(s, ridge_lambda=50.0)
        d = possession_deciles(players, n_bins=2)
        assert d["decile"].to_list() == [0, 1]
        assert d["n"].to_list() == [7, 7]
        assert d["poss_max"][0] <= d["poss_min"][1]
        assert {f"median_{c}" for c in _SE6} <= set(d.columns)
        assert (d["median_rapm_net_se"] > 0).all()

    def test_tighter_prior_tighter_posterior(self):
        s = aggregate_stints(_noisy_resolved(n_poss=1500, seed=4))
        loose, _ = solve_rapm_league(s, ridge_lambda=1.0)
        tight, _ = solve_rapm_league(s, ridge_lambda=1e5)
        assert tight["rapm_net_se"].max() < loose["rapm_net_se"].min()

    def test_net_se_within_triangle_bounds(self):
        players, _ = solve_rapm_league(aggregate_stints(_noisy_resolved(n_poss=1500, seed=5)), ridge_lambda=50.0)
        for sfx in ("", "_sampling"):
            o, d, n = (players[f"{c}{sfx}"].to_numpy() for c in ("orapm_se", "drapm_se", "rapm_net_se"))
            assert np.all(np.isfinite(n))
            assert np.all(n <= o + d + 1e-12) and np.all(n >= np.abs(o - d) - 1e-12)

    def test_compute_se_false_gives_nulls_not_nans(self):
        players, info = solve_rapm_league(
            aggregate_stints(_noisy_resolved(n_poss=300, seed=6)),
            ridge_lambda=50.0,
            compute_se=False,
        )
        for c in _SE6:
            assert players[c].dtype == pl.Float64
            assert players[c].null_count() == players.height
        assert "sigma2" not in info and "solve_max_abs_dev" not in info


class TestSplitHalfSeCheck:
    def test_shapes_summary_and_deciles(self):
        resolved = _noisy_resolved(n_poss=3000, n_games=10, seed=7)
        per_player, summary = split_half_se_check(resolved, ridge_lambda=50.0)
        assert (summary["n_games_a"], summary["n_games_b"]) == (5, 5)
        assert summary["n_players"] == per_player.height == 14
        for c in ("orapm", "drapm", "rapm_net"):
            want = {
                f"{c}_a",
                f"{c}_b",
                f"{c}_z",
                f"{c}_covered",
                f"{c}_z_sampling",
                f"{c}_covered_sampling",
            }
            assert want <= set(per_player.columns)
            assert 0.0 <= summary[f"coverage_{c}"] <= 1.0
            assert 0.0 <= summary[f"coverage_sampling_{c}"] <= 1.0
            assert per_player[f"{c}_z_sampling"].is_finite().all()
            # posterior SE >= sampling SE -> posterior |z| <= sampling |z| -> coverage no lower
            assert summary[f"coverage_{c}"] >= summary[f"coverage_sampling_{c}"]
            assert summary[f"z_sd_{c}"] <= summary[f"z_sd_sampling_{c}"]
        d = possession_deciles(per_player, n_bins=2)
        assert d["n"].to_list() == [7, 7]
        assert d["poss_max"][0] <= d["poss_min"][1]
        assert {"coverage_rapm_net", "coverage_rapm_net_sampling"} <= set(d.columns)

    def test_requires_integer_like_contest_id(self):
        resolved = _noisy_resolved(n_poss=200, seed=8)
        with pytest.raises(ValueError, match="contest_id"):
            split_half_se_check(resolved.drop("contest_id"), ridge_lambda=50.0)
        with pytest.raises(ValueError, match="integer-like"):
            split_half_se_check(
                resolved.with_columns(pl.lit("g-1").alias("contest_id")),
                ridge_lambda=50.0,
            )


class TestPriorMeanRidge:
    """``prior_mean=`` shrinks toward b0 instead of zero (SPM-prior RAPM)."""

    def _prior(self, ids, o=0.0, d=0.0):
        return pl.DataFrame(
            {
                "player_id": pl.Series(list(ids), dtype=pl.Utf8),
                "orapm_prior": [float(o)] * len(ids),
                "drapm_prior": [float(d)] * len(ids),
            }
        )

    def test_none_is_the_unchanged_flat_ridge(self):
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0, n=50)
        a, ia = solve_rapm_league(s, ridge_lambda=100.0, compute_se=False)
        b, ib = solve_rapm_league(s, ridge_lambda=100.0, prior_mean=None, compute_se=False)
        assert (a["orapm"] - b["orapm"]).abs().max() < 1e-12
        assert ia["prior_mean_mad"] == 0.0 and ib["prior_mean_mad"] == 0.0

    def test_heavy_penalty_collapses_onto_the_prior_not_zero(self):
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0, n=50)
        prior = self._prior(_H + _A + ["h6"], o=3.0, d=-1.0)
        players, info = solve_rapm_league(s, ridge_lambda=1e9, prior_mean=prior, compute_se=False)
        assert (players["orapm"] - 3.0).abs().max() < 1e-3
        assert (players["drapm"] + 1.0).abs().max() < 1e-3
        assert info["prior_mean_mad"] == pytest.approx(2.0)  # mean(|3|, |-1|)

    def test_output_actually_moved_off_the_flat_fit(self):
        # silent-no-op guard: the prior must change the ANSWER, not just the call.
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0, n=50)
        flat, _ = solve_rapm_league(s, ridge_lambda=500.0, compute_se=False)
        pri, _ = solve_rapm_league(
            s, ridge_lambda=500.0, prior_mean=self._prior(_H + _A + ["h6"], o=2.0), compute_se=False
        )
        j = flat.join(pri, on="player_id", suffix="_p")
        assert (j["orapm_p"] - j["orapm"]).abs().max() > 0.1

    def test_posterior_se_is_unchanged_by_the_prior_mean(self):
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0, n=50)
        flat, _ = solve_rapm_league(s, ridge_lambda=200.0)
        pri, _ = solve_rapm_league(s, ridge_lambda=200.0, prior_mean=self._prior(_H + _A + ["h6"], o=1.0, d=0.5))
        j = flat.join(pri, on="player_id", suffix="_p")
        # sigma2 shifts a little (different residuals), but the covariance shape does not.
        assert (j["rapm_net_se_p"] / j["rapm_net_se"]).std() < 1e-9

    def test_zero_overlap_prior_raises_instead_of_flat_ridge(self):
        s = _balanced_stints(n=20)
        with pytest.raises(ValueError, match="overlaps no rated player"):
            solve_rapm_league(s, ridge_lambda=100.0, prior_mean=self._prior(["nobody"], o=5.0))

    def test_wrong_dtype_key_raises(self):
        s = _balanced_stints(n=20)
        bad = self._prior(_H, o=1.0).with_columns(pl.lit(1).alias("player_id"))
        with pytest.raises(TypeError, match="Utf8"):
            solve_rapm_league(s, ridge_lambda=100.0, prior_mean=bad)

    def test_missing_column_raises(self):
        s = _balanced_stints(n=20)
        with pytest.raises(ValueError, match="missing columns"):
            solve_rapm_league(s, ridge_lambda=100.0, prior_mean=self._prior(_H, o=1.0).drop("drapm_prior"))


class TestStackedDesignColumns:
    """``fit_weight`` / ``y_offset``: the multi-season stacked design's two hooks."""

    def test_uniform_fit_weight_is_a_no_op(self):
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0, n=50)
        base, ib = solve_rapm_league(s, ridge_lambda=100.0, compute_se=False)
        wt, iw = solve_rapm_league(
            s.with_columns(pl.lit(1.0).alias("fit_weight")), ridge_lambda=100.0, compute_se=False
        )
        assert (base["orapm"] - wt["orapm"]).abs().max() < 1e-9
        assert ib["n_poss"] == iw["n_poss"]  # exposure is real possessions, not weighted

    def test_zero_weight_rows_drop_out_of_the_fit(self):
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0, n=50)
        half = s.with_columns(pl.when(pl.int_range(pl.len()) % 2 == 0).then(1.0).otherwise(0.0).alias("fit_weight"))
        kept = s.filter(pl.int_range(pl.len()) % 2 == 0)
        a, _ = solve_rapm_league(half, ridge_lambda=100.0, compute_se=False)
        b, _ = solve_rapm_league(kept, ridge_lambda=100.0, compute_se=False)
        j = a.join(b, on="player_id", suffix="_k")
        assert (j["orapm"] - j["orapm_k"]).abs().max() < 1e-8

    def test_off_poss_reports_real_possessions_under_a_decay_weight(self):
        s = _balanced_stints(n=20).with_columns(pl.lit(0.25).alias("fit_weight"))
        players, info = solve_rapm_league(s, ridge_lambda=100.0, compute_se=False)
        assert info["n_poss"] == s["n_poss"].sum()
        assert players["def_poss"].sum() + players["off_poss"].sum() == 10 * s["n_poss"].sum()

    def test_uniform_y_offset_only_moves_the_intercept(self):
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0, n=50)
        base, ib = solve_rapm_league(s, ridge_lambda=100.0, compute_se=False)
        off, io = solve_rapm_league(s.with_columns(pl.lit(7.0).alias("y_offset")), ridge_lambda=100.0, compute_se=False)
        assert (base["orapm"] - off["orapm"]).abs().max() < 1e-9
        assert io["intercept"] == pytest.approx(ib["intercept"] - 7.0)

    def test_y_offset_removes_a_per_season_scoring_level(self):
        # An older "season" pooled with a newer one, inflated by exactly 100 pts/100
        # (pts += n_poss). Offsetting it by 100 must reproduce the pooled fit on the
        # UN-inflated pair, exactly -- and without the offset it must not. The older
        # season is a SUBSET of the newer one's stints on purpose: a level shift on a
        # perfectly balanced pool is absorbed whole by the intercept, so only an
        # unbalanced pool (the real case -- players come and go) can see the offset.
        s = _balanced_stints(ppp_starter=2.0, ppp_sub=1.0, n=50)
        old = s.head(s.height // 2)
        b = old.with_columns((pl.col("pts") + pl.col("n_poss")).alias("pts"))
        want, _ = solve_rapm_league(pl.concat([s, old], how="vertical"), ridge_lambda=100.0, compute_se=False)
        centred, _ = solve_rapm_league(
            pl.concat(
                [
                    s.with_columns(pl.lit(0.0).alias("y_offset")),
                    b.with_columns(pl.lit(100.0).alias("y_offset")),
                ],
                how="vertical",
            ),
            ridge_lambda=100.0,
            compute_se=False,
        )
        raw, _ = solve_rapm_league(pl.concat([s, b], how="vertical"), ridge_lambda=100.0, compute_se=False)
        j = want.join(centred, on="player_id", suffix="_c").join(raw, on="player_id", suffix="_r")
        assert (j["orapm_c"] - j["orapm"]).abs().max() < 1e-9
        assert (j["orapm_r"] - j["orapm"]).abs().max() > 1e-3

    def test_negative_fit_weight_raises(self):
        s = _balanced_stints(n=20).with_columns(pl.lit(-1.0).alias("fit_weight"))
        with pytest.raises(ValueError, match="fit_weight"):
            solve_rapm_league(s, ridge_lambda=100.0, compute_se=False)
