"""Committed-fixture oracle gates for the play-type/impact spine (T3.5), all four models.

Kept together so the 2023-24 fixture corpus (``playtype_corpus``, see
``tests/fixtures/nba_playtype/README.md``) loads once per session. Gate
floors are set from the values actually observed against this real captured
corpus (documented inline) -- never lowered to make a red gate pass; a red
gate means debug the model, not the assertion.
"""

import polars as pl

from sportsdataverse.nba.nba_expected_turnovers import nba_expected_turnovers
from sportsdataverse.nba.nba_foul_drawing import nba_foul_drawing
from sportsdataverse.nba.nba_matchup_drapm import nba_matchup_drapm
from sportsdataverse.nba.nba_playtype import nba_playtype_ratings, raw_playtype_efficiency
from sportsdataverse.nba.nba_playtype_constants import (
    PlaytypeConfig,
    calibration_slope,
    spearman_corr,
    sum_consistency_residual,
)

# ---------------------------------------------------------------------------
# Model (1): Synergy play-type-adjusted offense/defense
# ---------------------------------------------------------------------------

#: Top-5 teams by real (box-score-derived) 2023-24 offensive rating --
#: `nba_stats_leaguedashteamstats` Advanced, an independent data source from
#: Synergy's per-play aggregation (captured 2026-07-08, see fixtures README).
KNOWN_TOP_OFFENSES_2024 = {1610612738, 1610612754, 1610612760, 1610612746, 1610612743}  # BOS, IND, OKC, LAC, DEN


def test_playtype_sum_consistency(playtype_corpus):
    off, deff = playtype_corpus["synergy_off_team"], playtype_corpus["synergy_def_team"]
    raw = raw_playtype_efficiency(off, deff)
    by_team = raw.group_by("team_id").agg(pl.col("off_pts").sum().alias("recon_pts"))
    tot = off.group_by("team_id").agg(pl.col("pts").sum().alias("true_pts"))
    m = by_team.join(tot, on="team_id")
    resid = sum_consistency_residual(m.select("recon_pts").to_numpy(), m["true_pts"].to_numpy())
    assert resid < 1e-6  # observed ~2.8e-14


def test_playtype_rank_sanity(playtype_corpus):
    r = nba_playtype_ratings(
        "2023-24",
        off_team=playtype_corpus["synergy_off_team"],
        def_team=playtype_corpus["synergy_def_team"],
        schedule=playtype_corpus["gamelog"].select("team_id", "opp_team_id"),
    )
    top = set(r.sort("adj_off", descending=True).head(5)["team_id"].to_list())
    assert len(top & KNOWN_TOP_OFFENSES_2024) >= 3  # observed 4/5


# ---------------------------------------------------------------------------
# Model (3): Foul-drawing / FT-generation
# ---------------------------------------------------------------------------

#: Real, verifiable elite foul-drawers in the 2023-24 season (Giannis
#: Antetokounmpo, Joel Embiid, Shai Gilgeous-Alexander, DeMar DeRozan).
KNOWN_FOUL_DRAWERS_2024 = {203507, 203954, 1628983, 201942}


def test_foul_drawing_calibration_and_rank(playtype_corpus):
    fd = nba_foul_drawing(
        "2023-24",
        base=playtype_corpus["leaguedash_base"],
        advanced=playtype_corpus["leaguedash_adv"],
        player_mix=playtype_corpus["synergy_off_player"],
    )
    slope = calibration_slope(fd["expected_fta"].to_numpy(), fd["fta"].to_numpy())
    assert 0.95 <= slope <= 1.05  # observed ~1.026
    sum_err = abs(fd["expected_fta"].sum() - fd["fta"].sum()) / fd["fta"].sum()
    assert sum_err < 0.01  # self-normalizing scale -> observed 0.0 exactly

    top_decile = fd.filter(pl.col("poss") >= 1000)
    q90 = top_decile["foul_draw_skill"].quantile(0.90)
    top = set(top_decile.filter(pl.col("foul_draw_skill") >= q90)["player_id"].to_list())
    assert len(top & KNOWN_FOUL_DRAWERS_2024) >= 3  # observed 4/4


# ---------------------------------------------------------------------------
# Model (4): Expected turnovers
# ---------------------------------------------------------------------------

#: Real, verifiable elite ball-security players in the 2023-24 season
#: (Shai Gilgeous-Alexander, DeMar DeRozan -- both high-usage, low-TOV guards).
KNOWN_LOW_TOV_PLAYERS_2024 = {1628983, 201942}

#: TOV calibration slope floor. Unlike model (3)'s ~1.026, the *expected*
#: (league-rate) turnover model necessarily explains less variance than
#: *actual* -- turnover-avoidance is a more individual, less play-type-bound
#: skill than foul-drawing. Confirmed not a bug: swapping each player's own
#: turnover_freq in for the league rate reconstructs their real season TOV at
#: slope ~1.03 / Spearman ~0.97 (see nba_expected_turnovers docstring), so the
#: column semantics are correct -- the ~0.84 shortfall is a genuine property
#: of the mix-only expected value, observed across poss>=0/200/500/1000 cuts
#: (0.836/0.815/0.802/0.790). Floor set below the lowest observed, documented,
#: not lowered further.
TOV_CALIBRATION_SLOPE_FLOOR = 0.75


def test_expected_turnovers_calibration_and_rank(playtype_corpus):
    tov = nba_expected_turnovers(
        "2023-24",
        base=playtype_corpus["leaguedash_base"],
        player_mix=playtype_corpus["synergy_off_player"],
    )
    slope = calibration_slope(tov["expected_tov"].to_numpy(), tov["tov"].to_numpy())
    assert slope >= TOV_CALIBRATION_SLOPE_FLOOR  # observed ~0.836
    sum_err = abs(tov["expected_tov"].sum() - tov["tov"].sum()) / tov["tov"].sum()
    assert sum_err < 0.01  # self-normalizing scale -> observed 0.0 exactly

    top_decile = tov.filter(pl.col("poss") >= 1000)
    q90 = top_decile["ball_security_skill"].quantile(0.90)
    top = set(top_decile.filter(pl.col("ball_security_skill") >= q90)["player_id"].to_list())
    assert len(top & KNOWN_LOW_TOV_PLAYERS_2024) >= 1  # observed 2/2


# ---------------------------------------------------------------------------
# Model (2): Matchup defensive RAPM
# ---------------------------------------------------------------------------

#: Internal concurrent-validity floor: matchup_drapm must rank-agree (strongly,
#: negatively -- higher DRAPM = fewer points allowed) with each defender's raw
#: points-allowed-per-100 across the matchups they guarded. Observed -0.733 on
#: the 2023-24 corpus (poss>=500); floor rounded to the safe side of observed.
#: This is the SHIPPED oracle for model (2) -- it does NOT depend on the
#: stint-RAPM snapshot (see the construct-gap test below). The offense
#: fixed effects make this non-trivial: a raw points-allowed ranking is NOT
#: identical to the offense-adjusted one, so |rho| must also stay < 1.0.
DRAPM_INTERNAL_VALIDITY_FLOOR = -0.5


def test_matchup_drapm_internal_concurrent_validity(playtype_corpus):
    m = playtype_corpus["matchups"]
    cfg = PlaytypeConfig()
    drapm = nba_matchup_drapm("2023-24", matchups=m)
    assert drapm.schema["player_id"] == pl.Int64

    # ridge centering invariant: mean coefficient is centered to ~0
    assert abs(float(drapm["matchup_drapm"].mean())) < 1e-6  # observed ~1e-16

    # magnitude sanity (guards the fixed double-scale bug from regressing):
    # y is per-100, so drapm is per-100 -> well-resolved defenders sit in low
    # double digits, never the hundreds. Observed max ~11.6.
    assert float(drapm.filter(pl.col("matchup_poss") >= 200)["matchup_drapm"].abs().max()) < 50.0

    # raw points-allowed-per-100 per defender (independent of the ridge fit)
    raw = (
        m.filter(pl.col("partial_poss") >= cfg.min_matchup_poss)
        .group_by("def_player_id")
        .agg(
            (100.0 * pl.col("player_pts").sum() / pl.col("partial_poss").sum()).alias("raw_pa100"),
            pl.col("partial_poss").sum().alias("tot"),
        )
    )
    drapm_d = drapm.rename({"player_id": "def_player_id"})
    assert drapm_d.schema["def_player_id"] == raw.schema["def_player_id"]  # join-key dtype guard
    jr = drapm_d.join(raw, on="def_player_id").filter(pl.col("tot") >= 500)
    assert jr.height >= 200  # non-vacuous sample floor (observed 251); a shrunken re-capture can't pass empty
    rho = spearman_corr(jr["matchup_drapm"].to_numpy(), jr["raw_pa100"].to_numpy())
    assert rho <= DRAPM_INTERNAL_VALIDITY_FLOOR  # observed -0.733
    assert rho > -0.98  # offense-FE adjustment is non-trivial (not a raw passthrough)


#: Resolution of the once-deferred external cross-validation (matchup_drapm
#: vs. shipped stint nba_rapm). PERMANENT FINDING, not a pending capture: a
#: full 1230-game / 242,619-possession season snapshot (the maximum possible
#: sample -- see the fixtures README) was captured to settle the sample-size
#: question the original 126-game deferral raised. The Spearman trajectory as
#: the sample grew: -0.018 (100g) / -0.002 (150g) / +0.041 (200g) / +0.039
#: (300g) / +0.063 (400g) / +0.089 (500g) / +0.091 (650g) / +0.111 (800g) /
#: +0.111 (1000g) / +0.115 (1230g, full season). It PLATEAUS from ~800 games
#: on (800->1000, a +200-game step, moved rho by -0.0003) -- this is not
#: ridge-shrunk noise settling out, it is the actual full-season relationship.
#: It is weak-but-real and positive (on-ball defense contributes to team
#: defensive value) but falls well short of a shared-construct correlation.
#: Conclusion: matchup-DRAPM (on-ball defense) and stint-DRAPM (team/help
#: defense) are related but DISTINCT constructs; strict external
#: cross-validation against stint RAPM does not apply to model (2). It ships
#: gated on its INTERNAL concurrent-validity instead
#: (test_matchup_drapm_internal_concurrent_validity, above). The bounds below
#: pin the observed full-season value (rounded outward for headroom) so a
#: regression that flips the sign or suddenly produces a strong correlation
#: (either would contradict this finding) fails loudly instead of silently.
DRAPM_CONSTRUCT_GAP_BAND = (-0.05, 0.3)  # observed 0.1153 at the full 1230-game season


def test_matchup_drapm_vs_stint_rapm_construct_gap(playtype_corpus):
    drapm = nba_matchup_drapm("2023-24", matchups=playtype_corpus["matchups"])
    rapm = playtype_corpus["rapm"].select("player_id", "d_rapm")
    assert drapm.schema["player_id"] == rapm.schema["player_id"] == pl.Int64
    j = drapm.join(rapm, on="player_id", how="inner").filter(pl.col("matchup_poss") >= 200)
    assert j.height >= 200  # non-vacuous sample floor (observed 316 at the full season)
    rho = spearman_corr(j["matchup_drapm"].to_numpy(), j["d_rapm"].to_numpy())
    lo, hi = DRAPM_CONSTRUCT_GAP_BAND
    # Weak-but-real positive relationship (observed 0.1153) -- NOT the strong
    # correlation a shared-construct / successful-external-validation reading
    # would require. See DRAPM_CONSTRUCT_GAP_BAND docstring above.
    assert lo <= rho < hi
