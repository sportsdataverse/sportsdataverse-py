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
from sportsdataverse.nba.nba_playtype import nba_playtype_ratings, raw_playtype_efficiency
from sportsdataverse.nba.nba_playtype_constants import calibration_slope, sum_consistency_residual

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
