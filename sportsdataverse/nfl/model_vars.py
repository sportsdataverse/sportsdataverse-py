"""NFL model variable registries: feature-column lists, play-type vectors, and constants.

This module is the single source of truth for:

- ESPN feature-column lists (``ep_start_columns``, ``wp_start_columns``, …) used
  by ``NFLPlayProcess.__process_epa`` / ``.__process_wpa`` in ``nfl_pbp.py``.
- Shared numeric constants consumed by ``ep_wp.py`` and (in future tasks) the
  ``enrich_nfl_pbp`` pipeline.
- ``NFLVERSE_FRAME_CONTRACT`` — the minimal column set that ``enrich_nfl_pbp``
  will require from any input DataFrame, documented per source.
"""

from __future__ import annotations

import numpy as np

# ---------------------------------------------------------------------------
# Shared numeric constants
# ---------------------------------------------------------------------------

#: EP class-weight array — mirrors nflfastR's ``ep_class_to_score_mapping``
#: and the identical vector in ``ep_wp.py`` at module level.
#: Class order: TD=0, OppTD=1, FG=2, OppFG=3, Safety=4, OppSafety=5, No_Score=6
_EP_POINT_VALUES: np.ndarray = np.array([7.0, -7.0, 3.0, -3.0, 2.0, -2.0, 0.0], dtype=np.float64)

#: Season-year boundaries for the five nflfastR era bins (upper-inclusive per era).
#:
#: Mapping::
#:
#:     era0  season <= 2001
#:     era1  2002 .. 2005
#:     era2  2006 .. 2013
#:     era3  2014 .. 2017
#:     era4  >= 2018
#:
#: These exact cuts are replicated in ``_make_model_mutations()`` and
#: ``_espn_ep_features()`` / ``_espn_wp_features()`` / ``_espn_cp_features()``
#: in ``ep_wp.py``.  If the model is ever retrained with different era bins,
#: update this tuple and those functions together.
ERA_SEASON_CUTS: tuple[int, int, int, int] = (2001, 2005, 2013, 2017)

#: Last season the era bins above (and the kickoff-touchback substitution below)
#: were validated for — the end year of the era-aware retrain corpus
#: (``nfl-data`` ``_stage.DEFAULT_SEASONS``).  ``era4`` is open-ended
#: (``season > 2017``), so a later season is scored under the last era with
#: nothing flagging that a rule change (e.g. the 2024 dynamic kickoff) may
#: warrant a new dummy.  Every era-building helper in ``ep_wp`` emits
#: :class:`sportsdataverse.errors.EraCoverageWarning` once per such season
#: instead of absorbing it silently.  Bump at each era-aware retrain, together
#: with the trainer's season span.
ERA_MAX_KNOWN_SEASON: int = 2025

#: Kickoff-touchback starting yardline **before** the 2016 rule change.
#: nflfastR canonical: the touchback was spotted at the 20-yard line, so
#: yards-to-endzone = 80.
TOUCHBACK_YARDLINE_PRE_2016: int = 80

#: Kickoff-touchback starting yardline **2016 and later**.
#: nflfastR canonical: the touchback was moved to the 25-yard line (2016 rule
#: change), so yards-to-endzone = 75.
#:
#: Note: ``nfl_pbp.py`` currently uses an inline ``season > 2013`` boundary
#: instead of this 2016 boundary — aligning that call site to this constant
#: is deferred to a later task (behavior-change + parity gate).
#:
#: **Dynamic-kickoff audit (2026-09-01).** The 2024 rule moved the kickoff
#: touchback to the 30 (yards-to-endzone 70) and the 2025 rule to the 35 (65),
#: but nflfastR's ``helper_add_ep_wp.R`` still substitutes
#: ``ifelse(season < 2016, 80, 75)`` and nflverse's published ``ep`` / ``wp``
#: on 2024–2025 kickoffs are therefore scored at 75.  This constant
#: deliberately stays at 75 — it is a *parity convention with that oracle*,
#: not the rule spot.  ``tests/nfl/test_nfl_ep_wp_real_rows.py`` pins both
#: facts on committed real rows: sdv-py == nflverse on kickoff rows season by
#: season, AND the data's actual post-touchback spot (75 → 70 → 65).  Moving
#: to the rule-correct spot is a retrain-time decision — trainer and applier
#: must move together.
TOUCHBACK_YARDLINE_POST_2016: int = 75

#: Exponent multiplier in the spread-time decay formula::
#:
#:     spread_time     = spread             * exp(SPREAD_TIME_DECAY_EXPONENT * elapsed_share)
#:     Diff_Time_Ratio = score_differential / exp(SPREAD_TIME_DECAY_EXPONENT * elapsed_share)
#:
#: where ``elapsed_share = clip((3600 - game_seconds_remaining) / 3600, 0, 1)``.
#: A *fitted* constant (nflfastR MODELS.R fixed it at -4), so it must travel
#: with retrains: this is the ONLY source on the applier side (``_add_wp_aux``,
#: ``_espn_wp_features`` and ``NFLPlayProcess.__add_spread_time`` all import
#: it), the nfl-data trainer writes the value it trained with into every WP
#: model card (``<model>.json`` → ``derived_feature_constants``
#: ``.spread_time_decay_exponent``), and ``ep_wp._load_booster_from`` raises
#: ``ValueError`` when a card beside a model disagrees with this value.
SPREAD_TIME_DECAY_EXPONENT: float = -4.0

# ---------------------------------------------------------------------------
# NFLVERSE_FRAME_CONTRACT
# ---------------------------------------------------------------------------

#: Minimal column set that ``enrich_nfl_pbp`` requires from its input DataFrame.
#:
#: **Column source mapping**
#:
#: The three data paths that feed ``enrich_nfl_pbp`` use different column-naming
#: conventions.  The contract lists the *canonical nflverse names* (snake_case,
#: matching what ``load_nfl_pbp`` returns and what ``calculate_expected_points`` /
#: ``calculate_win_probability`` expect).  Adapters convert the other two paths:
#:
#: ESPN ``start.`` / ``end.`` format (``NFLPlayProcess`` in ``nfl_pbp.py``)::
#:
#:     nflverse name                   ESPN column name
#:     ─────────────────────────────   ─────────────────────────────────────
#:     half_seconds_remaining       ←  start.TimeSecsRem
#:     game_seconds_remaining       ←  start.adj_TimeSecsRem
#:     yardline_100                 ←  start.yardsToEndzone
#:     ydstogo                      ←  start.distance
#:     down                         ←  start.down
#:     posteam_timeouts_remaining   ←  start.posTeamTimeouts
#:     defteam_timeouts_remaining   ←  start.defPosTeamTimeouts
#:     score_differential           ←  pos_score_diff_start
#:     receive_2h_ko                ←  start.pos_team_receives_2H_kickoff
#:     spread_line                  ←  (pre-computed via __add_spread_time)
#:     home                         ←  start.is_home (cast Int8)
#:     posteam                      ←  homeTeamId / awayTeamId resolution
#:     defteam                      ←  homeTeamId / awayTeamId resolution
#:     home_team                    ←  homeTeamId
#:
#: Shield / native NFL API (``nfl_api_parsers.py``)::
#:
#:     nflverse name                   Shield / native underscore name
#:     ─────────────────────────────   ────────────────────────────────
#:     game_id                      ←  game_id  (nflfastR GSIS format)
#:     play_id                      ←  play_id
#:     posteam                      ←  posteam
#:     defteam                      ←  defteam
#:     home_team                    ←  home_team
#:     season                       ←  season
#:     game_half                    ←  game_half  ("Half1" | "Half2" | "OT")
#:     posteam_score                ←  posteam_score
#:     defteam_score                ←  defteam_score
#:
#: nflverse parquet native (``load_nfl_pbp`` output — no renaming needed)::
#:
#:     All columns below are already in nflverse snake_case.  The parquet
#:     schema from nflverse/nfl-data is the reference; the contract subset
#:     listed here is the EP/WP-pipeline minimum.
#:
#: The contract is a ``frozenset`` so it is immutable and hashable.
#: Membership tests (``"col" in NFLVERSE_FRAME_CONTRACT``) are O(1).
NFLVERSE_FRAME_CONTRACT: frozenset[str] = frozenset(
    {
        # ── identity ──────────────────────────────────────────────────────────
        "game_id",
        "play_id",
        "season",
        "game_half",  # "Half1" | "Half2" | "OT" — gates EPA sign-flip
        "posteam",
        "defteam",
        "home_team",
        # ── EP feature inputs (nflverse native names, _make_model_mutations) ─
        "half_seconds_remaining",
        "yardline_100",
        "ydstogo",
        "down",
        "posteam_timeouts_remaining",
        "defteam_timeouts_remaining",
        # derived columns (computed from inputs; listed for contract clarity)
        "home",  # 1 if posteam == home_team, else 0
        "retractable",  # 1 if roof in {None, "open", "closed"}, else 0
        "dome",  # 1 if roof == "dome", else 0
        "outdoors",  # 1 if roof == "outdoors", else 0
        # ── WP feature inputs ─────────────────────────────────────────────────
        "score_differential",
        "game_seconds_remaining",
        "spread_line",  # None/null → wp_naive model; non-null → wp_spread
        "receive_2h_ko",  # 1 if posteam receives 2nd-half kickoff
        # ── EPA / WPA derivation inputs ───────────────────────────────────────
        "posteam_score",  # score of possessing team at start of play
        "defteam_score",  # score of defending team at start of play
        # ── roof (source for retractable/dome/outdoors) ───────────────────────
        "roof",  # "dome" | "outdoors" | "open" | "closed" | null
    }
)

# ---------------------------------------------------------------------------
# Existing dict (kept for backward compatibility — _EP_POINT_VALUES above
# carries the same information as an ndarray for model dot-product use)
# ---------------------------------------------------------------------------

ep_class_to_score_mapping = {0: 7, 1: -7, 2: 3, 3: -3, 4: 2, 5: -2, 6: 0}

wp_start_touchback_columns = [
    "start.pos_team_receives_2H_kickoff",
    "start.spread_time",
    "start.TimeSecsRem",
    "start.adj_TimeSecsRem",
    "start.ExpScoreDiff_Time_Ratio_touchback",
    "pos_score_diff_start",
    "start.down",
    "start.distance",
    "start.yardsToEndzone.touchback",
    "start.is_home",
    "start.posTeamTimeouts",
    "start.defPosTeamTimeouts",
    "period",
]
wp_start_columns = [
    "start.pos_team_receives_2H_kickoff",
    "start.spread_time",
    "start.TimeSecsRem",
    "start.adj_TimeSecsRem",
    "start.ExpScoreDiff_Time_Ratio",
    "pos_score_diff_start",
    "start.down",
    "start.distance",
    "start.yardsToEndzone",
    "start.is_home",
    "start.posTeamTimeouts",
    "start.defPosTeamTimeouts",
    "period",
]
wp_end_columns = [
    "end.pos_team_receives_2H_kickoff",
    "end.spread_time",
    "end.TimeSecsRem",
    "end.adj_TimeSecsRem",
    "end.ExpScoreDiff_Time_Ratio",
    "end.pos_score_diff",
    "end.down",
    "end.distance",
    "end.yardsToEndzone",
    "end.is_home",
    "end.posTeamTimeouts",
    "end.defPosTeamTimeouts",
    "period",
]

ep_start_touchback_columns = [
    "start.TimeSecsRem",
    "start.yardsToEndzone.touchback",
    "distance",
    "down_1",
    "down_2",
    "down_3",
    "down_4",
    "pos_score_diff_start",
]
ep_start_columns = [
    "start.TimeSecsRem",
    "start.yardsToEndzone",
    "start.distance",
    "down_1",
    "down_2",
    "down_3",
    "down_4",
    "pos_score_diff_start",
]
ep_end_columns = [
    "end.TimeSecsRem",
    "end.yardsToEndzone",
    "end.distance",
    "down_1_end",
    "down_2_end",
    "down_3_end",
    "down_4_end",
    "pos_score_diff_end",
]

ep_final_names = [
    "TimeSecsRem",
    "yards_to_goal",
    "distance",
    "down_1",
    "down_2",
    "down_3",
    "down_4",
    "pos_score_diff_start",
]
wp_final_names = [
    "pos_team_receives_2H_kickoff",
    "spread_time",
    "TimeSecsRem",
    "adj_TimeSecsRem",
    "ExpScoreDiff_Time_Ratio",
    "pos_score_diff_start",
    "down",
    "distance",
    "yards_to_goal",
    "is_home",
    "pos_team_timeouts_rem_before",
    "def_pos_team_timeouts_rem_before",
    "period",
]

# -------Play type vectors-------------
scores_vec = [
    "Blocked Punt Touchdown",
    "Blocked Punt (Safety)",
    "Punt (Safety)",
    "Blocked Field Goal Touchdown",
    "Missed Field Goal Return Touchdown",
    "Fumble Recovery (Opponent) Touchdown",
    "Fumble Return Touchdown",
    "Interception Return Touchdown",
    "Pass Interception Return Touchdown",
    "Punt Touchdown",
    "Punt Return Touchdown",
    "Sack Touchdown",
    "Uncategorized Touchdown",
    "Defensive 2pt Conversion",
    "Uncategorized",
    "Two Point Rush",
    "Safety",
    "Penalty (Safety)",
    "Punt Team Fumble Recovery Touchdown",
    "Kickoff Team Fumble Recovery Touchdown",
    "Kickoff (Safety)",
    "Passing Touchdown",
    "Rushing Touchdown",
    "Field Goal Good",
    "Pass Reception Touchdown",
    "Fumble Recovery (Own) Touchdown",
]
defense_score_vec = [
    "Blocked Punt Touchdown",
    "Blocked Field Goal Touchdown",
    "Missed Field Goal Return Touchdown",
    "Punt Return Touchdown",
    "Fumble Recovery (Opponent) Touchdown",
    "Fumble Return Touchdown",
    "Kickoff Touchdown",  # <--- Kickoff Team recovers the return team fumble and scores
    "Defensive 2pt Conversion",
    "Safety",
    "Sack Touchdown",
    "Interception Return Touchdown",
    "Pass Interception Return Touchdown",
    "Uncategorized Touchdown",
]
turnover_vec = [
    "Blocked Field Goal",
    "Blocked Field Goal Touchdown",
    "Blocked Punt",
    "Blocked Punt Touchdown",
    "Field Goal Missed",
    "Missed Field Goal Return",
    "Missed Field Goal Return Touchdown",
    "Fumble Recovery (Opponent)",
    "Fumble Recovery (Opponent) Touchdown",
    "Fumble Return Touchdown",
    "Defensive 2pt Conversion",
    "Interception",
    "Interception Return",
    "Interception Return Touchdown",
    "Pass Interception Return",
    "Pass Interception Return Touchdown",
    "Kickoff Team Fumble Recovery",
    "Kickoff Team Fumble Recovery Touchdown",
    "Punt Touchdown",
    "Punt Return Touchdown",
    "Sack Touchdown",
    "Uncategorized Touchdown",
]
normalplay = [
    "Rush",
    "Pass",
    "Pass Reception",
    "Pass Incompletion",
    "Pass Completion",
    "Sack",
    "Fumble Recovery (Own)",
]
penalty = ["Penalty", "Penalty (Kickoff)", "Penalty (Safety)"]
offense_score_vec = [
    "Passing Touchdown",
    "Rushing Touchdown",
    "Field Goal Good",
    "Pass Reception Touchdown",
    "Fumble Recovery (Own) Touchdown",
    "Punt Touchdown",  # <--- Punting Team recovers the return team fumble and scores
    "Punt Team Fumble Recovery Touchdown",
    "Kickoff Return Touchdown",
    "Kickoff Team Fumble Recovery Touchdown",
]
punt_vec = [
    "Blocked Punt",
    "Blocked Punt Touchdown",
    "Blocked Punt (Safety)",
    "Punt (Safety)",
    "Punt",
    "Punt Return",
    "Punt Touchdown",
    "Punt Team Fumble Recovery",
    "Punt Team Fumble Recovery Touchdown",
    "Punt Return Touchdown",
]
kickoff_vec = [
    "Kickoff",
    "Kickoff Return (Offense)",
    "Kickoff Return Touchdown",
    "Kickoff Touchdown",
    "Kickoff Team Fumble Recovery",
    "Kickoff Team Fumble Recovery Touchdown",
    "Kickoff (Safety)",
    "Penalty (Kickoff)",
]
int_vec = [
    "Interception",
    "Interception Return",
    "Interception Return Touchdown",
    "Pass Interception",
    "Pass Interception Return",
    "Pass Interception Return Touchdown",
]
end_change_vec = [
    "Blocked Field Goal",
    "Blocked Field Goal Touchdown",
    "Field Goal Missed",
    "Missed Field Goal Return",
    "Missed Field Goal Return Touchdown",
    "Blocked Punt",
    "Blocked Punt Touchdown",
    "Punt",
    "Punt Return",
    "Punt Touchdown",
    "Punt Return Touchdown",
    "Kickoff Team Fumble Recovery",
    "Kickoff Team Fumble Recovery Touchdown",
    "Fumble Recovery (Opponent)",
    "Fumble Recovery (Opponent) Touchdown",
    "Fumble Return Touchdown",
    "Sack Touchdown",
    "Defensive 2pt Conversion",
    "Interception",
    "Interception Return",
    "Interception Return Touchdown",
    "Pass Interception Return",
    "Pass Interception Return Touchdown",
    "Uncategorized Touchdown",
]
kickoff_turnovers = ["Kickoff Team Fumble Recovery", "Kickoff Team Fumble Recovery Touchdown"]

qbr_vars = ["qbr_epa", "sack_epa", "pass_epa", "rush_epa", "pen_epa", "spread"]
