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

from typing import NamedTuple

import numpy as np
import polars as pl

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
#: ``ep_wp.py`` (``_make_model_mutations()``, ``_espn_*_features()``,
#: ``calculate_xpass``) and ``nfl_fourth_down._prepare`` read these cuts; none
#: restates them (sdv-internal-refs ``rules/nfl.yaml#nfl-2002-nflfastr-era-bins``).
#: If the model is ever retrained with different era bins, update this tuple.
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
#: 2026 review (2026-09-14): the 2026 season plays under the 2025 rulebook for
#: everything the EP/WP features see -- dynamic kickoff with the touchback at
#: the 35 (:func:`touchback_yardline` resolves it, as for 2025) and the
#: regular-season overtime format introduced in 2025 (:data:`OVERTIME_FORMATS`
#: selects its closed form) -- so it stays in ``era4``
#: without a new dummy. This bump is a coverage assertion, not a retrain: the
#: era-aware corpus still ends at 2025.
ERA_MAX_KNOWN_SEASON: int = 2026

#: Kickoff-touchback yards-to-endzone by the FIRST season each spot applied,
#: newest first.  The rule spots (sdv-internal-refs ``rules/nfl.yaml``):
#:
#: * ``nfl-2016-kickoff-touchback-to-25`` -- the 25 (75) from 2016; the 20 (80) before.
#: * ``nfl-2024-dynamic-kickoff`` -- the 30 (70) for the 2024 dynamic kickoff.
#: * ``nfl-2025-kickoff-touchback-to-35`` -- the 35 (65) from 2025.
#:
#: **This is the rule spot, not nflfastR's.**  ``helper_add_ep_wp.R`` still
#: substitutes ``ifelse(season < 2016, 80, 75)`` (L355 / L1047), so nflverse's
#: published ``ep`` / ``wp`` on 2024+ kickoffs are scored at 75.  Until 2026-10
#: sdv-py kept 75 as a parity convention; the catalog review moved every
#: consumer to the rule spot through :func:`touchback_yardline`, and
#: ``tests/nfl/test_nfl_ep_wp_real_rows.py`` pins the resulting, measured gap
#: to nflverse on 2024-25 kickoff rows (and that re-scoring at 75 closes it).
#: The EP/WP boosters were trained through 2025 on the recorded ``yardline_100``
#: of real snaps, so a 1st-and-10 from the 30 / 35 is inside their domain --
#: no refit is needed for the applier to spot a touchback where the rule does.
TOUCHBACK_YARDLINE_BY_SEASON: tuple[tuple[int, int], ...] = ((2025, 65), (2024, 70), (2016, 75), (0, 80))

#: Kickoff-touchback yards-to-endzone before 2016 (the 20).  Kept as a name for
#: the pre-2016 era; new code resolves the spot with :func:`touchback_yardline`.
TOUCHBACK_YARDLINE_PRE_2016: int = 80

#: Kickoff-touchback yards-to-endzone 2016-2023 (the 25).  The name predates
#: the 2024 / 2025 moves; it is NOT the spot for 2024+ -- resolve with
#: :func:`touchback_yardline`.
TOUCHBACK_YARDLINE_POST_2016: int = 75

#: Dynamic-kickoff touchback spot, 2024 only (the 30).
TOUCHBACK_YARDLINE_2024: int = 70

#: Dynamic-kickoff touchback spot from 2025 (the 35).
TOUCHBACK_YARDLINE_2025: int = 65

#: First season ESPN's NFL play text is a full sentence on kickoffs and punts. Not a playing rule: a
#: feed era (finding R808-4). The 2006-08 feed writes a bare "Kickoff" / "Punt", so an unscored row
#: typed "Kickoff Return Touchdown" / "Punt Return Touchdown" with no touchdown in its text looks the
#: same whether it is a real return TD (four in 2006-07, nflverse ``touchdown`` 1) or not (13 rows
#: 2006-08). From 2009 every such row is an onside or short-of-landing-zone kick or a plain punt
#: (32 rows in 2024, nflverse ``touchdown`` 0), so ``nfl_pbp._touchdown_did_not_stand`` clears it.
ESPN_FULL_KICK_TEXT_FROM: int = 2009

#: First season whose ESPN NFL ``period.number`` is not repaired from its neighbours. Not a playing
#: rule: a feed era (finding R832-10). Before it ESPN files a play under a LATER period than the plays
#: on both sides of it in id order while the clock runs on (261022011's Q2 kickoff at 0:08 as period
#: 3): 727 such rows in nfl-raw 2002-2013 (721 of them 2005-08), every one of the 722 that join to
#: nflverse ``qtr`` agreeing with the neighbours' period. From 2014 the same pattern fires on 3 rows,
#: one of them (401030749/906) a play whose id is out of order and whose ESPN period is right; the floor
#: gives up the other 2, correct repairs, to avoid that 1 wrong one. Each guard of
#: ``nfl_pbp._repair_espn_period``, measured alone on nfl-raw 2002-2025 against nflverse ``qtr``:
#: lowering only (raising a sandwiched row would be wrong 50 of 51 times); no repair just before an
#: "End ..." row (without it 6 early-quarter rows beside a late "End Period" row move the wrong way);
#: the clock running down across the three rows (it blocks 2 wrong lowerings and 10 right ones, kept so
#: that no row moves away from nflverse).
ESPN_PERIOD_REPAIR_BEFORE: int = 2014


def touchback_yardline(season: int) -> int:
    """Kickoff-touchback yards-to-endzone for *season* (80 / 75 / 70 / 65).

    Args:
        season: NFL season year (the September year).

    Returns:
        80 before 2016, 75 for 2016-2023, 70 for 2024, 65 from 2025.

    Example:
        Quick start::

            from sportsdataverse.nfl.model_vars import touchback_yardline

            print([touchback_yardline(s) for s in (2015, 2016, 2024, 2025)])  # [80, 75, 70, 65]
    """
    for first_season, spot in TOUCHBACK_YARDLINE_BY_SEASON:
        if season >= first_season:
            return spot
    return TOUCHBACK_YARDLINE_BY_SEASON[-1][1]  # pragma: no cover - the (0, 80) row always matches


def touchback_yardline_expr(season: pl.Expr) -> pl.Expr:
    """Polars form of :func:`touchback_yardline` for a ``season`` column expression.

    Args:
        season: Integer season expression (``pl.col("season")``).

    Returns:
        An ``Int64`` expression of the touchback yards-to-endzone per row.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl.model_vars import touchback_yardline_expr

            df = pl.DataFrame({"season": [2015, 2016, 2024, 2025]})
            print(df.select(touchback_yardline_expr(pl.col("season"))).to_series().to_list())
    """
    expr = pl.lit(TOUCHBACK_YARDLINE_BY_SEASON[-1][1], dtype=pl.Int64)
    for first_season, spot in reversed(TOUCHBACK_YARDLINE_BY_SEASON[:-1]):
        expr = pl.when(season >= first_season).then(pl.lit(spot, dtype=pl.Int64)).otherwise(expr)
    return expr


#: ``yardline_100`` at which the FG model scores a one-point PAT **before 2015**.
#: The kick was snapped from the 2; nflfastR ``helper_add_ep_wp.R`` (``add_wp_variables``,
#: L926) scores it at ``ifelse(season >= 2015, 15, 3)``, and 3 is that FG-model convention,
#: not the snap spot (rules/nfl.yaml#nfl-2015-pat-snap-from-15). ``nfl_fourth_down.py`` is
#: the consumer; ``ep_wp._apply_feature_substitution`` deliberately does NOT re-spot a try
#: (its docstring has the measurement).
PAT_YARDLINE_PRE_2015: int = 3

#: ``yardline_100`` at which the FG model scores a one-point PAT **2015 and later**:
#: the May 2015 rule moved the snap to the 15 (a 33-yard kick); two-point tries stayed at the 2.
PAT_YARDLINE_POST_2015: int = 15

#: First season of the 15-yard PAT (effective 2015-09-10).
PAT_FIRST_SEASON_FROM_15: int = 2015


# ---------------------------------------------------------------------------
# Overtime formats
# ---------------------------------------------------------------------------

#: Pure sudden death: the first score of any kind wins.
OT_SUDDEN_DEATH: str = "sudden_death"
#: Modified sudden death: a first-possession field goal gives the other team a
#: possession; a first-possession touchdown, or any later score, wins.
OT_MODIFIED_SUDDEN_DEATH: str = "modified_sudden_death"
#: Both teams possess: the opening possession never ends the game (a touchdown
#: included) unless the kicking team scores a safety on it; sudden death after
#: both have possessed.
OT_BOTH_POSSESS: str = "both_possess"


class OvertimeFormat(NamedTuple):
    """One row of :data:`OVERTIME_FORMATS`."""

    #: First season the row applies to (inclusive); rows are newest first.
    first_season: int
    #: :data:`OT_SUDDEN_DEATH`, :data:`OT_MODIFIED_SUDDEN_DEATH` or :data:`OT_BOTH_POSSESS`.
    name: str
    #: Length of one overtime period in seconds.  Documentation only: no
    #: closed form in ``ep_wp`` carries a time term (``nfl-2017-overtime-ten-minutes``).
    period_seconds: int
    #: The ``rules/nfl.yaml`` entry that put the row here.
    rule_id: str


#: NFL overtime format by ``season_type`` (nflverse ``"REG"`` / ``"POST"``), newest
#: row first; :func:`overtime_format` picks the first row whose ``first_season``
#: is at or before the season.  Catalog: sdv-internal-refs ``rules/nfl.yaml``.
OVERTIME_FORMATS: dict[str, tuple[OvertimeFormat, ...]] = {
    "REG": (
        OvertimeFormat(2025, OT_BOTH_POSSESS, 600, "nfl-2025-overtime-both-possess-regular-season"),
        OvertimeFormat(2017, OT_MODIFIED_SUDDEN_DEATH, 600, "nfl-2017-overtime-ten-minutes"),
        OvertimeFormat(2012, OT_MODIFIED_SUDDEN_DEATH, 900, "nfl-2012-overtime-modified-sudden-death-regular-season"),
        OvertimeFormat(0, OT_SUDDEN_DEATH, 900, "nfl-2012-overtime-modified-sudden-death-regular-season"),
    ),
    "POST": (
        OvertimeFormat(2022, OT_BOTH_POSSESS, 900, "nfl-2022-overtime-both-possess-postseason"),
        OvertimeFormat(2010, OT_MODIFIED_SUDDEN_DEATH, 900, "nfl-2010-overtime-modified-sudden-death-postseason"),
        OvertimeFormat(0, OT_SUDDEN_DEATH, 900, "nfl-2010-overtime-modified-sudden-death-postseason"),
    ),
}


def overtime_format(season: int, season_type: str = "REG") -> OvertimeFormat:
    """Resolve the NFL overtime format for a season and season type.

    Args:
        season: NFL season year.
        season_type: nflverse ``season_type`` -- ``"REG"`` (default) or
            ``"POST"``.  Anything else is treated as ``"REG"``.

    Returns:
        The matching :class:`OvertimeFormat` row of :data:`OVERTIME_FORMATS`.

    Example:
        Quick start::

            from sportsdataverse.nfl.model_vars import overtime_format

            print(overtime_format(2011).name, overtime_format(2011, "POST").name)
            # sudden_death modified_sudden_death
            print(overtime_format(2025).name, overtime_format(2025).period_seconds)
            # both_possess 600
    """
    rows = OVERTIME_FORMATS["POST" if season_type == "POST" else "REG"]
    for row in rows:
        if season >= row.first_season:
            return row
    return rows[-1]  # pragma: no cover - the first_season == 0 row always matches


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
        # the bundled models' trainer (nfl-data play_level.make_model_mutations):
        # "dome" / "closed" -> dome, everything else (incl. "open", None) -> outdoors;
        # retractable is 1 only for the literal "retractable", which never occurs
        "retractable",  # 1 if roof == "retractable", else 0 (never in nflverse data)
        "dome",  # 1 if roof in {"dome", "closed"}, else 0
        "outdoors",  # 1 otherwise ("outdoors", "open", None)
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
# ESPN clock-stoppage rows are not plays. The CFB-derived lists only knew the
# team "Timeout"; ESPN's NFL feed also emits "Official Timeout" (TV / injury /
# replay) and "Two-minute warning", and those rows carry no game state of their
# own -- an Official Timeout after a touchdown arrives as down=-1, distance=15,
# yardsToEndzone=15, which the EP model scores as ~3.8 EP and the next play then
# "loses". Every scrimmage/play aggregate and every EPA/WPA neutralisation keys
# on this list, never on "Timeout" alone (2026 week 1: 124 Official Timeouts
# across 15 games leaked -0.9 to -18.2 EPA into each team's EPA_overall_off).
clock_stoppage_vec = [
    "Timeout",
    "Official Timeout",
    "Two-minute warning",
    "Two-Minute Warning",
    "End Period",
    "End of Half",
    "End of Game",
]
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

# The trainer's column order (cfbfastR-dev/cfb_qbr/qbr_xgb.R:233), embedded in qbr_model.ubj's
# feature_names, so a permuted DMatrix raises instead of scoring sack EPA as pass EPA (gap-C-083).
qbr_vars = ["qbr_epa", "pass_epa", "rush_epa", "sack_epa", "pen_epa", "spread"]
