"""GSIS statType decode + per-play stats summation.

Faithful Python port of nflfastR's ``sum_play_stats`` (R/helper_tidy_play_stats.R).
Each play in the Shield ``driveChart`` carries a ``stats`` array; every entry has a
``statType`` (GSIS stat id), a ``yards`` value, a ``gsisPlayerId`` (already in the
nflverse ``00-00xxxxx`` id space), a ``gsisPlayerName``, and a ``teamId`` (UUID).
:func:`sum_play_stats` collapses that array into the flat outcome columns
nflfastR/nflverse expose (complete_pass, air_yards, passer/receiver ids, etc.).

Source-token semantics (per stat-effect tuple):
    ONE          -> literal 1 (binary indicator)
    YARDS        -> the entry's ``yards``
    ZERO         -> literal 0
    PID / PNAME  -> the entry's player id / name
    TEAM         -> the entry's teamId (resolved to an abbr downstream)
    COUNT        -> +1 accumulator (each matching stat entry adds one). Used for
                    exact per-play counts the team aggregation sums verbatim
                    (e.g. ``def_tackles_for_loss`` = count of statId 402), so a
                    >2-credit play is no longer capped at the old 2-slot ceiling.
    SUMYARDS     -> += the entry's ``yards`` accumulator (e.g. ``misc_yards``,
                    lateral-recovery yards). Distinct from YARDS, which overwrites.
    *_IFNA       -> written only if the target column is still None (first writer wins)
    *_FILL       -> first-available slot in the matching FILL_GROUPS set, with
                    same-player de-dup (a player already in an earlier slot is skipped)

The COUNT / SUMYARDS accumulators (and the additive ``rushing_yards`` /
``receiving_yards`` totals) make the team-aggregation columns
:func:`nflfastR::calculate_stats` derives — ``def_tackles_for_loss``,
``def_tackles_for_loss_yards``, ``misc_yards``, lateral fumble-recovery yards,
and the lateral-inclusive rush/reception yardage — exactly reconstructible from
the per-play frame. Each stat-id range is taken verbatim from
``nflfastR/R/calculate_stats.R``.
"""

from __future__ import annotations

from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# statType -> [(column, source_token), ...]  (89 branches, ported verbatim)
# ---------------------------------------------------------------------------

STAT_ID_EFFECTS: Dict[int, List[tuple[str, str]]] = {
    2: [
        ("punt_blocked", "ONE"),
        ("punt_attempt", "ONE"),
        ("kick_distance", "YARDS"),
        ("punter_player_id", "PID"),
        ("punter_player_name", "PNAME"),
    ],
    3: [("first_down_rush", "ONE")],
    4: [("first_down_pass", "ONE")],
    5: [("first_down_penalty", "ONE")],
    6: [("third_down_converted", "ONE")],
    7: [("third_down_failed", "ONE")],
    8: [("fourth_down_converted", "ONE")],
    9: [("fourth_down_failed", "ONE")],
    # rushing_yards is a SUMYARDS accumulator over statIds 10:13 (calculate_stats.R
    # rushing_yards = sum((stat_id %in% 10:13) * yards)) so the lateral ball-carrier's
    # yards (12/13) are credited to the team total, closing the lateral-row diffs.
    10: [
        ("rush_attempt", "ONE"),
        ("rusher_player_id", "PID"),
        ("rusher_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("rushing_yards", "SUMYARDS"),
    ],
    11: [
        ("rush_attempt", "ONE"),
        ("touchdown", "ONE"),
        ("first_down_rush", "ONE"),
        ("rush_touchdown", "ONE"),
        ("rusher_player_id", "PID"),
        ("rusher_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("rushing_yards", "SUMYARDS"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
    ],
    12: [
        ("rush_attempt", "ONE"),
        ("lateral_rush", "ONE"),
        ("lateral_rusher_player_id", "PID"),
        ("lateral_rusher_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("lateral_rushing_yards", "YARDS"),
        ("rushing_yards", "SUMYARDS"),
    ],
    13: [
        ("rush_attempt", "ONE"),
        ("touchdown", "ONE"),
        ("rush_touchdown", "ONE"),
        ("lateral_rush", "ONE"),
        ("lateral_rusher_player_id", "PID"),
        ("lateral_rusher_player_name", "PNAME"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("lateral_rushing_yards", "YARDS"),
        ("rushing_yards", "SUMYARDS"),
    ],
    14: [
        ("incomplete_pass", "ONE"),
        ("pass_attempt", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
    ],
    15: [
        ("pass_attempt", "ONE"),
        ("complete_pass", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("passing_yards", "YARDS"),
    ],
    16: [
        ("pass_attempt", "ONE"),
        ("touchdown", "ONE"),
        ("pass_touchdown", "ONE"),
        ("complete_pass", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("passing_yards", "YARDS"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
    ],
    19: [
        ("interception", "ONE"),
        ("pass_attempt", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
    ],
    20: [
        ("pass_attempt", "ONE"),
        ("sack", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
    ],
    # receiving_yards is a SUMYARDS accumulator over statIds 21:24 (calculate_stats.R
    # receiving_yards = sum((stat_id %in% 21:24) * yards)) — credits the lateral receiver.
    21: [
        ("pass_attempt", "ONE"),
        ("complete_pass", "ONE"),
        ("receiver_player_id", "PID"),
        ("receiver_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("receiving_yards", "SUMYARDS"),
    ],
    22: [
        ("pass_attempt", "ONE"),
        ("touchdown", "ONE"),
        ("pass_touchdown", "ONE"),
        ("complete_pass", "ONE"),
        ("receiver_player_id", "PID"),
        ("receiver_player_name", "PNAME"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("receiving_yards", "SUMYARDS"),
    ],
    23: [
        ("pass_attempt", "ONE"),
        ("complete_pass", "ONE"),
        ("lateral_reception", "ONE"),
        ("lateral_receiver_player_id", "PID"),
        ("lateral_receiver_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("lateral_receiving_yards", "YARDS"),
        ("receiving_yards", "SUMYARDS"),
    ],
    24: [
        ("pass_attempt", "ONE"),
        ("touchdown", "ONE"),
        ("pass_touchdown", "ONE"),
        ("complete_pass", "ONE"),
        ("lateral_reception", "ONE"),
        ("lateral_receiver_player_id", "PID"),
        ("lateral_receiver_player_name", "PNAME"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("yards_gained", "YARDS"),
        ("lateral_receiving_yards", "YARDS"),
        ("receiving_yards", "SUMYARDS"),
    ],
    25: [
        ("pass_attempt", "ONE"),
        ("interception_player_id", "PID"),
        ("interception_player_name", "PNAME"),
        ("return_team", "TEAM"),
        ("return_yards", "YARDS"),
        ("return_penalty_fix", "ONE"),
    ],
    26: [
        ("pass_attempt", "ONE"),
        ("touchdown", "ONE"),
        ("return_touchdown", "ONE"),
        ("interception_player_id", "PID"),
        ("interception_player_name", "PNAME"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("return_team", "TEAM"),
        ("return_yards", "YARDS"),
        ("return_penalty_fix", "ONE"),
    ],
    27: [
        ("pass_attempt", "ONE"),
        ("lateral_return", "ONE"),
        ("lateral_interception_player_id", "PID"),
        ("lateral_interception_player_name", "PNAME"),
        ("return_yards", "YARDS"),
        ("return_penalty_fix", "ONE"),
    ],
    28: [
        ("pass_attempt", "ONE"),
        ("touchdown", "ONE"),
        ("return_touchdown", "ONE"),
        ("lateral_return", "ONE"),
        ("lateral_interception_player_id", "PID"),
        ("lateral_interception_player_name", "PNAME"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("return_yards", "YARDS"),
        ("return_penalty_fix", "ONE"),
    ],
    29: [
        ("punt_attempt", "ONE"),
        ("punter_player_id", "PID"),
        ("punter_player_name", "PNAME"),
        ("kick_distance", "YARDS"),
    ],
    30: [
        ("punt_inside_twenty", "ONE"),
        ("punt_attempt", "ONE"),
        ("punter_player_id", "PID"),
        ("punter_player_name", "PNAME"),
    ],
    31: [
        ("punt_in_endzone", "ONE"),
        ("punt_attempt", "ONE"),
        ("punter_player_id", "PID"),
        ("punter_player_name", "PNAME"),
        ("kick_distance", "YARDS"),
    ],
    32: [
        ("punt_attempt", "ONE"),
        ("kick_distance", "YARDS"),
        ("punter_player_id", "PID"),
        ("punter_player_name", "PNAME"),
    ],
    33: [
        ("punt_attempt", "ONE"),
        ("punt_returner_player_id", "PID"),
        ("punt_returner_player_name", "PNAME"),
        ("return_yards", "YARDS"),
        ("return_team", "TEAM"),
        ("return_penalty_fix", "ONE"),
    ],
    34: [
        ("touchdown", "ONE"),
        ("return_touchdown", "ONE"),
        ("punt_attempt", "ONE"),
        ("punt_returner_player_id", "PID"),
        ("punt_returner_player_name", "PNAME"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("return_team", "TEAM"),
        ("return_yards", "YARDS"),
        ("return_penalty_fix", "ONE"),
    ],
    35: [
        ("punt_attempt", "ONE"),
        ("lateral_return", "ONE"),
        ("lateral_punt_returner_player_id", "PID"),
        ("lateral_punt_returner_player_name", "PNAME"),
        ("return_yards", "YARDS"),
        ("return_penalty_fix", "ONE"),
    ],
    36: [
        ("touchdown", "ONE"),
        ("return_touchdown", "ONE"),
        ("punt_attempt", "ONE"),
        ("lateral_return", "ONE"),
        ("lateral_punt_returner_player_id", "PID"),
        ("lateral_punt_returner_player_name", "PNAME"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("return_yards", "YARDS"),
        ("return_team", "TEAM"),
        ("return_penalty_fix", "ONE"),
    ],
    37: [("punt_out_of_bounds", "ONE"), ("punt_attempt", "ONE"), ("return_yards", "ZERO"), ("return_team", "TEAM")],
    38: [("punt_downed", "ONE"), ("punt_attempt", "ONE"), ("return_team", "TEAM")],
    39: [
        ("punt_fair_catch", "ONE"),
        ("punt_attempt", "ONE"),
        ("punt_returner_player_id", "PID"),
        ("punt_returner_player_name", "PNAME"),
        ("return_team", "TEAM"),
    ],
    40: [("punt_attempt", "ONE"), ("return_team", "TEAM")],
    41: [
        ("kickoff_attempt", "ONE"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
        ("kick_distance", "YARDS"),
    ],
    42: [
        ("kickoff_inside_twenty", "ONE"),
        ("kickoff_attempt", "ONE"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
    ],
    43: [
        ("kickoff_in_endzone", "ONE"),
        ("kickoff_attempt", "ONE"),
        ("kick_distance", "YARDS"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
    ],
    44: [
        ("kickoff_attempt", "ONE"),
        ("kick_distance", "YARDS"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
    ],
    45: [
        ("kickoff_attempt", "ONE"),
        ("kickoff_returner_player_id", "PID"),
        ("kickoff_returner_player_name", "PNAME"),
        ("return_yards", "YARDS"),
        ("return_team", "TEAM"),
        ("return_penalty_fix", "ONE"),
    ],
    46: [
        ("touchdown", "ONE"),
        ("return_touchdown", "ONE"),
        ("kickoff_attempt", "ONE"),
        ("kickoff_returner_player_id", "PID"),
        ("kickoff_returner_player_name", "PNAME"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("return_yards", "YARDS"),
        ("return_team", "TEAM"),
        ("return_penalty_fix", "ONE"),
    ],
    47: [
        ("kickoff_attempt", "ONE"),
        ("lateral_return", "ONE"),
        ("lateral_kickoff_returner_player_id", "PID"),
        ("lateral_kickoff_returner_player_name", "PNAME"),
        ("return_yards", "YARDS"),
        ("return_team", "TEAM"),
        ("return_penalty_fix", "ONE"),
    ],
    48: [
        ("touchdown", "ONE"),
        ("return_touchdown", "ONE"),
        ("kickoff_attempt", "ONE"),
        ("lateral_return", "ONE"),
        ("lateral_kickoff_returner_player_id", "PID"),
        ("lateral_kickoff_returner_player_name", "PNAME"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("return_yards", "YARDS"),
        ("return_team", "TEAM"),
        ("return_penalty_fix", "ONE"),
    ],
    49: [("kickoff_out_of_bounds", "ONE"), ("kickoff_attempt", "ONE"), ("return_team", "TEAM")],
    50: [
        ("kickoff_fair_catch", "ONE"),
        ("kickoff_attempt", "ONE"),
        ("kickoff_returner_player_id", "PID"),
        ("kickoff_returner_player_name", "PNAME"),
        ("return_team", "TEAM"),
    ],
    51: [("kickoff_attempt", "ONE"), ("return_team", "TEAM")],
    52: [
        ("fumble_forced", "ONE"),
        ("fumble", "ONE"),
        ("fumbled_FILL_player_id", "PID"),
        ("fumbled_FILL_player_name", "PNAME"),
        ("fumbled_FILL_team", "TEAM"),
    ],
    53: [
        ("fumble_not_forced", "ONE"),
        ("fumble", "ONE"),
        ("fumbled_FILL_player_id", "PID"),
        ("fumbled_FILL_player_name", "PNAME"),
        ("fumbled_FILL_team", "TEAM"),
    ],
    54: [
        ("fumble_out_of_bounds", "ONE"),
        ("fumble", "ONE"),
        ("fumbled_FILL_player_id", "PID"),
        ("fumbled_FILL_player_name", "PNAME"),
        ("fumbled_FILL_team", "TEAM"),
    ],
    55: [
        ("fumble", "ONE"),
        ("fumble_recovery_FILL_player_id", "PID"),
        ("fumble_recovery_FILL_player_name", "PNAME"),
        ("fumble_recovery_FILL_team", "TEAM"),
        ("fumble_recovery_FILL_yards", "YARDS"),
    ],
    56: [
        ("touchdown", "ONE"),
        ("fumble", "ONE"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("fumble_recovery_FILL_player_id", "PID"),
        ("fumble_recovery_FILL_player_name", "PNAME"),
        ("fumble_recovery_FILL_team", "TEAM"),
        ("fumble_recovery_FILL_yards", "YARDS"),
    ],
    # 57/58 are OWN-recovery laterals, 61/62 OPP-recovery laterals: they don't count
    # as a recovery (no FILL slot) but their yards DO count toward the recovery-yards
    # totals (calculate_stats.R fumble_recovery_yards_own = sum((stat_id %in% 55:58)*yards),
    # fumble_recovery_yards_opp = sum((stat_id %in% 59:62)*yards)). Emit the lateral
    # yards as separate accumulators so the team agg adds them to the 55/56 (own) and
    # 59/60 (opp) FILL-slot recovery yards exactly.
    57: [("fumble", "ONE"), ("lateral_recovery", "ONE"), ("fumble_recovery_own_lateral_yards", "SUMYARDS")],
    58: [
        ("touchdown", "ONE"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("fumble", "ONE"),
        ("lateral_recovery", "ONE"),
        ("fumble_recovery_own_lateral_yards", "SUMYARDS"),
    ],
    59: [
        ("fumble", "ONE"),
        ("fumble_recovery_FILL_player_id", "PID"),
        ("fumble_recovery_FILL_player_name", "PNAME"),
        ("fumble_recovery_FILL_team", "TEAM"),
        ("fumble_recovery_FILL_yards", "YARDS"),
    ],
    60: [
        ("touchdown", "ONE"),
        ("return_touchdown", "ONE"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("fumble", "ONE"),
        ("fumble_recovery_FILL_player_id", "PID"),
        ("fumble_recovery_FILL_player_name", "PNAME"),
        ("fumble_recovery_FILL_team", "TEAM"),
        ("fumble_recovery_FILL_yards", "YARDS"),
    ],
    61: [("fumble", "ONE"), ("lateral_recovery", "ONE"), ("fumble_recovery_opp_lateral_yards", "SUMYARDS")],
    62: [
        ("touchdown", "ONE"),
        ("return_touchdown", "ONE"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("fumble", "ONE"),
        ("lateral_recovery", "ONE"),
        ("fumble_recovery_opp_lateral_yards", "SUMYARDS"),
    ],
    # statIds 63:64 yards = "misc yards" (calculate_stats.R misc_yards = sum((stat_id %in%
    # 63:64)*yards)) — mostly yards gained after blocked punts / FGs. 64 also scores a TD.
    63: [("misc_yards", "SUMYARDS")],
    64: [
        ("touchdown", "ONE"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("misc_yards", "SUMYARDS"),
    ],
    68: [("timeout", "ONE"), ("timeout_team", "TEAM")],
    69: [
        ("field_goal_missed", "ONE"),
        ("field_goal_attempt", "ONE"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
        ("kick_distance", "YARDS"),
    ],
    70: [
        ("field_goal_made", "ONE"),
        ("field_goal_attempt", "ONE"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
        ("kick_distance", "YARDS"),
    ],
    71: [
        ("field_goal_blocked", "ONE"),
        ("field_goal_attempt", "ONE"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
        ("kick_distance", "YARDS"),
    ],
    72: [
        ("extra_point_good", "ONE"),
        ("extra_point_attempt", "ONE"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
    ],
    73: [
        ("extra_point_failed", "ONE"),
        ("extra_point_attempt", "ONE"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
    ],
    74: [
        ("extra_point_blocked", "ONE"),
        ("extra_point_attempt", "ONE"),
        ("kicker_player_id", "PID"),
        ("kicker_player_name", "PNAME"),
    ],
    75: [
        ("two_point_rush_good", "ONE"),
        ("rush_attempt", "ONE"),
        ("two_point_attempt", "ONE"),
        ("rusher_player_id", "PID"),
        ("rusher_player_name", "PNAME"),
    ],
    76: [
        ("two_point_rush_failed", "ONE"),
        ("rush_attempt", "ONE"),
        ("two_point_attempt", "ONE"),
        ("rusher_player_id", "PID"),
        ("rusher_player_name", "PNAME"),
    ],
    77: [
        ("two_point_pass_good", "ONE"),
        ("pass_attempt", "ONE"),
        ("two_point_attempt", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
    ],
    78: [
        ("two_point_pass_failed", "ONE"),
        ("pass_attempt", "ONE"),
        ("two_point_attempt", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
    ],
    79: [
        ("solo_tackle", "ONE"),
        ("solo_tackle_FILL_player_id", "PID"),
        ("solo_tackle_FILL_player_name", "PNAME"),
        ("solo_tackle_FILL_team", "TEAM"),
    ],
    80: [
        ("tackle_with_assist", "ONE"),
        ("tackle_with_assist_FILL_player_id", "PID"),
        ("tackle_with_assist_FILL_player_name", "PNAME"),
        ("tackle_with_assist_FILL_team", "TEAM"),
    ],
    82: [
        ("assist_tackle", "ONE"),
        ("assist_tackle_FILL_player_id", "PID"),
        ("assist_tackle_FILL_player_name", "PNAME"),
        ("assist_tackle_FILL_team", "TEAM"),
    ],
    83: [("sack", "ONE"), ("sack_player_id", "PID"), ("sack_player_name", "PNAME")],
    84: [
        ("sack", "ONE"),
        ("assist_tackle", "ONE"),
        ("half_sack_FILL_player_id", "PID"),
        ("half_sack_FILL_player_name", "PNAME"),
    ],
    85: [("pass_defense_FILL_player_id", "PID"), ("pass_defense_FILL_player_name", "PNAME")],
    86: [("punt_attempt", "ONE"), ("blocked_player_id", "PID"), ("blocked_player_name", "PNAME")],
    87: [("blocked_player_id", "PID"), ("blocked_player_name", "PNAME")],
    88: [("field_goal_attempt", "ONE"), ("blocked_player_id", "PID"), ("blocked_player_name", "PNAME")],
    89: [("safety", "ONE"), ("safety_player_id", "PID"), ("safety_player_name", "PNAME")],
    91: [
        ("fumble", "ONE"),
        ("forced_fumble_player_FILL_player_id", "PID"),
        ("forced_fumble_player_FILL_player_name", "PNAME"),
        ("forced_fumble_player_FILL_team", "TEAM"),
    ],
    93: [
        ("penalty", "ONE"),
        ("penalty_player_id", "PID"),
        ("penalty_player_name", "PNAME"),
        ("penalty_team", "TEAM"),
        ("penalty_yards", "YARDS"),
    ],
    95: [("tackled_for_loss", "ONE")],
    96: [("extra_point_safety", "ONE"), ("extra_point_attempt", "ONE")],
    99: [
        ("two_point_rush_safety", "ONE"),
        ("rush_attempt", "ONE"),
        ("two_point_attempt", "ONE"),
        ("rusher_player_id", "PID"),
        ("rusher_player_name", "PNAME"),
    ],
    100: [
        ("two_point_pass_safety", "ONE"),
        ("pass_attempt", "ONE"),
        ("two_point_attempt", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
    ],
    102: [("kickoff_downed", "ONE"), ("kickoff_attempt", "ONE")],
    103: [("lateral_sack_player_id", "PID"), ("lateral_sack_player_name", "PNAME")],
    104: [
        ("two_point_pass_reception_good", "ONE"),
        ("pass_attempt", "ONE"),
        ("two_point_attempt", "ONE"),
        ("receiver_player_id", "PID"),
        ("receiver_player_name", "PNAME"),
    ],
    105: [
        ("two_point_pass_reception_failed", "ONE"),
        ("pass_attempt", "ONE"),
        ("two_point_attempt", "ONE"),
        ("receiver_player_id", "PID"),
        ("receiver_player_name", "PNAME"),
    ],
    106: [
        ("fumble_lost", "ONE"),
        ("fumble", "ONE"),
        ("fumbled_FILL_player_id", "PID"),
        ("fumbled_FILL_player_name", "PNAME"),
        ("fumbled_FILL_team", "TEAM"),
    ],
    107: [
        ("own_kickoff_recovery", "ONE"),
        ("kickoff_attempt", "ONE"),
        ("own_kickoff_recovery_player_id", "PID"),
        ("own_kickoff_recovery_player_name", "PNAME"),
    ],
    108: [
        ("own_kickoff_recovery_td", "ONE"),
        ("touchdown", "ONE"),
        ("td_team", "TEAM"),
        ("td_player_id", "PID"),
        ("td_player_name", "PNAME"),
        ("kickoff_attempt", "ONE"),
        ("own_kickoff_recovery_player_id", "PID"),
        ("own_kickoff_recovery_player_name", "PNAME"),
    ],
    110: [("qb_hit", "ONE"), ("qb_hit_FILL_player_id", "PID"), ("qb_hit_FILL_player_name", "PNAME")],
    111: [
        ("pass_attempt", "ONE"),
        ("complete_pass", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
        ("air_yards", "YARDS"),
    ],
    112: [
        ("pass_attempt", "ONE"),
        ("passer_player_id", "PID"),
        ("passer_player_name", "PNAME"),
        ("air_yards", "YARDS"),
    ],
    113: [
        ("pass_attempt", "ONE"),
        ("complete_pass", "ONE"),
        ("receiver_player_id", "PID_IFNA"),
        ("receiver_player_name", "PNAME_IFNA"),
        ("yards_after_catch", "YARDS_IFNA"),
    ],
    115: [("pass_attempt", "ONE"), ("receiver_player_id", "PID"), ("receiver_player_name", "PNAME")],
    120: [("tackle_for_loss_FILL_player_id", "PID"), ("tackle_for_loss_FILL_player_name", "PNAME")],
    301: [("extra_point_aborted", "ONE"), ("extra_point_attempt", "ONE")],
    # statId 402 = a defender's tackle-for-loss credit. calculate_stats.R
    # def_tackles_for_loss = sum(stat_id == 402) and def_tackles_for_loss_yards =
    # sum((stat_id == 402)*yards). Emit as COUNT/SUMYARDS accumulators so a play with
    # >2 TFL credits is counted exactly (the old 2-slot tackle_for_loss_FILL capped at 2).
    402: [("def_tackles_for_loss", "COUNT"), ("def_tackles_for_loss_yards", "SUMYARDS")],
    403: [("defensive_two_point_attempt", "ONE")],
    404: [("defensive_two_point_conv", "ONE")],
    405: [("defensive_extra_point_attempt", "ONE")],
    406: [("defensive_extra_point_conv", "ONE")],
    410: [("kickoff_attempt", "ONE"), ("kicker_player_id", "PID"), ("kicker_player_name", "PNAME")],
    420: [("two_point_return", "ONE"), ("two_point_attempt", "ONE")],
}

# nflfastR td_ids() (calculate_stats.R) — the statIds that count toward def_tds /
# special_teams_tds. NB: 56/58/60/62 (fumble-recovery TDs) are DELIBERATELY excluded
# (they're counted in fumble_recovery_tds, not def_tds). 18 is absent from the Shield
# effect table (never appears). Every member already sets ``touchdown`` + ``td_team``.
# We additionally emit a ``td_ids_touchdown`` COUNT so the team aggregation can compute
# def_tds = sum(td_ids_touchdown >= 1 & special != 1 & td_team == defteam) — capturing
# the blocked-kick-return TD (64) the old ``return_touchdown``-only path missed, while
# correctly leaving fumble-return TDs to fumble_recovery_tds.
TD_IDS: tuple[int, ...] = (11, 13, 16, 22, 24, 26, 28, 34, 36, 46, 48, 64, 108)
for _tid in TD_IDS:
    STAT_ID_EFFECTS.setdefault(_tid, []).append(("td_ids_touchdown", "COUNT"))
del _tid

# Ordered first-available slot sets for the ``*_FILL`` pseudo-columns.
FILL_GROUPS: Dict[str, List[str]] = {
    "fumbled_FILL_player_id": ["fumbled_1_player_id", "fumbled_2_player_id"],
    "fumbled_FILL_player_name": ["fumbled_1_player_name", "fumbled_2_player_name"],
    "fumbled_FILL_team": ["fumbled_1_team", "fumbled_2_team"],
    "fumble_recovery_FILL_player_id": ["fumble_recovery_1_player_id", "fumble_recovery_2_player_id"],
    "fumble_recovery_FILL_player_name": ["fumble_recovery_1_player_name", "fumble_recovery_2_player_name"],
    "fumble_recovery_FILL_team": ["fumble_recovery_1_team", "fumble_recovery_2_team"],
    "fumble_recovery_FILL_yards": ["fumble_recovery_1_yards", "fumble_recovery_2_yards"],
    "solo_tackle_FILL_player_id": ["solo_tackle_1_player_id", "solo_tackle_2_player_id"],
    "solo_tackle_FILL_player_name": ["solo_tackle_1_player_name", "solo_tackle_2_player_name"],
    "solo_tackle_FILL_team": ["solo_tackle_1_team", "solo_tackle_2_team"],
    "tackle_with_assist_FILL_player_id": ["tackle_with_assist_1_player_id", "tackle_with_assist_2_player_id"],
    "tackle_with_assist_FILL_player_name": ["tackle_with_assist_1_player_name", "tackle_with_assist_2_player_name"],
    "tackle_with_assist_FILL_team": ["tackle_with_assist_1_team", "tackle_with_assist_2_team"],
    "assist_tackle_FILL_player_id": [
        "assist_tackle_1_player_id",
        "assist_tackle_2_player_id",
        "assist_tackle_3_player_id",
        "assist_tackle_4_player_id",
    ],
    "assist_tackle_FILL_player_name": [
        "assist_tackle_1_player_name",
        "assist_tackle_2_player_name",
        "assist_tackle_3_player_name",
        "assist_tackle_4_player_name",
    ],
    "assist_tackle_FILL_team": [
        "assist_tackle_1_team",
        "assist_tackle_2_team",
        "assist_tackle_3_team",
        "assist_tackle_4_team",
    ],
    "half_sack_FILL_player_id": ["half_sack_1_player_id", "half_sack_2_player_id"],
    "half_sack_FILL_player_name": ["half_sack_1_player_name", "half_sack_2_player_name"],
    "pass_defense_FILL_player_id": ["pass_defense_1_player_id", "pass_defense_2_player_id"],
    "pass_defense_FILL_player_name": ["pass_defense_1_player_name", "pass_defense_2_player_name"],
    "forced_fumble_player_FILL_player_id": ["forced_fumble_player_1_player_id", "forced_fumble_player_2_player_id"],
    "forced_fumble_player_FILL_player_name": [
        "forced_fumble_player_1_player_name",
        "forced_fumble_player_2_player_name",
    ],
    "forced_fumble_player_FILL_team": ["forced_fumble_player_1_team", "forced_fumble_player_2_team"],
    "qb_hit_FILL_player_id": ["qb_hit_1_player_id", "qb_hit_2_player_id"],
    "qb_hit_FILL_player_name": ["qb_hit_1_player_name", "qb_hit_2_player_name"],
    "tackle_for_loss_FILL_player_id": ["tackle_for_loss_1_player_id", "tackle_for_loss_2_player_id"],
    "tackle_for_loss_FILL_player_name": ["tackle_for_loss_1_player_name", "tackle_for_loss_2_player_name"],
}

# Group key -> the id-slot list used to de-dup a player across slots. A FILL write is
# skipped when the entry's player id already occupies one of these id slots.
_FILL_ID_SLOTS = {
    "fumbled": "fumbled_FILL_player_id",
    "fumble_recovery": "fumble_recovery_FILL_player_id",
    "solo_tackle": "solo_tackle_FILL_player_id",
    "tackle_with_assist": "tackle_with_assist_FILL_player_id",
    "assist_tackle": "assist_tackle_FILL_player_id",
    "half_sack": "half_sack_FILL_player_id",
    "pass_defense": "pass_defense_FILL_player_id",
    "forced_fumble_player": "forced_fumble_player_FILL_player_id",
    "qb_hit": "qb_hit_FILL_player_id",
    "tackle_for_loss": "tackle_for_loss_FILL_player_id",
}


def _fill_group_prefix(fill_col: str) -> str:
    """Return the de-dup group key for a ``*_FILL_*`` pseudo-column."""
    return fill_col.split("_FILL_", 1)[0]


def sum_play_stats(stats: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Collapse a Shield play's ``stats`` array into nflverse-shape outcome columns.

    Faithful port of nflfastR ``sum_play_stats``. Iterates the per-play stat
    entries in order and applies each statType's effects. Unknown / no-op
    statTypes (and those absent from :data:`STAT_ID_EFFECTS`) are ignored,
    matching the R ``else NULL`` fall-through.

    Args:
        stats: The play's ``stats`` list. Each entry should carry ``statType``
            (int), ``yards`` (number), ``gsisPlayerId`` (str), ``gsisPlayerName``
            (str), and ``teamId`` (str). Missing keys default to ``None``/0.

    Returns:
        A flat dict of the outcome columns set by the play's stats. Binary
        indicators are ``1``; numeric columns hold their yard value; player /
        team columns hold ids / names / teamIds (TEAM resolved to abbr later).
        Columns not touched by any stat are simply absent (treat as 0 / None).

    Example:
        Sum a single completed-pass stat block::

            from sportsdataverse.nfl.shield_pbp.stat_ids import sum_play_stats
            row = sum_play_stats([
                {"statType": 15, "yards": 12, "gsisPlayerId": "00-0034796",
                 "gsisPlayerName": "L.Jackson", "teamId": "BAL"},
                {"statType": 113, "yards": 7, "gsisPlayerId": "00-0037197",
                 "gsisPlayerName": "Z.Flowers", "teamId": "BAL"},
            ])
            assert row["complete_pass"] == 1 and row["yards_after_catch"] == 7
    """
    row: Dict[str, Any] = {}

    def _val(token: str, entry: Dict[str, Any]) -> Any:
        if token in ("ONE",):
            return 1
        if token == "ZERO":
            return 0
        if token in ("YARDS", "YARDS_IFNA"):
            return entry.get("yards")
        if token in ("PID", "PID_IFNA"):
            return entry.get("gsisPlayerId")
        if token in ("PNAME", "PNAME_IFNA"):
            return entry.get("gsisPlayerName")
        if token == "TEAM":
            return entry.get("teamId")
        raise ValueError(f"Unknown source token: {token!r}")

    for entry in stats or []:
        try:
            stat_id = int(entry.get("statType"))
        except (TypeError, ValueError):
            continue
        effects = STAT_ID_EFFECTS.get(stat_id)
        if not effects:
            continue
        for col, token in effects:
            if token == "COUNT":
                # Exact per-play count accumulator (e.g. def_tackles_for_loss).
                row[col] = (row.get(col) or 0) + 1
                continue
            if token == "SUMYARDS":
                # Additive yards accumulator (e.g. misc_yards, lateral recovery yards,
                # lateral-inclusive rushing/receiving totals). None yards count as 0.
                yds = entry.get("yards")
                row[col] = (row.get(col) or 0) + (yds if yds is not None else 0)
                continue
            value = _val(token, entry)
            if "_FILL_" in col:
                slots = FILL_GROUPS[col]
                group = _fill_group_prefix(col)
                id_group_col = _FILL_ID_SLOTS[group]
                id_slots = FILL_GROUPS[id_group_col]
                pid = entry.get("gsisPlayerId")
                # Write into the first empty slot of this specific field's list.
                # De-dup matches nflfastR (helper_tidy_play_stats.R): slot 1 always
                # fills; a later slot fills only when the player isn't already in a
                # STRICTLY EARLIER id slot. Keying de-dup off the target slot's index
                # (not "any id slot") is essential — otherwise writing the _name/_team
                # of an entry whose _player_id just filled slot 1 would be wrongly
                # skipped (slot-1 id == pid), nulling the companion name/team columns.
                for idx, slot in enumerate(slots):
                    if row.get(slot) is not None:
                        continue
                    if pid is not None and any(row.get(s) == pid for s in id_slots[:idx]):
                        break  # player already in an earlier slot — don't advance
                    row[slot] = value
                    break
            elif token.endswith("_IFNA"):
                if row.get(col) is None:
                    row[col] = value
            else:
                row[col] = value

    return row
