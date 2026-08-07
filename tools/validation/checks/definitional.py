"""Row-level definitional consistency rules for play-by-play datasets.

Each rule is a named polars boolean expression where ``True`` means the row
VIOLATES the definition (e.g. a play flagged both ``rush`` and ``pass``).
Rules are declared per dataset in ``RULES``; ``run`` evaluates every
applicable rule's violation count in one pass and emits one Finding per
fired rule, with up to ``_SAMPLE_N`` offending rows attached for triage.

Null semantics are load-bearing: a violation expression involving a null
input evaluates to null, and ``sum()`` skips nulls — so identity rules
(e.g. ``epa == ep_after - ep_before``) automatically apply only to rows
where every input is populated. Rules therefore do NOT need explicit
``is_not_null`` guards on their own inputs, only on scope columns whose
nullness changes the rule's meaning.

Severity calibration policy (mirrors the measured-thresholds rule): a rule
ships as ERROR only if it fires zero times on the current published data
(or its firings are confirmed producer bugs); rules that fire on
legitimate rows are scoped tighter or downgraded to WARN with
``needs_judgment=True`` so the Tier-2 workflow routes them to a reviewer.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity

_SAMPLE_N = 3
_EPS = 1e-6


@dataclass(frozen=True)
class Rule:
    """One row-level definitional rule.

    Attributes:
        name: Stable identifier recorded on the finding locator.
        columns: Every column the expression reads; the rule is skipped
            (not failed) when any is absent from the frame, so one rule
            table serves datasets whose column sets drift across releases.
        violation: Boolean expression; ``True`` marks a violating row.
        description: Human-readable statement of the violated definition.
        severity: Severity when the rule fires.
        needs_judgment: Route the finding to a reviewer (WARN rules whose
            firings can be legitimate data quirks).
    """

    name: str
    columns: tuple[str, ...]
    violation: pl.Expr
    description: str
    severity: Severity = Severity.ERROR
    needs_judgment: bool = False


def _c(name: str) -> pl.Expr:
    return pl.col(name)


def _clean_string_rule(col: str) -> Rule:
    """A string column must be null or a non-empty, unpadded value.

    Content-level twin of the schema dtype check: catches the
    empty-string-as-null and whitespace-padding producer bug classes that a
    declared ``String`` dtype can't see.
    """
    return Rule(
        f"{col}_clean_string",
        (col,),
        (_c(col) == "") | (_c(col) != _c(col).str.strip_chars()),
        f"{col} must be null or a non-empty unpadded string",
    )


def _range_rule(
    col: str,
    lo: float,
    hi: float,
    *,
    severity: Severity = Severity.ERROR,
    needs_judgment: bool = False,
) -> Rule:
    """A numeric column must lie within its definitional bounds when non-null."""
    return Rule(
        f"{col}_range",
        (col,),
        ~_c(col).is_between(lo, hi),
        f"{col} must be within [{lo:g}, {hi:g}]",
        severity=severity,
        needs_judgment=needs_judgment,
    )


def _id_format_rule(col: str, pattern: str, label: str) -> Rule:
    """A non-null string id must match its canonical format.

    Subsumes the float-artifact ('123.0') and empty-string id bug classes —
    neither can match a canonical id pattern.
    """
    return Rule(
        f"{col}_format",
        (col,),
        ~_c(col).str.contains(pattern),
        f"{col} must match the {label} format",
    )


# ---------------------------------------------------------------------------
# CFB — cfb_model_pbp (cfb-data producer output)
# ---------------------------------------------------------------------------

_CFB_SCRIMMAGE = (_c("pass") == True) | (_c("rush") == True)  # noqa: E712

_CFB_RULES: tuple[Rule, ...] = (
    Rule(
        "rush_pass_mutually_exclusive",
        ("rush", "pass"),
        (_c("rush") == True) & (_c("pass") == True),  # noqa: E712
        "a play cannot be flagged both rush and pass",
    ),
    Rule(
        "completion_requires_pass",
        ("completion", "pass"),
        (_c("completion") == True) & (_c("pass") == False),  # noqa: E712
        "completion=True requires pass=True",
    ),
    Rule(
        "cpoe_requires_completion_prob",
        ("cpoe", "completion_prob"),
        _c("cpoe").is_not_null() & _c("completion_prob").is_null(),
        "cpoe is derived from completion_prob and cannot exist without it",
    ),
    Rule(
        "completion_prob_only_on_pass",
        ("completion_prob", "pass"),
        _c("completion_prob").is_not_null() & (_c("pass") == False),  # noqa: E712
        "completion probability is a pass-play model output",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "scrimmage_down_in_1_4",
        ("rush", "pass", "start.down"),
        _CFB_SCRIMMAGE & ~_c("start.down").is_between(1, 4),
        "a rush/pass play must start on down 1-4",
    ),
    Rule(
        "distance_non_negative",
        ("start.distance",),
        _c("start.distance") < 0,
        "distance-to-first-down cannot be negative",
    ),
    Rule(
        "distance_within_yards_to_endzone",
        ("rush", "pass", "start.distance", "start.yardsToEndzone"),
        _CFB_SCRIMMAGE & (_c("start.distance") > _c("start.yardsToEndzone")),
        "distance to the first-down line cannot exceed distance to the endzone",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "scrimmage_yards_to_endzone_1_99",
        ("rush", "pass", "start.yardsToEndzone"),
        _CFB_SCRIMMAGE & ~_c("start.yardsToEndzone").is_between(1, 99),
        "a scrimmage play starts 1-99 yards from the endzone",
    ),
    Rule(
        "time_secs_rem_0_1800",
        ("start.TimeSecsRem",),
        ~_c("start.TimeSecsRem").is_between(0, 1800),
        "half seconds remaining must be within [0, 1800]",
    ),
    Rule(
        "period_positive",
        ("period",),
        _c("period") < 1,
        "period must be >= 1",
    ),
    # The producer's ACTUAL epa contract (triaged 2026-08-07, verdict
    # dismissed-as-designed with 0.88 confidence; verified 0 residual on
    # both this dataset and the 3.1M-row full pbp): sdv-py
    # cfb_pbp.py:5289-5302 deliberately overlays EPA beyond the snapshot
    # diff on two play classes — non-scoring end-of-half plays get
    # EPA = -EP_start, and penalty-in-text plays (non-Penalty type,
    # non-kickoff) get EPA += EP_between. cfb-data carries the columns
    # verbatim (CARRY_RENAME). Outside those two classes the snapshot
    # identity is exact; the classes are derived here because model_pbp
    # does not ship the producer's end_of_half/penalty_in_text flags.
    Rule(
        "epa_snapshot_identity_outside_overlays",
        ("epa", "ep_after", "ep_before", "text", "type.text", "period", "game_play_number", "game_id"),
        ((_c("ep_after") - _c("ep_before") - _c("epa")).abs() > _EPS)
        & ~(
            _c("game_play_number")
            == _c("game_play_number").max().over("game_id", pl.when(_c("period") <= 2).then(1).otherwise(2))
        )
        & ~(
            _c("text").str.contains("(?i)penalt")
            & (_c("type.text") != "Penalty")
            & ~_c("type.text").str.contains("(?i)kickoff")
        ),
        "epa must equal ep_after - ep_before outside the end-of-half and penalty-in-text overlays",
    ),
    _range_rule("statYardage", -99, 99, severity=Severity.WARN, needs_judgment=True),
    Rule(
        "wpa_is_wp_after_minus_wp_before",
        ("wpa", "wp_after", "wp_before"),
        (_c("wp_after") - _c("wp_before") - _c("wpa")).abs() > _EPS,
        "wpa must equal wp_after - wp_before",
    ),
    Rule(
        "pos_team_is_a_participant",
        ("pos_team", "homeTeamId", "awayTeamId"),
        (_c("pos_team") != _c("homeTeamId")) & (_c("pos_team") != _c("awayTeamId")),
        "pos_team must be the home or away team",
    ),
    Rule(
        "pos_team_not_def_pos_team",
        ("pos_team", "def_pos_team"),
        _c("pos_team") == _c("def_pos_team"),
        "a team cannot possess and defend on the same play",
    ),
    Rule(
        "is_home_matches_pos_team",
        ("start.is_home", "pos_team", "homeTeamId"),
        _c("start.is_home") != (_c("pos_team") == _c("homeTeamId")),
        "start.is_home must equal (pos_team == homeTeamId)",
    ),
    Rule(
        "passing_down_definition",
        ("rush", "pass", "passing_down", "start.down", "start.distance"),
        _CFB_SCRIMMAGE
        & (
            _c("passing_down")
            != (
                ((_c("start.down") == 2) & (_c("start.distance") >= 8))
                | (_c("start.down").is_in([3, 4]) & (_c("start.distance") >= 5))
            )
        ),
        "passing_down must equal (down 2 & dist>=8) | (down 3/4 & dist>=5) on scrimmage plays",
    ),
    # Deliberately order-dependent: the harness contract (see
    # boundary_leakage) is that frames arrive in play order within each
    # game, and this rule VERIFIES that stored order — sorting by
    # game_play_number first would make it tautological.
    Rule(
        "game_play_number_strictly_increasing",
        ("game_id", "game_play_number"),
        _c("game_play_number").diff().over("game_id") <= 0,
        "game_play_number must strictly increase within a game (in stored row order)",
    ),
    # Participant columns (measured 2026-08-07 on the published sample):
    # passer is populated on 3141/3141 pass plays -> hard invariant; the
    # reverse direction has 3 legitimate firings (Fumble Return Touchdown
    # rows where the participants extractor found the passer on a play the
    # producer classifies as neither rush nor pass) -> triage, not error;
    # name-appears-in-text held on 3071/3144 (97.7%) -> triage.
    Rule(
        "passer_populated_on_pass",
        ("pass", "passer_player_name"),
        (_c("pass") == True) & _c("passer_player_name").is_null(),  # noqa: E712
        "a pass play must carry passer_player_name",
    ),
    Rule(
        "passer_only_on_pass_plays",
        ("pass", "passer_player_name"),
        _c("passer_player_name").is_not_null() & (_c("pass") == False),  # noqa: E712
        "passer_player_name on a non-pass play (fumble-return TDs are known-legit)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "passer_name_appears_in_text",
        ("passer_player_name", "text"),
        ~_c("text").str.contains(_c("passer_player_name"), literal=True),
        "the extracted passer name should appear verbatim in the play text",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    # Content-level type rules (measured 2026-08-07: 100% clean on the
    # published sample; drive.id is a pure numeric string — a '.0' float
    # artifact or empty string cannot match).
    _clean_string_rule("passer_player_name"),
    _id_format_rule("drive.id", r"^\d+$", "numeric-string drive id"),
)


def _nfl_pair_rule(stem: str) -> Rule:
    """Name/id columns of one participant stem must be populated together."""
    n, i = f"{stem}_player_name", f"{stem}_player_id"
    return Rule(
        f"{stem}_name_id_populated_together",
        (n, i),
        _c(n).is_null() != _c(i).is_null(),
        f"{n} and {i} must both be set or both be null",
    )


def _nfl_name_in_desc_rule(stem: str) -> Rule:
    """An extracted participant name must appear verbatim in the play desc."""
    n = f"{stem}_player_name"
    return Rule(
        f"{stem}_name_appears_in_desc",
        (n, "desc"),
        ~_c("desc").str.contains(_c(n), literal=True),
        f"the extracted {n} should appear verbatim in desc",
    )


# ---------------------------------------------------------------------------
# NFL — nfl_model_pbp (native pipeline output, nflfastR-shaped 0/1 flags)
# ---------------------------------------------------------------------------

_NFL_RULES: tuple[Rule, ...] = (
    Rule(
        "rush_pass_mutually_exclusive",
        ("rush_attempt", "pass_attempt"),
        (_c("rush_attempt") == 1) & (_c("pass_attempt") == 1),
        "a play cannot be both a rush attempt and a pass attempt",
    ),
    Rule(
        "complete_incomplete_mutually_exclusive",
        ("complete_pass", "incomplete_pass"),
        (_c("complete_pass") == 1) & (_c("incomplete_pass") == 1),
        "a pass cannot be both complete and incomplete",
    ),
    Rule(
        "complete_pass_requires_pass_attempt",
        ("complete_pass", "pass_attempt"),
        (_c("complete_pass") == 1) & (_c("pass_attempt") != 1),
        "complete_pass=1 requires pass_attempt=1",
    ),
    Rule(
        "interception_requires_pass_attempt",
        ("interception", "pass_attempt"),
        (_c("interception") == 1) & (_c("pass_attempt") != 1),
        "an interception requires a pass attempt",
    ),
    Rule(
        "sack_incompatible_with_completion",
        ("sack", "complete_pass"),
        (_c("sack") == 1) & (_c("complete_pass") == 1),
        "a sack cannot also be a completed pass",
    ),
    Rule(
        "td_subtype_implies_touchdown",
        ("pass_touchdown", "rush_touchdown", "return_touchdown", "touchdown"),
        ((_c("pass_touchdown") == 1) | (_c("rush_touchdown") == 1) | (_c("return_touchdown") == 1))
        & (_c("touchdown") != 1),
        "any touchdown subtype flag requires touchdown=1",
    ),
    Rule(
        "td_subtypes_at_most_one",
        ("pass_touchdown", "rush_touchdown", "return_touchdown"),
        (_c("pass_touchdown") + _c("rush_touchdown") + _c("return_touchdown")) > 1,
        "a play scores at most one kind of touchdown",
    ),
    Rule(
        "touchdown_implies_scoring_play",
        ("touchdown", "sp"),
        (_c("touchdown") == 1) & (_c("sp") != 1),
        "touchdown=1 requires the scoring-play flag sp=1",
    ),
    Rule(
        "fg_results_at_most_one",
        ("field_goal_made", "field_goal_missed", "field_goal_blocked"),
        (_c("field_goal_made") + _c("field_goal_missed") + _c("field_goal_blocked")) > 1,
        "a field goal attempt has at most one outcome",
    ),
    Rule(
        "fg_result_requires_attempt",
        ("field_goal_made", "field_goal_missed", "field_goal_blocked", "field_goal_attempt"),
        ((_c("field_goal_made") == 1) | (_c("field_goal_missed") == 1) | (_c("field_goal_blocked") == 1))
        & (_c("field_goal_attempt") != 1),
        "a field goal outcome flag requires field_goal_attempt=1",
    ),
    Rule(
        "fumble_lost_requires_fumble",
        ("fumble_lost", "fumble"),
        (_c("fumble_lost") == 1) & (_c("fumble") != 1),
        "fumble_lost=1 requires fumble=1",
    ),
    Rule(
        "down_in_1_4",
        ("down",),
        _c("down").is_not_null() & ~_c("down").is_between(1, 4),
        "down must be 1-4 (or null on untimed/no-down plays)",
    ),
    Rule(
        "ydstogo_non_negative",
        ("ydstogo",),
        _c("ydstogo") < 0,
        "yards to go cannot be negative",
    ),
    Rule(
        "yardline_100_range_on_downed_plays",
        ("down", "yardline_100"),
        _c("down").is_not_null() & ~_c("yardline_100").is_between(1, 99),
        "a play with a down starts 1-99 yards from the opponent endzone",
    ),
    Rule(
        "goal_to_go_means_ydstogo_is_yardline",
        ("goal_to_go", "ydstogo", "yardline_100"),
        (_c("goal_to_go") == 1) & (_c("ydstogo") != _c("yardline_100")),
        "goal-to-go means the first-down line is the goal line",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "quarter_seconds_0_900",
        ("quarter_seconds_remaining",),
        ~_c("quarter_seconds_remaining").is_between(0, 900),
        "quarter seconds remaining must be within [0, 900]",
    ),
    # Scoped to regulation: the producer's OT convention is
    # game_seconds_remaining = 0 while quarter/half count down from 600
    # (measured 2026-08-06 — all 483 unscoped firings were qtr 5).
    Rule(
        "clock_hierarchy_regulation",
        ("qtr", "quarter_seconds_remaining", "half_seconds_remaining", "game_seconds_remaining"),
        (_c("qtr") <= 4)
        & (
            (_c("half_seconds_remaining") < _c("quarter_seconds_remaining"))
            | (_c("game_seconds_remaining") < _c("half_seconds_remaining"))
        ),
        "in regulation, game clock >= half clock >= quarter clock",
    ),
    Rule(
        "score_differential_identity",
        ("score_differential", "posteam_score", "defteam_score"),
        _c("score_differential") != (_c("posteam_score") - _c("defteam_score")),
        "score_differential must equal posteam_score - defteam_score",
    ),
    Rule(
        "posteam_timeouts_0_3",
        ("posteam_timeouts_remaining",),
        ~_c("posteam_timeouts_remaining").is_between(0, 3),
        "a team has 0-3 timeouts",
    ),
    Rule(
        "defteam_timeouts_0_3",
        ("defteam_timeouts_remaining",),
        ~_c("defteam_timeouts_remaining").is_between(0, 3),
        "a team has 0-3 timeouts",
    ),
    Rule(
        "kneel_spike_mutually_exclusive",
        ("qb_kneel", "qb_spike"),
        (_c("qb_kneel") == 1) & (_c("qb_spike") == 1),
        "a play cannot be both a kneel and a spike",
    ),
    Rule(
        "kickoff_punt_mutually_exclusive",
        ("kickoff_attempt", "punt_attempt"),
        (_c("kickoff_attempt") == 1) & (_c("punt_attempt") == 1),
        "a play cannot be both a kickoff and a punt",
    ),
    Rule(
        "play_type_run_has_rush_attempt",
        ("play_type", "rush_attempt"),
        (_c("play_type") == "run") & (_c("rush_attempt") != 1),
        "play_type='run' should carry rush_attempt=1",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "play_type_pass_has_dropback_flag",
        ("play_type", "pass_attempt", "sack"),
        (_c("play_type") == "pass") & (_c("pass_attempt") != 1) & (_c("sack") != 1),
        "play_type='pass' should carry pass_attempt=1 (or be a sack)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "air_plus_yac_is_passing_yards",
        ("complete_pass", "passing_yards", "air_yards", "yards_after_catch"),
        (_c("complete_pass") == 1) & ((_c("air_yards") + _c("yards_after_catch")) != _c("passing_yards")),
        "on a completion, air_yards + yards_after_catch should equal passing_yards",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "home_away_wp_complement",
        ("home_wp", "away_wp"),
        (_c("home_wp") + _c("away_wp") - 1.0).abs() > _EPS,
        "home_wp + away_wp must sum to 1",
    ),
    # Participant columns (all measured 2026-08-07 at 100% on 94,978
    # published rows, both directions unless noted): name/id pairs never
    # half-populate; passer/rusher are iff their attempt flag (passer
    # includes sacks); receiver is required on completions but legitimately
    # null on ~14% of incompletes (throwaways) so only that direction is a
    # rule; td/penalty/timeout columns are iff their flag; every extracted
    # name appears verbatim in desc.
    _nfl_pair_rule("passer"),
    _nfl_pair_rule("rusher"),
    _nfl_pair_rule("receiver"),
    _nfl_pair_rule("td"),
    Rule(
        "passer_iff_pass_attempt",
        ("pass_attempt", "passer_player_name"),
        (_c("pass_attempt") == 1) != _c("passer_player_name").is_not_null(),
        "passer_player_name must be populated exactly on pass attempts (incl. sacks)",
    ),
    Rule(
        "rusher_iff_rush_attempt",
        ("rush_attempt", "rusher_player_name"),
        (_c("rush_attempt") == 1) != _c("rusher_player_name").is_not_null(),
        "rusher_player_name must be populated exactly on rush attempts",
    ),
    Rule(
        "receiver_populated_on_completion",
        ("complete_pass", "receiver_player_name"),
        (_c("complete_pass") == 1) & _c("receiver_player_name").is_null(),
        "a completed pass must carry receiver_player_name",
    ),
    Rule(
        "receiver_requires_pass_attempt",
        ("pass_attempt", "receiver_player_name"),
        _c("receiver_player_name").is_not_null() & (_c("pass_attempt") != 1),
        "receiver_player_name requires a pass attempt",
    ),
    Rule(
        "td_player_iff_touchdown",
        ("touchdown", "td_player_name"),
        (_c("touchdown") == 1) != _c("td_player_name").is_not_null(),
        "td_player_name must be populated exactly on touchdowns",
    ),
    Rule(
        "td_team_iff_touchdown",
        ("touchdown", "td_team"),
        (_c("touchdown") == 1) != _c("td_team").is_not_null(),
        "td_team must be populated exactly on touchdowns",
    ),
    Rule(
        "td_team_is_a_participant",
        ("td_team", "home_team", "away_team"),
        (_c("td_team") != _c("home_team")) & (_c("td_team") != _c("away_team")),
        "td_team must be the home or away team",
    ),
    Rule(
        "penalty_team_iff_penalty",
        ("penalty", "penalty_team"),
        (_c("penalty") == 1) != _c("penalty_team").is_not_null(),
        "penalty_team must be populated exactly on penalty plays",
    ),
    Rule(
        "timeout_team_iff_timeout",
        ("timeout", "timeout_team"),
        (_c("timeout") == 1) != _c("timeout_team").is_not_null(),
        "timeout_team must be populated exactly on timeout rows",
    ),
    _nfl_name_in_desc_rule("passer"),
    _nfl_name_in_desc_rule("rusher"),
    _nfl_name_in_desc_rule("receiver"),
    _nfl_name_in_desc_rule("td"),
    # Content-level type rules (all measured 2026-08-07 at 100% on 94,978
    # published rows): every player id is GSIS-format, game_id is the
    # nflverse composite key, no empty/padded strings, and posteam/defteam
    # are always distinct game participants when set (null posteam is legit
    # on ~1% of rows: kickoffs/timeouts/period markers).
    _id_format_rule("passer_player_id", r"^\d{2}-\d{7}$", "GSIS id"),
    _id_format_rule("rusher_player_id", r"^\d{2}-\d{7}$", "GSIS id"),
    _id_format_rule("receiver_player_id", r"^\d{2}-\d{7}$", "GSIS id"),
    _id_format_rule("td_player_id", r"^\d{2}-\d{7}$", "GSIS id"),
    _id_format_rule("game_id", r"^\d{4}_\d{2}_[A-Z]{2,3}_[A-Z]{2,3}$", "nflverse game id"),
    _clean_string_rule("passer_player_name"),
    _clean_string_rule("rusher_player_name"),
    _clean_string_rule("receiver_player_name"),
    _clean_string_rule("td_player_name"),
    Rule(
        "posteam_is_a_participant",
        ("posteam", "home_team", "away_team"),
        (_c("posteam") != _c("home_team")) & (_c("posteam") != _c("away_team")),
        "posteam must be the home or away team",
    ),
    Rule(
        "defteam_is_a_participant",
        ("defteam", "home_team", "away_team"),
        (_c("defteam") != _c("home_team")) & (_c("defteam") != _c("away_team")),
        "defteam must be the home or away team",
    ),
    Rule(
        "posteam_not_defteam",
        ("posteam", "defteam"),
        _c("posteam") == _c("defteam"),
        "a team cannot possess and defend on the same play",
    ),
    # Penalty + yardage relationships (measured 2026-08-07 on 94,978
    # published rows). penalty=1 always has a desc mention, penalty_yards
    # both ways, and first_down_penalty=>penalty all held at 100%. The
    # reverse desc direction has 962 legit firings (declined/offsetting
    # penalties narrated without penalty=1) -> triage. Play types observed
    # on penalty=1 rows: no_play dominates (4,935) but run/pass/punt/
    # kickoff accepted-penalty plays are legitimate, so no play_type rule.
    Rule(
        "penalty_implies_desc_mention",
        ("penalty", "desc"),
        (_c("penalty") == 1) & ~_c("desc").str.contains("(?i)penalty"),
        "penalty=1 requires a penalty mention in desc",
    ),
    Rule(
        "desc_penalty_mention_without_flag",
        ("penalty", "desc"),
        _c("desc").str.contains("(?i)penalty") & (_c("penalty") != 1),
        "desc mentions a penalty but penalty!=1 (declined/offset narrations are known-legit)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "penalty_yards_populated_on_penalty",
        ("penalty", "penalty_yards"),
        (_c("penalty") == 1) & _c("penalty_yards").is_null(),
        "a penalty play must carry penalty_yards",
    ),
    Rule(
        "penalty_yards_nonzero_requires_penalty",
        ("penalty", "penalty_yards"),
        (_c("penalty_yards") != 0) & (_c("penalty") != 1),
        "nonzero penalty_yards requires penalty=1",
    ),
    Rule(
        "first_down_penalty_requires_penalty",
        ("first_down_penalty", "penalty"),
        (_c("first_down_penalty") == 1) & (_c("penalty") != 1),
        "a penalty first down requires penalty=1",
    ),
    Rule(
        "rushing_yards_requires_rush_attempt",
        ("rushing_yards", "rush_attempt"),
        _c("rushing_yards").is_not_null() & (_c("rush_attempt") != 1),
        "rushing_yards requires a rush attempt",
    ),
    Rule(
        "passing_yards_requires_pass_attempt",
        ("passing_yards", "pass_attempt"),
        _c("passing_yards").is_not_null() & (_c("pass_attempt") != 1),
        "passing_yards requires a pass attempt",
    ),
    Rule(
        "receiving_equals_passing_on_completion",
        ("complete_pass", "receiving_yards", "passing_yards"),
        (_c("complete_pass") == 1) & (_c("receiving_yards") != _c("passing_yards")),
        "receiving_yards should equal passing_yards on a completion (laterals diverge)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "passing_yards_equals_yards_gained_on_completion",
        ("complete_pass", "passing_yards", "yards_gained"),
        (_c("complete_pass") == 1) & (_c("passing_yards") != _c("yards_gained")),
        "passing_yards should equal yards_gained on a completion (laterals diverge)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "rushing_yards_equals_yards_gained_on_rush",
        ("rush_attempt", "rushing_yards", "yards_gained"),
        (_c("rush_attempt") == 1) & (_c("rushing_yards") != _c("yards_gained")),
        "rushing_yards should equal yards_gained on a rush (laterals diverge)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "kick_distance_requires_kick_play",
        ("kick_distance", "punt_attempt", "kickoff_attempt", "field_goal_attempt"),
        _c("kick_distance").is_not_null()
        & (_c("punt_attempt") != 1)
        & (_c("kickoff_attempt") != 1)
        & (_c("field_goal_attempt") != 1),
        "kick_distance requires a punt, kickoff, or field goal attempt",
    ),
    Rule(
        "kick_distance_populated_on_kickoff",
        ("kick_distance", "kickoff_attempt"),
        (_c("kickoff_attempt") == 1) & _c("kick_distance").is_null(),
        "a kickoff must carry kick_distance",
    ),
    Rule(
        "kick_distance_populated_on_punt",
        ("kick_distance", "punt_attempt"),
        (_c("punt_attempt") == 1) & _c("kick_distance").is_null(),
        "a punt should carry kick_distance (blocked punts are known-legit nulls)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    _range_rule("penalty_yards", 0, 99),
    _range_rule("yards_gained", -99, 99),
    _range_rule("air_yards", -99, 99),
    _range_rule("yards_after_catch", -99, 99),
    _range_rule("passing_yards", -99, 99),
    _range_rule("rushing_yards", -99, 99),
    _range_rule("receiving_yards", -99, 99),
    _range_rule("kick_distance", 0, 99),
)


def _cfb_penalty_subtype_rule(flag: str) -> Rule:
    """A penalty subtype/outcome flag requires the base penalty_flag."""
    return Rule(
        f"{flag}_requires_penalty_flag",
        (flag, "penalty_flag"),
        (_c(flag) == True) & (_c("penalty_flag") == False),  # noqa: E712
        f"{flag}=True requires penalty_flag=True",
    )


# Full play_by_play dataset (projected read; see registry read_columns).
# All measured 2026-08-07 across 3,145,840 rows / 22 seasons. ERROR rules
# held at 100%; WARN rules fire on real data and carry the measured count:
# yds_penalty is non-numeric on 80,478 of 200,177 non-null rows (samples
# 'U (', 'k 8', ' 37' — a real producer extraction bug), statYardage has
# impossible outliers (-5114..11131), yds_fg max 109, yds_kickoff has a
# 999 sentinel, penalty_yards_signed max 131 (>99 is impossible on a
# football field), declined&no_play co-fire on 409 rows, and penalty_text
# is null on 45 penalty_flag rows. isPenalty vs penalty_flag disagree on
# 55,479 rows and penalty_in_text is narrower than a text regex by design
# (167,776 rows) — different contracts, deliberately NOT ruled.
_CFB_PBP_RULES: tuple[Rule, ...] = (
    _cfb_penalty_subtype_rule("penalty_declined"),
    _cfb_penalty_subtype_rule("penalty_no_play"),
    _cfb_penalty_subtype_rule("penalty_offset"),
    _cfb_penalty_subtype_rule("penalty_1st_conv"),
    _cfb_penalty_subtype_rule("penalty_safety"),
    _cfb_penalty_subtype_rule("penalty_assessed_on_kickoff"),
    Rule(
        "penalty_type_play_has_flag",
        ("type.text", "penalty_flag"),
        (_c("type.text") == "Penalty") & (_c("penalty_flag") == False),  # noqa: E712
        "a Penalty-type play must carry penalty_flag=True",
    ),
    Rule(
        "penalty_text_requires_flag",
        ("penalty_text", "penalty_flag"),
        _c("penalty_text").is_not_null() & (_c("penalty_flag") == False),  # noqa: E712
        "penalty_text requires penalty_flag=True",
    ),
    Rule(
        "penalty_text_populated_on_flag",
        ("penalty_text", "penalty_flag"),
        (_c("penalty_flag") == True) & _c("penalty_text").is_null(),  # noqa: E712
        "a flagged penalty should carry penalty_text (45 known gaps)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "penalty_in_text_means_text_mentions",
        ("penalty_in_text", "text"),
        (_c("penalty_in_text") == True) & ~_c("text").str.contains("(?i)penalt"),  # noqa: E712
        "penalty_in_text=True requires a penalty mention in the play text",
    ),
    Rule(
        "penalty_yards_signed_nonzero_requires_flag",
        ("penalty_yards_signed", "penalty_flag"),
        (_c("penalty_yards_signed") != 0) & (_c("penalty_flag") == False),  # noqa: E712
        "nonzero penalty_yards_signed requires penalty_flag=True",
    ),
    Rule(
        "penalty_yards_signed_populated_on_flag",
        ("penalty_yards_signed", "penalty_flag"),
        (_c("penalty_flag") == True) & _c("penalty_yards_signed").is_null(),  # noqa: E712
        "a flagged penalty must carry penalty_yards_signed",
    ),
    _range_rule("penalty_yards_signed", -99, 99, severity=Severity.WARN, needs_judgment=True),
    Rule(
        "yds_penalty_numeric_string",
        ("yds_penalty",),
        ~_c("yds_penalty").str.contains(r"^-?\d+$"),
        "yds_penalty should be a signed numeric string (40% garbage measured — real extraction bug)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "penalty_declined_no_play_exclusive",
        ("penalty_declined", "penalty_no_play"),
        (_c("penalty_declined") == True) & (_c("penalty_no_play") == True),  # noqa: E712
        "a declined penalty should not also be a no-play (409 measured co-fires)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "epa_snapshot_identity_outside_overlays",
        ("EPA", "EP_end", "EP_start", "end_of_half", "scoring_play", "penalty_in_text", "type.text", "kickoff_play"),
        ((_c("EP_end") - _c("EP_start") - _c("EPA")).abs() > _EPS)
        & ~((_c("end_of_half") == True) & (_c("scoring_play") == False))  # noqa: E712
        & ~(
            (_c("penalty_in_text") == True)  # noqa: E712
            & (_c("type.text") != "Penalty")
            & (_c("kickoff_play") == False)  # noqa: E712
        ),
        "EPA must equal EP_end - EP_start outside the end-of-half and penalty-in-text overlays",
    ),
    Rule(
        "game_play_number_strictly_increasing",
        ("game_id", "game_play_number"),
        _c("game_play_number").diff().over("game_id") <= 0,
        "game_play_number must strictly increase within a game (in stored row order)",
    ),
    _range_rule("statYardage", -99, 99, severity=Severity.WARN, needs_judgment=True),
    _range_rule("yds_rushed", -99, 99),
    _range_rule("yds_receiving", -99, 99),
    _range_rule("yds_sacked", -99, 0),
    _range_rule("yds_punted", 0, 99),
    _range_rule("yds_fg", 0, 99, severity=Severity.WARN, needs_judgment=True),
    _range_rule("yds_kickoff", 0, 99, severity=Severity.WARN, needs_judgment=True),
    _range_rule("yds_kickoff_return", 0, 100),
    _range_rule("yds_punt_return", 0, 100),
    _range_rule("yds_int_return", -100, 100),
    _range_rule("yds_fumble_return", 0, 100),
    _range_rule("air_yards", -99, 99),
    _range_rule("yards_after_catch", -99, 100),
    # --- play-mechanics grammar -------------------------------------------
    # Measured 2026-08-07 across all 3,145,840 rows / 22 seasons. The eight
    # ERROR rules below held at EXACTLY zero violations; everything that
    # fired at all is a WARN carrying its measured count, so a regression
    # shows up as a count change rather than a silent pass.
    Rule(
        "sack_implies_pass",
        ("sack", "pass"),
        (_c("sack") == True) & (_c("pass") == False),  # noqa: E712
        "a sack is a pass play",
    ),
    Rule(
        "completion_implies_pass",
        ("completion", "pass"),
        (_c("completion") == True) & (_c("pass") == False),  # noqa: E712
        "a completion requires pass=True",
    ),
    Rule(
        "target_implies_pass",
        ("target", "pass"),
        (_c("target") == True) & (_c("pass") == False),  # noqa: E712
        "a target requires pass=True",
    ),
    Rule(
        "fumble_lost_implies_fumble",
        ("fumble_lost", "fumble_vec"),
        (_c("fumble_lost") == True) & (_c("fumble_vec") == False),  # noqa: E712
        "a lost fumble requires a fumble",
    ),
    Rule(
        "fg_made_implies_fg_attempt",
        ("fg_made", "fg_attempt"),
        (_c("fg_made") == True) & (_c("fg_attempt") == False),  # noqa: E712
        "a made field goal requires a field goal attempt",
    ),
    Rule(
        "int_implies_turnover",
        ("int", "turnover_vec"),
        (_c("int") == True) & (_c("turnover_vec") == False),  # noqa: E712
        "an interception is a turnover",
    ),
    Rule(
        "kneel_implies_rush",
        ("kneel_down", "rush"),
        (_c("kneel_down") == True) & (_c("rush") == False),  # noqa: E712
        "a kneel-down is a rush play",
    ),
    Rule(
        "kickoff_scrimmage_exclusive",
        ("kickoff_play", "scrimmage_play"),
        (_c("kickoff_play") == True) & (_c("scrimmage_play") == True),  # noqa: E712
        "a kickoff is not a scrimmage play",
    ),
    # --- measured-nonzero: WARN with the count observed on publish ---------
    Rule(
        "rush_pass_exclusive",
        ("rush", "pass"),
        (_c("rush") == True) & (_c("pass") == True),  # noqa: E712
        "a play cannot be flagged both rush and pass (7 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "kickoff_punt_exclusive",
        ("kickoff_play", "punt_play"),
        (_c("kickoff_play") == True) & (_c("punt_play") == True),  # noqa: E712
        "a play cannot be both a kickoff and a punt (3 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "punt_scrimmage_exclusive",
        ("punt_play", "scrimmage_play"),
        (_c("punt_play") == True) & (_c("scrimmage_play") == True),  # noqa: E712
        "a punt is not a scrimmage play (116 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "pass_implies_scrimmage",
        ("pass", "scrimmage_play"),
        (_c("pass") == True) & (_c("scrimmage_play") == False),  # noqa: E712
        "a pass is a scrimmage play (12 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "rush_implies_scrimmage",
        ("rush", "scrimmage_play"),
        (_c("rush") == True) & (_c("scrimmage_play") == False),  # noqa: E712
        "a rush is a scrimmage play (7,249 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    # This is the rule that would have caught the 2013 sack outage: that
    # season carries 52 sacks (2012: 3,254 / 2014: 3,537) and yds_sacked is
    # 100% null, so every surviving sack row violates the pairing.
    Rule(
        "sack_requires_yds_sacked",
        ("sack", "yds_sacked"),
        (_c("sack") == True) & _c("yds_sacked").is_null(),  # noqa: E712
        "a sack must carry sack yardage (105 measured; 52 are the 2013 outage)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "td_play_implies_scoring",
        ("td_play", "scoring_play"),
        (_c("td_play") == True) & (_c("scoring_play") == False),  # noqa: E712
        "a touchdown play should be a scoring play (3,031 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "fumble_lost_implies_turnover",
        ("fumble_lost", "turnover_vec"),
        (_c("fumble_lost") == True) & (_c("turnover_vec") == False),  # noqa: E712
        "a lost fumble is a turnover (3,514 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "int_implies_pass",
        ("int", "pass"),
        (_c("int") == True) & (_c("pass") == False),  # noqa: E712
        "an interception is a pass play (12,890 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "completion_requires_yds_receiving",
        ("completion", "yds_receiving"),
        (_c("completion") == True) & _c("yds_receiving").is_null(),  # noqa: E712
        "a completion must carry receiving yardage (3,377 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
    Rule(
        "rush_requires_yds_rushed",
        ("rush", "yds_rushed"),
        (_c("rush") == True) & _c("yds_rushed").is_null(),  # noqa: E712
        "a rush must carry rushing yardage (35,369 measured)",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
)


RULES: dict[str, tuple[Rule, ...]] = {
    "cfb_model_pbp": _CFB_RULES,
    "cfb_pbp": _CFB_PBP_RULES,
    "nfl_model_pbp": _NFL_RULES,
}


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Evaluate the dataset's definitional rules and report violations.

    Rules whose columns are absent from the frame are skipped (schema drift
    is the ``schema_contract`` check's finding, not a definitional one).
    All applicable rules' violation counts are computed in a single pass;
    fired rules each get one Finding carrying the violation count as
    ``metric`` and up to ``_SAMPLE_N`` offending rows (join keys + the
    rule's own columns) as ``sample``.

    Args:
        dataset: Registered dataset name (selects the rule table).
        frame: The data frame under validation.
        ctx: Check context supplying domain and join keys.

    Returns:
        A list of Finding records; empty when no rules are registered for
        the dataset or nothing fires.
    """
    rules = [r for r in RULES.get(dataset, ()) if all(c in frame.columns for c in r.columns)]
    if not rules:
        return []

    try:
        counts = frame.select([r.violation.sum().cast(pl.Int64).alias(r.name) for r in rules]).row(0, named=True)
    except pl.exceptions.PolarsError:
        # A degenerate column dtype (e.g. an all-null series polars typed as
        # Null) breaks expression resolution — as SchemaError for
        # str.strip_chars but InvalidOperationError for str.contains and abs
        # on polars 1.42, and ComputeError for other shapes. Fall back to
        # per-rule evaluation and skip only the unresolvable rules so one
        # degenerate column cannot suppress every other rule's findings —
        # the dtype divergence itself is schema_contract's finding, not a
        # definitional violation. (Rule-authoring errors are covered by the
        # per-rule unit tests, not this handler.)
        counts = {}
        for r in rules:
            try:
                counts[r.name] = frame.select(r.violation.sum().cast(pl.Int64)).item()
            except pl.exceptions.PolarsError:
                counts[r.name] = 0

    findings: list[Finding] = []
    for rule in rules:
        n = counts[rule.name] or 0
        if n == 0:
            continue
        sample_cols = [k for k in ctx.join_keys if k in frame.columns]
        sample_cols += [c for c in rule.columns if c not in sample_cols]
        sample = frame.filter(rule.violation).select(sample_cols).head(_SAMPLE_N).to_dicts()
        findings.append(
            Finding(
                "definitional",
                rule.severity,
                ctx.domain,
                dataset,
                f"{rule.name}: {rule.description} ({n} violating row(s))",
                locator={"rule": rule.name, "columns": list(rule.columns)},
                metric=float(n),
                needs_judgment=rule.needs_judgment,
                sample=sample,
            )
        )
    return findings
