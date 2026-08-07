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
    # Measured 2026-08-06 on the published sample: fires on ~1.2% of rows
    # (47/101 last-play-of-period, rest ordinary plays, deviations up to
    # ~1.4 EP) while the wpa twin holds exactly — epa appears to have its
    # own computation path in the cfb-data producer rather than being the
    # snapshot diff. Routed to triage instead of hard-failing.
    Rule(
        "epa_is_ep_after_minus_ep_before",
        ("epa", "ep_after", "ep_before"),
        (_c("ep_after") - _c("ep_before") - _c("epa")).abs() > _EPS,
        "epa is expected to equal ep_after - ep_before",
        severity=Severity.WARN,
        needs_judgment=True,
    ),
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
)


RULES: dict[str, tuple[Rule, ...]] = {
    "cfb_model_pbp": _CFB_RULES,
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
