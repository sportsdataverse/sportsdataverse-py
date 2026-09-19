"""Per-game validation gate: :func:`validate_game` and the report it returns.

One pure, offline function every consumer can call on a processed game --
:class:`~sportsdataverse.nfl.NFLPlayProcess` / :class:`~sportsdataverse.cfb.CFBPlayProcess`
output, a release build stage, Game on Paper's live route. It scores the frame
against the invariant rules in :mod:`sportsdataverse.validation.pbp_invariants`
(the same table the offline sweep runs) and returns a small, JSON-serialisable
:class:`GameReport`.

A rule's raw severity is whatever the rule table gives it; :data:`RULE_SCOPE`
then narrows it per league, per source and per era. Scoping is what makes the
gate usable: the sweep measured several rules that fire on ESPN's *feed* rather
than on the processor (surplus timeout rows in modern CFB, goal-to-go text that
contradicts the spot, pre-2015 score rows re-entered after the scoring play,
NFL touchdown yardage that is ESPN's own arithmetic), so those read ``warn``
instead of failing a build. Nothing is dropped -- every fired rule is counted
in ``counts_by_rule`` whatever its severity.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from sportsdataverse.validation.findings import Severity
from sportsdataverse.validation.pbp_invariants import RuleResult, evaluate

#: Seasons a rule is trusted through when its scope has no upper bound.
_OPEN = 2100

#: Sources :func:`validate_game` knows: ESPN plus the adapted alternates.
SOURCES = ("espn", "shield", "cbs", "yahoo", "fox", "ncaa")


@dataclass(frozen=True)
class Rule:
    """Scope metadata for one invariant rule.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | id | str | the rule's stable ``<group>.<name>`` identifier |
        | severity | Severity | severity inside the rule's scope (``error`` / ``warn`` / ``info``) |
        | era_scope | dict[str, tuple[int, int]] or None | ``{league: (first_season, last_season)}`` the rule is meaningful in; outside it the finding is demoted to ``warn``. None = every league and season |
        | leagues | tuple[str, ...] | leagues the rule applies to at all; others skip it |
        | sources | tuple[str, ...] | sources the rule applies to at all; others skip it |
        | note | str | why the rule is scoped this way |
    """

    id: str
    severity: Severity = Severity.ERROR
    era_scope: Mapping[str, tuple[int, int]] | None = None
    leagues: tuple[str, ...] = ("nfl", "cfb")
    sources: tuple[str, ...] = SOURCES
    note: str = ""

    def applies(self, league: str, source: str) -> bool:
        return league in self.leagues and source in self.sources

    def severity_for(self, league: str, season: int | None) -> Severity:
        """The rule's severity for this game: demoted to ``warn`` outside ``era_scope``."""
        if self.severity is Severity.INFO or self.era_scope is None:
            return self.severity
        window = self.era_scope.get(league)
        if window is None:  # the league was measured as out of scope entirely
            return Severity.WARN
        if season is None:  # an unresolvable season must not silently open the gate
            return self.severity
        return self.severity if window[0] <= season <= window[1] else Severity.WARN


def _scope(*rules: Rule) -> dict[str, Rule]:
    return {r.id: r for r in rules}


#: Rules whose severity, league, source or era differs from the rule table's default.
#: Every entry cites the sweep finding that measured it
#: (``background-research/2026-09-17-football-sources-program/s1-sweep``).
RULE_SCOPE: dict[str, Rule] = _scope(
    Rule(
        "timeouts.timeout_row_not_charged",
        Severity.WARN,
        sources=("espn",),
        note="run3 R2/O8: ESPN emits more than three team-named timeout rows per half "
        "(CFB 2023-26) and null timeout text pre-2010 -- a feed defect, not the processor",
    ),
    Rule(
        "down.goal_to_go_distance_eq_ytg",
        Severity.WARN,
        sources=("espn",),
        note="run3 R3/O9: fires on rows whose ESPN downDistanceText contradicts the corrected spot",
    ),
    Rule(
        "score.scoring_play_without_change",
        Severity.WARN,
        era_scope={"nfl": (2015, _OPEN), "cfb": (2015, _OPEN)},
        note="run3 R4/O7 (ledger C41): 2002-14 feeds re-enter late rows carrying the pre-score value",
    ),
    Rule(
        "ytg.td_start_eq_yards_gained",
        era_scope={"cfb": (2002, _OPEN)},
        note="run3 R5: every NFL violation is ESPN's own yardage vs its own start.yardsToEndzone",
    ),
    Rule(
        "ytg.td_start_eq_stat_yardage",
        Severity.WARN,
        note="run3 R5, the statYardage twin of ytg.td_start_eq_yards_gained",
    ),
    Rule(
        "ytg.start_matches_down_distance_text",
        era_scope={"nfl": (2025, _OPEN)},
        sources=("espn",),
        note="FINDINGS N6: ESPN's own 'at HOU 32' disagrees with its spot on 3-11% of NFL "
        "scrimmage plays 2015-24 and on none from 2025; run3 O13 triaged the CFB residual as source",
    ),
    Rule(
        "down.scrimmage_distance_ge_1",
        era_scope={"nfl": (2002, _OPEN)},
        note="run3 O13: the CFB residual (0.32%) is ESPN's own down-distance, triaged as source. "
        "Not source-scoped: the rule reads only start.distance, so a sub-1 distance from an "
        "adapted source is the adapter's defect and must still fail",
    ),
    Rule(
        "flags.rush_pass_not_scrimmage",
        Severity.WARN,
        note="run3 O13: 'TEAM run for a loss' kneels and tries -- a convention call, not a defect",
    ),
    Rule(
        "ytg.continuity_next_snap",
        Severity.WARN,
        note="ESPN's end.distance matches the next start.distance on .30-.78 of adjacent "
        "pairs 2018-24 (.92 in 2025-26) -- CBS NFL adapter gate G1",
    ),
    Rule(
        "poss.end_team_flips_without_change",
        Severity.WARN,
        sources=("espn",),
        note="run3 O10: measures the ESPN feed's own end.team flip on a retained possession",
    ),
    Rule(
        "wp.home_wp_continuity",
        Severity.WARN,
        note="run3 O12: diagnostic by design -- start and end WP are modelled independently",
    ),
    Rule(
        "wp.wpa_sums_to_result",
        Severity.WARN,
        note="run3 O4: still true of ~70% of games; open whether the metric or the values are wrong",
    ),
    Rule(
        "flags.no_play_counted_as_attempt",
        Severity.WARN,
        note="FINDINGS §1: nflfastR also keeps pass/rush on a nullified play -- a convention call",
    ),
    Rule(
        "type.null_abbreviation",
        Severity.WARN,
        note="ESPN itself emits a null abbreviation on Sack / Pass Incompletion (sources/contract.py)",
    ),
    Rule(
        "type.null_id",
        era_scope={"nfl": (2010, _OPEN), "cfb": (2010, _OPEN)},
        note="the stored 2002-09 summaries carry rows with no type at all (run3 O5)",
    ),
    Rule(
        "period.monotone",
        Severity.WARN,
        note="the ESPN feed itself carries out-of-order period rows: cfb 401628439's raw "
        "drives.previous has a period-2 row after period 6, and 401752913's interception "
        "return sorts by play id into the next drive's block -- 2/105 games on the V1 sweep, "
        "both feed order, neither a processor defect",
    ),
    Rule(
        "box.usage_shares_sum_to_one",
        Severity.WARN,
        note="usage_box._team_totals takes each share's denominator from EVERY standing "
        "scrimmage play, so an unattributed target leaves a team's shares summing to LESS "
        "than one: the shortfall is attribution coverage (attr.* measures it), not a box "
        "defect. The rule still fails closed on a share that is not its own count over the "
        "team's, and on a team whose shares sum above one",
    ),
    Rule(
        "plays.unexplained_drop",
        era_scope={"nfl": (2010, _OPEN), "cfb": (2010, _OPEN)},
        note="FINDINGS N5: the constant yardsToEndzone=0 of 2002-09 makes consecutive snaps look identical",
    ),
)

#: The V1b ``advBoxScore`` reconciliations (``box.team_totals_match_plays``,
#: ``box.player_sums_match_team``, ``box.rates_recompute_from_counts``,
#: ``box.sections_mirror_off_def``, ``box.turnovers_match_flags``,
#: ``box.drives_match_drive_rows``, ``box.team_ids_in_game``) keep the table's ``error``:
#: they fired on 0 of the 80-game V1b gate slice (nfl + cfb, 2012-2025), so there is no
#: measurement that would justify a demotion. Attribution coverage (``attr.*``) and the
#: ESPN box parity rules (``box.*_vs_espn``) are INFO / WARN in the rule table itself and stay there: 2002-09 text is null or a stub (run3 O5) and the
#: pre-2015 stored summaries are incomplete against ESPN's own box (run3 O11).


# ---------------------------------------------------------------------------
# per-source column maps and not-applicable rules
# ---------------------------------------------------------------------------

#: ESPN-shaped column -> expression over an adapted source's own columns.
#:
#: The rule table is written against the ESPN processor vocabulary
#: (``start.yardsToEndzone``, ``type.text``, ``end.homeScore`` ...). A source
#: whose producer emits the same quantity under a different name was silently
#: skipping every rule that read it. :func:`validate_game` materialises these
#: as extra columns on a *view* of the frame before evaluating -- the caller's
#: frame is not mutated and nothing is published from it, so the published
#: schema of an adapted source stays exactly what its producer defines.
#:
#: Only genuine renames and arithmetic identities belong here. Where the
#: source cannot supply the quantity, the rule is named in
#: :data:`NOT_APPLICABLE` instead -- never faked into passing.
SOURCE_COLUMNS: dict[str, dict[str, pl.Expr]] = {
    # stats.ncaa.org -> cfbfastR (``sportsdataverse.cfb.to_cfbfastr``).
    "ncaa": {
        "id": pl.col("id_play"),
        "text": pl.col("play_text"),
        # the mapper's ``play_type`` IS the cfbfastR/ESPN label vocabulary
        # ("Pass Reception", "Sack", "Field Goal Good", ...), which is what the
        # play-type rules match against.
        "type.text": pl.col("play_type"),
        # ``orig_play_type`` on this path is the raw stats.ncaa.org structural
        # type ("field_goal", "rush"), NOT ESPN's pre-processor type, so the
        # rules that read it are pointed at the cfbfastR label instead.
        "orig_play_type": pl.col("play_type"),
        "period.number": pl.col("period"),
        # "M:SS" -- the shape ``clock.monotone_within_period`` parses
        "clock.displayValue": pl.format(
            "{}:{}", pl.col("clock.minutes"), pl.col("clock.seconds").cast(pl.Utf8).str.pad_start(2, "0")
        ),
        "start.down": pl.col("down"),
        "start.distance": pl.col("distance"),
        "start.yardsToEndzone": pl.col("yards_to_goal"),
        "end.yardsToEndzone": pl.col("yards_to_goal_end"),
        "drive.id": pl.col("drive_id"),
        "scoringPlay": pl.col("scoring_play"),
        "fg_attempt": pl.col("fg_inds"),
        "kickoff_return_player_name": pl.col("kickoff_returner_player_name"),
        "punt_return_player_name": pl.col("punt_returner_player_name"),
        # cfbfastR carries ONE possession per row; the ESPN frame carries a
        # start and an end. The end alias is the same value, which is why
        # ``poss.end_team_flips_without_change`` is not applicable below.
        "start.pos_team.id": pl.col("pos_team"),
        "end.pos_team.id": pl.col("pos_team"),
        # cfbfastR's scores are the offense/defense pair AFTER the play; the
        # rules read home/away before and after. ``pos_team`` resolves to
        # ``home`` or ``away`` on 7,501/7,501 rows of the 40-game ay2025 probe.
        # The *start* score is the previous row's end score (0-0 before the
        # first row) -- the assumption that no points are scored between two
        # consecutive rows is exactly what ``score.change_on_non_scoring_play``
        # and ``score.delta_value`` are there to test.
        "end.homeScore": pl.when(pl.col("pos_team") == pl.col("home"))
        .then(pl.col("pos_team_score"))
        .otherwise(pl.col("def_pos_team_score")),
        "end.awayScore": pl.when(pl.col("pos_team") == pl.col("away"))
        .then(pl.col("pos_team_score"))
        .otherwise(pl.col("def_pos_team_score")),
    },
}
# the start scores are the end scores shifted, so they are built from the end
# aliases rather than restating the home/away resolution twice
SOURCE_COLUMNS["ncaa"]["start.homeScore"] = SOURCE_COLUMNS["ncaa"]["end.homeScore"].shift(1).fill_null(0)
SOURCE_COLUMNS["ncaa"]["start.awayScore"] = SOURCE_COLUMNS["ncaa"]["end.awayScore"].shift(1).fill_null(0)


#: ``{source: {rule_id: why}}`` -- rules whose quantity does not exist for a
#: source. They are skipped *with a reason*: the report counts them in
#: ``n_not_applicable`` and lists them in ``not_applicable``, which is what
#: separates "we looked and there is nothing to look at" from a silent skip.
#:
#: The companion to :data:`RULE_SCOPE`, kept as its own table rather than folded
#: into it: most of these rules are not scoped at all today, and a placeholder
#: :class:`Rule` for them would override the severity the rule table itself
#: assigns (the ``box.*`` family is WARN there and would have become ERROR).
NOT_APPLICABLE: dict[str, dict[str, str]] = {
    "ncaa": {
        # --- no timeout state on the source at all -------------------------
        # stats.ncaa.org pbp pages print a "Timeout TEAM" row but never a
        # timeouts-remaining count, and ``to_cfbfastr`` emits no timeout
        # column. Deriving counters (3 per half, decrement per timeout row)
        # would make all seven rules pass by construction.
        "timeouts.range": "stats.ncaa.org carries no timeouts-remaining counters",
        "timeouts.range_overtime": "stats.ncaa.org carries no timeouts-remaining counters",
        "timeouts.increase_within_half": "stats.ncaa.org carries no timeouts-remaining counters",
        "timeouts.second_half_reset": "stats.ncaa.org carries no timeouts-remaining counters",
        "timeouts.timeout_row_not_charged": "stats.ncaa.org carries no timeouts-remaining counters",
        "timeouts.timeout_row_charged_both": "stats.ncaa.org carries no timeouts-remaining counters",
        "timeouts.charged_on_non_timeout_row": "stats.ncaa.org carries no timeouts-remaining counters",
        # --- no EP / WP model on this path ---------------------------------
        # ``to_cfbfastr`` maps raw text to cfbfastR names and runs no model
        # ("Model outputs (EPA/WP/...) are out of scope by design"), so there
        # is no EP_start / EP_end / EPA / wp_* column to judge.
        "ep.start_range": "no EP model runs on the stats.ncaa.org mapper path",
        "ep.end_range_non_scoring": "no EP model runs on the stats.ncaa.org mapper path",
        "ep.epa_identity": "no EP model runs on the stats.ncaa.org mapper path",
        "ep.offense_td_end_not_realized": "no EP model runs on the stats.ncaa.org mapper path",
        "ep.offense_td_pat_in_text_unresolved": "no EP model runs on the stats.ncaa.org mapper path",
        "ep.offense_td_failed_try_scored_as_made": "no EP model runs on the stats.ncaa.org mapper path",
        "wp.wp_before_range": "no WP model runs on the stats.ncaa.org mapper path",
        "wp.wp_after_range": "no WP model runs on the stats.ncaa.org mapper path",
        "wp.home_wp_continuity": "no WP model runs on the stats.ncaa.org mapper path",
        "wp.home_wp_after_complemented": "no WP model runs on the stats.ncaa.org mapper path",
        "wp.final_home_wp_matches_result": "no WP model runs on the stats.ncaa.org mapper path",
        "wp.wpa_sums_to_result": "no WP model runs on the stats.ncaa.org mapper path",
        "epa.team_sum_matches_box": "no EP model runs on the stats.ncaa.org mapper path",
        # --- needs the ESPN summary / box, which this path never has --------
        "score.final_matches_header": "no ESPN summary header; the NCAA producer checks the final "
        "against the official linescore in its own qa_pbp_vs_linescore artefact",
        "plays.unexplained_drop": "no ESPN summary to diff the processed rows against",
        "plays.rows_not_in_raw": "no ESPN summary to diff the processed rows against",
        "drive.count_matches_feed": "no ESPN summary drive groups to count against",
        "box.plays_pass_yards_vs_espn": "no ESPN summary boxscore to reconcile against",
        "box.plays_completions_vs_espn": "no ESPN summary boxscore to reconcile against",
        "box.plays_pass_attempts_vs_espn": "no ESPN summary boxscore to reconcile against",
        "box.plays_rush_attempts_vs_espn": "no ESPN summary boxscore to reconcile against",
        "box.adv_pass_yards_vs_espn": "no ESPN summary boxscore to reconcile against",
        "box.adv_pass_yards_vs_live_plays": "no advBoxScore is built on the mapper path",
        "box.adv_rush_yards_vs_live_plays": "no advBoxScore is built on the mapper path",
        "box.plays_rush_yards_vs_espn": "NFL-only box comparison (NCAA charges sacks to rushing)",
        "box.plays_sacks_vs_espn": "NFL-only box comparison (NCAA charges sacks to rushing)",
        "box.adv_rush_yards_vs_espn": "NFL-only box comparison (NCAA charges sacks to rushing)",
        "box.team_totals_match_plays": "no advBoxScore is built on the mapper path",
        "box.player_sums_match_team": "no advBoxScore is built on the mapper path",
        "box.rates_recompute_from_counts": "no advBoxScore is built on the mapper path",
        "box.sections_mirror_off_def": "no advBoxScore is built on the mapper path",
        "box.turnovers_match_flags": "no advBoxScore is built on the mapper path",
        "box.drives_match_drive_rows": "no advBoxScore is built on the mapper path",
        "box.usage_shares_sum_to_one": "no advBoxScore is built on the mapper path",
        "box.team_ids_in_game": "no advBoxScore is built on the mapper path",
        # --- ESPN-only fields the source has no counterpart for -------------
        "type.null_id": "stats.ncaa.org has no ESPN play-type ids",
        "type.null_abbreviation": "stats.ncaa.org has no ESPN play-type abbreviations",
        "ytg.td_start_eq_stat_yardage": "statYardage is an ESPN field with no NCAA counterpart",
        "ytg.start_matches_down_distance_text": "stats.ncaa.org prints no down-and-distance text to "
        "cross-check the spot against",
        "down.goal_to_go_distance_eq_ytg": "no down-and-distance text; the mapper's Goal_To_Go is itself "
        "derived from distance vs yards_to_goal, so a rule fed from it could not fail",
        # --- the alias exists but would make the rule vacuous ---------------
        "poss.end_team_flips_without_change": "cfbfastR carries one possession per row, so end.pos_team "
        "is the start alias and the rule could never fire",
        "poss.offense_matches_drive_team": "the mapper has no drive-team label independent of pos_team "
        "(it derives pos_team from the drive title), so the rule would be tautological",
        "ytg.continuity_next_snap": "yards_to_goal_end is already expressed in the post-play offense's "
        "frame and, where the end spot is unknown at a change of possession, back-filled from the next "
        "snap's yards_to_goal -- the cross-possession arm would be double-flipped or tautological",
        "flags.rush_pass_not_scrimmage": "the mapper emits no scrimmage_play column; deriving it from "
        "rush/pass is the rule's own left-hand side",
        "flags.fg_relabeled_extra_point": "the mapper keeps no pre-relabel play type, so orig and type "
        "are the same column and the rule could never fire",
        "attr.sacker": "the mapper emits tacklers, not a sack_player_name",
        "attr.kickoff_returner": "the mapper emits no kickoff touchback / out-of-bounds / onside "
        "sub-flags, so the rule cannot scope out the kicks with no returner",
        "attr.punt_returner": "the mapper emits no punt touchback / out-of-bounds / downed sub-flags, "
        "so the rule cannot scope out the punts with no returner",
    },
}


def _as_source(frame: pl.DataFrame, source: str) -> pl.DataFrame:
    """A view of ``frame`` carrying the ESPN-shaped aliases :data:`SOURCE_COLUMNS` defines.

    An alias whose inputs the frame lacks is dropped rather than raising. An
    alias always wins over a same-named column already on the frame: the map
    describes *this* source's frame, and ``ncaa``'s ``orig_play_type`` is the
    raw stats.ncaa.org structural type rather than the ESPN quantity of that
    name.
    """
    exprs = SOURCE_COLUMNS.get(source)
    if not exprs:
        return frame
    add = []
    for name, expr in exprs.items():
        try:
            frame.lazy().select(expr.alias(name)).collect_schema()
        except Exception:  # noqa: BLE001 -- a source frame missing the inputs just skips the alias
            continue
        add.append(expr.alias(name))
    return frame.with_columns(add) if add else frame


@dataclass(frozen=True)
class Finding:
    """One rule that fired on one game.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | rule_id | str | the rule's stable ``<group>.<name>`` identifier |
        | severity | str | ``"error"``, ``"warn"`` or ``"info"`` after era / league / source scoping |
        | n_rows | int | rows (or teams / games) that broke the rule |
        | n_checked | int | rows the rule applied to -- the denominator behind ``n_rows`` |
        | sample_row_ids | list | up to five offending play ids (or the sample's own keys) |
        | message | str | the definition the rows break |
    """

    rule_id: str
    severity: str
    n_rows: int
    n_checked: int
    sample_row_ids: list[Any] = field(default_factory=list)
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity,
            "n_rows": self.n_rows,
            "n_checked": self.n_checked,
            "sample_row_ids": list(self.sample_row_ids),
            "message": self.message,
        }


@dataclass(frozen=True)
class GameReport:
    """The per-game verdict :func:`validate_game` returns.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | game_id | int or None | the game's ESPN id |
        | league | str | ``"nfl"`` or ``"cfb"`` |
        | source | str | the source the game was processed from (``"espn"`` by default) |
        | season | int or None | the season the era scoping was resolved against |
        | processing_version | str | the installed ``sportsdataverse`` version that produced the frame |
        | n_rows | int | rows in the validated frame |
        | ok | bool | True when no rule fired at ``error`` severity |
        | errors | list[Finding] | findings at ``error`` severity |
        | warnings | list[Finding] | findings at ``warn`` severity (``info`` findings are counted, not listed) |
        | counts_by_rule | dict[str, int] | violation count per fired rule, whatever its severity |
        | not_applicable | list[str] | rules the source cannot support, skipped with a reason in :data:`NOT_APPLICABLE` -- a declared skip, not a silent one |
    """

    game_id: int | None
    league: str
    source: str
    season: int | None
    processing_version: str
    n_rows: int
    ok: bool
    errors: list[Finding] = field(default_factory=list)
    warnings: list[Finding] = field(default_factory=list)
    counts_by_rule: dict[str, int] = field(default_factory=dict)
    not_applicable: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """A stable, JSON-serialisable dict (key order is part of the contract)."""
        return {
            "game_id": self.game_id,
            "league": self.league,
            "source": self.source,
            "season": self.season,
            "processing_version": self.processing_version,
            "n_rows": self.n_rows,
            "ok": self.ok,
            "n_errors": len(self.errors),
            "n_warnings": len(self.warnings),
            "n_not_applicable": len(self.not_applicable),
            "errors": [f.to_dict() for f in self.errors],
            "warnings": [f.to_dict() for f in self.warnings],
            "counts_by_rule": dict(sorted(self.counts_by_rule.items())),
            "not_applicable": list(self.not_applicable),
        }

    def to_row(self) -> dict[str, Any]:
        """One flat row for a ``<family>_qa_{season}`` asset (no nested values)."""
        return {
            "game_id": self.game_id,
            "league": self.league,
            "source": self.source,
            "season": self.season,
            "processing_version": self.processing_version,
            "n_rows": self.n_rows,
            "ok": self.ok,
            "n_errors": len(self.errors),
            "n_warnings": len(self.warnings),
            "n_not_applicable": len(self.not_applicable),
            "failed_rule_ids": sorted(f.rule_id for f in self.errors),
            "warned_rule_ids": sorted(f.rule_id for f in self.warnings),
        }


def _version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("sportsdataverse")
    except PackageNotFoundError:  # pragma: no cover -- source checkout without an install
        return "unknown"


def _scalar(frame: pl.DataFrame, column: str) -> Any:
    if column not in frame.columns or frame.height == 0:
        return None
    series = frame.get_column(column).drop_nulls()
    return series[0] if series.len() else None


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _game_facts(frame: pl.DataFrame, header: dict | None) -> tuple[int | None, int | None]:
    """``(game_id, season)`` from the frame, falling back to an ESPN-shaped header."""
    head = header or {}
    game_id = _int(_scalar(frame, "game_id")) or _int(head.get("id"))
    season = _int(_scalar(frame, "season")) or _int((head.get("season") or {}).get("year"))
    return game_id, season


def _sample_ids(result: RuleResult) -> list[Any]:
    """The play ids behind a fired rule, or the whole sample row when it carries no id."""
    out = []
    for row in result.samples:
        out.append(row.get("id", row) if isinstance(row, dict) else row)
    return out


def validate_game(
    frame: pl.DataFrame,
    league: str,
    *,
    header: dict | None = None,
    source: str = "espn",
    summary: dict | None = None,
    box: dict | None = None,
) -> GameReport:
    """Validate one processed game against the packaged invariant rules.

    Pure and offline: nothing is fetched, nothing is written, the frame is not
    mutated. A rule whose columns the frame lacks is skipped rather than failed,
    so a slim frame validates the rules it can support.

    For a source whose producer names the same quantities differently,
    :data:`SOURCE_COLUMNS` supplies the ESPN-shaped aliases on a view of the
    frame, and :data:`NOT_APPLICABLE` names the rules that source cannot
    support at all -- those are reported in ``not_applicable`` rather than
    skipped silently.

    Args:
        frame: one game's processed plays, in processor row order -- the
            ``plays_frame`` attribute of ``NFLPlayProcess`` / ``CFBPlayProcess``.
        league: ``"nfl"`` or ``"cfb"``.
        header: the game's ESPN-shaped ``header`` dict. Only used to resolve
            ``game_id`` / ``season`` when the frame carries neither.
        source: the source the game came from (``"espn"``, ``"shield"``, ``"cbs"``,
            ``"yahoo"``, ``"fox"``, ``"ncaa"``). Rules that only judge ESPN's own
            feed are skipped for an adapted source.
        summary: the full ESPN-shaped summary, when available. Enables the
            header-score, final-WP, dropped-play, drive-count and ESPN box rules.
        box: the processor's ``advBoxScore`` dict. Enables the team box and team
            EPA aggregations.

    Returns:
        GameReport: ``ok`` is True when no rule fired at ``error`` severity.

    Raises:
        ValueError: If ``league`` is neither ``"nfl"`` nor ``"cfb"``.

    Example:
        Validate a processed NFL game::

            from sportsdataverse.nfl import NFLPlayProcess
            from sportsdataverse.validation import validate_game

            proc = NFLPlayProcess(gameId=401671801)
            proc.espn_nfl_pbp()
            game = proc.run_processing_pipeline()
            report = validate_game(proc.plays_frame, "nfl", summary=proc.json, box=game.get("advBoxScore"))
            report.ok, sorted(report.counts_by_rule)

    See Also:
        * :meth:`sportsdataverse.nfl.NFLPlayProcess.run_processing_pipeline` --
          pass ``validate=True`` to attach this report to the returned game.
    """
    league = str(league).lower()
    if league not in ("nfl", "cfb"):
        raise ValueError(f"league must be 'nfl' or 'cfb'; got {league!r}")
    game_id, season = _game_facts(frame, header)
    errors: list[Finding] = []
    warnings: list[Finding] = []
    counts: dict[str, int] = {}
    unsupported = NOT_APPLICABLE.get(source, {})
    not_applicable = sorted(unsupported)
    for result in evaluate(_as_source(frame, source), summary=summary, box=box, league=league):
        if not result.n_violations:
            continue
        if result.rule in unsupported:
            continue
        rule = RULE_SCOPE.get(result.rule) or Rule(result.rule, result.severity)
        if not rule.applies(league, source):
            continue
        counts[result.rule] = result.n_violations
        severity = rule.severity_for(league, season)
        if severity is Severity.INFO:
            continue
        finding = Finding(
            result.rule,
            severity.value,
            result.n_violations,
            result.n_checked,
            _sample_ids(result),
            result.description,
        )
        (errors if severity is Severity.ERROR else warnings).append(finding)
    return GameReport(
        game_id=game_id,
        league=league,
        source=source,
        season=season,
        processing_version=_version(),
        n_rows=frame.height,
        ok=not errors,
        errors=errors,
        warnings=warnings,
        counts_by_rule=counts,
        not_applicable=not_applicable,
    )
