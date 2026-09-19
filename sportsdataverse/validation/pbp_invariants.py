"""Processor invariants over ONE game's processed ESPN play-by-play frame.

The definitional rules in :mod:`tools.validation.checks.definitional` judge a
published dataset row by row. These judge the *processor output for a single
game* -- the ``plays_frame`` that ``NFLPlayProcess`` / ``CFBPlayProcess`` build
from an ESPN summary -- against football facts that must hold whatever the
text grammar of the era looked like: timeouts stay in [0, 3] and reset at the
half, field position is continuous within a drive, scores never decrease and
end at the header's final, possession flips on kicks, a touchdown ends at the
realized point value, flags agree with the play type, no raw play disappears
unexplained, the box score adds up to ESPN's, and every pass has a passer.

Both processors share the column vocabulary (``start.yardsToEndzone``,
``end.homeTeamTimeouts``, ``EP_end``, ``passer_player_name`` ...), so one rule
table serves both leagues. A rule whose columns are absent is skipped, not
failed.

:func:`evaluate` returns one :class:`RuleResult` per applicable rule, carrying
its denominator (``n_checked``) as well as its violation count, so a sweep can
report *rates* by league and era. :func:`run` is the harness-shaped wrapper
that turns fired rules into :class:`~sportsdataverse.validation.findings.Finding` records.

The invariants are grouped by number (``invariant`` on each result):

1. timeouts  2. field position  3. score  4. possession  5. down/distance
6. EP/WP     7. play-type flags 8. dropped/duplicated plays  9. box score
10. player attribution coverage  11. play order / type identity
12. game-level aggregations
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import polars as pl

from sportsdataverse.validation.box_reconcile import evaluate_box
from sportsdataverse.validation.findings import CheckContext, Finding, Severity

_SAMPLE_N = 5
_TEXT_CHARS = 140
_EPS = 1e-6
#: EP_end arrives as Float32 from some processors (6.92 -> 6.920000076...).
_FLOAT32_EPS = 1e-4

c = pl.col

#: Raw ESPN rows the processors drop on purpose (``__add_downs_data``).
DOCUMENTED_DROP_RE = r"(?i)end of|coin toss|end period|wins toss"

#: A replay-review clause quotes the ruling it is challenging ("... is challenging the
#: ruling on the field - \"Incomplete pass\". PLAY STANDS."), so the text after it
#: describes the *challenge*, not the play, and a rule that reads the text for the
#: play's own outcome strips it first.
REVIEW_TAIL_RE = r"(?i)(is challenging|under review|the previous play is under|\(original play:).*$"

#: ESPN play types that are a try (PAT / two-point / defensive conversion), not a scrimmage down.
TRY_RE = r"(?i)two.?point|extra point|conversion|\bPAT\b"

#: ESPN play types that ARE a pass / a rush by definition.
PASS_TYPES = (
    "Pass Reception",
    "Pass Completion",
    "Passing Touchdown",
    "Pass Incompletion",
    "Pass Interception Return",
    "Interception Return",
    "Interception Return Touchdown",
    "Sack",
)
RUSH_TYPES = ("Rush", "Rushing Touchdown")

#: Columns every sample row carries for triage (when present).
_BASE_SAMPLE_COLS = ("id", "period.number", "clock.displayValue", "type.text", "text")


@dataclass(frozen=True)
class RuleResult:
    """One rule's outcome on one game.

    Attributes:
        rule: Stable ``<group>.<name>`` identifier.
        invariant: Invariant group number (1-12, see module docstring).
        description: The definition a violating row breaks.
        n_checked: Rows (or teams / games) the rule applied to.
        n_violations: How many of those broke it.
        samples: Up to ``_SAMPLE_N`` offending rows with the columns that matter.
        severity: Severity when the rule fires.
    """

    rule: str
    invariant: int
    description: str
    n_checked: int
    n_violations: int
    samples: list[dict[str, Any]] = field(default_factory=list)
    severity: Severity = Severity.ERROR

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "invariant": self.invariant,
            "description": self.description,
            "n_checked": self.n_checked,
            "n_violations": self.n_violations,
            "samples": self.samples,
            "severity": self.severity.value,
        }


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _t(name: str) -> pl.Expr:
    """A boolean flag column read as strictly True (null -> False)."""
    return (c(name) == True).fill_null(False)  # noqa: E712


def _has(frame: pl.DataFrame, *cols: str) -> bool:
    return all(col in frame.columns for col in cols)


def _sample(frame: pl.DataFrame, cols: tuple[str, ...]) -> list[dict[str, Any]]:
    keep = [col for col in dict.fromkeys(_BASE_SAMPLE_COLS + cols) if col in frame.columns]
    rows = frame.head(_SAMPLE_N).select(keep).to_dicts()
    for row in rows:
        if isinstance(row.get("text"), str):
            row["text"] = row["text"][:_TEXT_CHARS]
    return rows


def _row_rule(
    frame: pl.DataFrame,
    rule: str,
    invariant: int,
    description: str,
    scope: pl.Expr,
    violation: pl.Expr,
    cols: tuple[str, ...],
    *,
    severity: Severity = Severity.ERROR,
    sample_cols: tuple[str, ...] = (),
) -> RuleResult | None:
    """Count ``scope`` rows and the ``scope & violation`` subset.

    A scope row whose ``violation`` is undecidable (null inputs) leaves the
    denominator rather than passing silently. Returns None when the expressions
    reference a column the frame lacks.
    ``cols`` (and ``sample_cols``) only choose what a sample row shows.
    """
    try:
        marked = frame.with_columns(
            __scope=(scope & violation.is_not_null()).fill_null(False),
            __viol=(scope & violation).fill_null(False),
        )
    except pl.exceptions.ColumnNotFoundError:
        return None
    n_checked = int(marked.get_column("__scope").sum())
    bad = marked.filter(c("__viol"))
    return RuleResult(
        rule,
        invariant,
        description,
        n_checked,
        bad.height,
        _sample(bad, cols + sample_cols),
        severity,
    )


def _is_try() -> pl.Expr:
    """A point-after / two-point try (including a defensive conversion return)."""
    return c("type.text").str.contains(TRY_RE).fill_null(False)


def _scrimmage() -> pl.Expr:
    """A rush/pass play that is not a try (PAT / two-point attempt)."""
    return (_t("rush") | _t("pass")) & ~_is_try()


# ---------------------------------------------------------------------------
# 1. timeouts
# ---------------------------------------------------------------------------

_TO = ("start.homeTeamTimeouts", "start.awayTeamTimeouts", "end.homeTeamTimeouts", "end.awayTeamTimeouts")


def _is_timeout_row() -> pl.Expr:
    """A team timeout row: ESPN's ``Timeout`` type, or legacy timeout text on an unlabeled row.

    Official / TV / injury timeouts and the two-minute warning charge nobody.
    """
    text = c("text").fill_null("")
    typ = c("type.text").fill_null("")
    official = text.str.contains(r"(?i)official|\btv\b|injury|two.minute") | typ.str.contains(
        r"(?i)official|two.minute"
    )
    legacy = text.str.contains(r"(?i)^\s*timeout\b|\btimeout #\s*\d")
    return ((typ == "Timeout") | (legacy & ~_t("rush") & ~_t("pass"))) & ~official


def _timeouts(df: pl.DataFrame) -> list[RuleResult | None]:
    if not _has(df, *_TO, "period.number"):
        return []
    f = df.with_columns(
        # each overtime period is its own allotment (NFL re-allots for OT; NCAA one per OT period)
        __half=pl.when(c("period.number") <= 2)
        .then(1)
        .when(c("period.number") <= 4)
        .then(2)
        .otherwise(c("period.number")),
        __home_used=c("start.homeTeamTimeouts") - c("end.homeTeamTimeouts"),
        __away_used=c("start.awayTeamTimeouts") - c("end.awayTeamTimeouts"),
    ).with_columns(
        __prev_half=c("__half").shift(1),
        __prev_end_home=c("end.homeTeamTimeouts").shift(1),
        __prev_end_away=c("end.awayTeamTimeouts").shift(1),
        __first_2h=(c("__half") == 2) & (c("__half").shift(1) == 1),
        __is_to=_is_timeout_row(),
    )
    rng = pl.any_horizontal([~c(col).is_between(0, 3) for col in _TO])
    same_half = c("__half") == c("__prev_half")
    return [
        _row_rule(
            f,
            "timeouts.range",
            1,
            "timeouts remaining per team must be within [0, 3] in regulation",
            pl.all_horizontal([c(col).is_not_null() for col in _TO]) & (c("period.number") <= 4),
            rng,
            _TO,
        ),
        _row_rule(
            f,
            "timeouts.range_overtime",
            1,
            "timeouts remaining per team must be within [0, 3] in overtime (the allotment resets)",
            pl.all_horizontal([c(col).is_not_null() for col in _TO]) & (c("period.number") >= 5),
            rng,
            _TO,
        ),
        _row_rule(
            f,
            "timeouts.increase_within_half",
            1,
            "a team's timeouts remaining never increase within a half (each overtime period is its own allotment)",
            same_half,
            (c("end.homeTeamTimeouts") > c("__prev_end_home")) | (c("end.awayTeamTimeouts") > c("__prev_end_away")),
            _TO,
        ),
        _row_rule(
            f,
            "timeouts.second_half_reset",
            1,
            "both teams start the second half with 3 timeouts",
            c("__first_2h"),
            (c("start.homeTeamTimeouts") != 3) | (c("start.awayTeamTimeouts") != 3),
            _TO,
        ),
        _row_rule(
            f,
            "timeouts.timeout_row_not_charged",
            1,
            "a team timeout row decrements exactly one team (none charged)",
            c("__is_to"),
            (c("__home_used") + c("__away_used")) == 0,
            _TO,
            sample_cols=("homeTeamAbbrev", "awayTeamAbbrev", "homeTeamMascot", "awayTeamMascot"),
        ),
        _row_rule(
            f,
            "timeouts.timeout_row_charged_both",
            1,
            "a team timeout row decrements exactly one team (both charged)",
            c("__is_to"),
            (c("__home_used") > 0) & (c("__away_used") > 0),
            _TO,
            sample_cols=("homeTeamAbbrev", "awayTeamAbbrev", "homeTeamMascot", "awayTeamMascot"),
        ),
        _row_rule(
            f,
            "timeouts.charged_on_non_timeout_row",
            1,
            "only a timeout row may decrement a team's timeouts (second-half opener excluded)",
            ~c("__is_to") & ~c("__first_2h").fill_null(False),
            (c("__home_used") != 0) | (c("__away_used") != 0),
            _TO,
        ),
    ]


# ---------------------------------------------------------------------------
# 2. field position
# ---------------------------------------------------------------------------


def _field_position(df: pl.DataFrame) -> list[RuleResult | None]:
    out: list[RuleResult | None] = [
        _row_rule(
            df,
            "ytg.start_range",
            2,
            "start yards-to-endzone must be within [0, 100]",
            c("start.yardsToEndzone").is_not_null(),
            ~c("start.yardsToEndzone").is_between(0, 100),
            ("start.yardsToEndzone",),
        ),
        _row_rule(
            df,
            "ytg.end_range",
            2,
            "end yards-to-endzone must be within [0, 100]",
            c("end.yardsToEndzone").is_not_null(),
            ~c("end.yardsToEndzone").is_between(0, 100),
            ("end.yardsToEndzone",),
        ),
    ]
    if _has(df, "start.downDistanceText", "homeTeamAbbrev", "awayTeamAbbrev", "homeTeamId", "start.pos_team.id"):
        # "3rd & 3 at HOU 32": the spot ESPN prints next to the down. The side is
        # resolved against the offense/defense abbreviation when it matches;
        # otherwise only the two mirror values are admissible.
        spot = c("start.downDistanceText").str.extract_groups(r"at (?:([A-Za-z&\.]{2,6}) )?(\d{1,2})$")
        home = c("start.pos_team.id").cast(pl.Int64) == c("homeTeamId").cast(pl.Int64)
        f = df.with_columns(
            __side=spot.struct.field("1").str.to_uppercase(),
            __yl=spot.struct.field("2").cast(pl.Int64, strict=False),
            __off=pl.when(home).then(c("homeTeamAbbrev")).otherwise(c("awayTeamAbbrev")).str.to_uppercase(),
            __def=pl.when(home).then(c("awayTeamAbbrev")).otherwise(c("homeTeamAbbrev")).str.to_uppercase(),
        ).with_columns(
            __text_ytg=pl.when(c("__yl") == 50)
            .then(50)
            .when(c("__side") == c("__off"))
            .then(100 - c("__yl"))
            .when(c("__side") == c("__def"))
            .then(c("__yl"))
            .otherwise(None),
        )
        out.append(
            _row_rule(
                f,
                "ytg.start_matches_down_distance_text",
                2,
                "start yards-to-endzone agrees with the spot in ESPN's own downDistanceText",
                # "at GASO 0" is ESPN's placeholder on some penalty rows, not a spot
                _scrimmage() & c("__yl").is_between(1, 50) & c("start.yardsToEndzone").is_not_null(),
                pl.when(c("__text_ytg").is_not_null())
                .then(c("start.yardsToEndzone") != c("__text_ytg"))
                .otherwise((c("start.yardsToEndzone") != c("__yl")) & (c("start.yardsToEndzone") != 100 - c("__yl"))),
                ("start.downDistanceText", "start.yardsToEndzone", "__text_ytg", "__side", "__off"),
            )
        )
    if _has(df, "rush", "pass", "type.text", "start.yardsToEndzone"):
        out.append(
            _row_rule(
                df,
                "ytg.scrimmage_start_1_99",
                2,
                "a scrimmage play starts 1-99 yards from the endzone",
                _scrimmage(),
                ~c("start.yardsToEndzone").is_between(1, 99),
                ("rush", "pass", "type.text", "start.yardsToEndzone"),
            )
        )
    cols = ("rush", "pass", "type.text", "drive.id", "period.number", "start.pos_team.id", "end.pos_team.id")
    if _has(df, *cols, "start.yardsToEndzone", "end.yardsToEndzone"):
        f = df.with_columns(__scrim=_scrimmage()).with_columns(
            __prev_scrim=c("__scrim").shift(1),
            __prev_end_ytg=c("end.yardsToEndzone").shift(1),
            __prev_drive=c("drive.id").shift(1),
            __prev_period=c("period.number").shift(1),
            __prev_end_pos=c("end.pos_team.id").shift(1),
            __prev_start_pos=c("start.pos_team.id").shift(1),
            __prev_score=(_t("scoringPlay") | _t("td_play")).shift(1)
            if _has(df, "scoringPlay", "td_play")
            else pl.lit(False),
            __prev_id=c("id").shift(1),
        )
        scope = (
            c("__scrim")
            & c("__prev_scrim").fill_null(False)
            & (c("drive.id") == c("__prev_drive"))
            & (c("period.number") == c("__prev_period"))
            & (c("__prev_start_pos") == c("start.pos_team.id"))
            & (c("__prev_end_pos") == c("start.pos_team.id"))
            & ~c("__prev_score").fill_null(False)
        )
        out.append(
            _row_rule(
                f,
                "ytg.continuity",
                2,
                "a scrimmage play starts where the previous same-drive scrimmage play ended",
                scope,
                c("__prev_end_ytg") != c("start.yardsToEndzone"),
                ("__prev_id", "__prev_end_ytg", "start.yardsToEndzone"),
                severity=Severity.WARN,
            )
        )
    td_scope_cols = ("rush_td", "pass_td", "penalty_flag", "fumble_vec", "start.yardsToEndzone")
    if _has(df, *td_scope_cols, "yds_rushed", "yds_receiving"):
        f = df.with_columns(
            __gained=pl.when(_t("rush_td")).then(c("yds_rushed")).otherwise(c("yds_receiving")),
        )
        scope = (_t("rush_td") ^ _t("pass_td")) & ~_t("penalty_flag") & ~_t("fumble_vec") & _scrimmage() & _offense_td()
        out.append(
            _row_rule(
                f,
                "ytg.td_start_eq_yards_gained",
                2,
                "a rushing/receiving TD gains exactly the start yards-to-endzone",
                scope,
                c("__gained") != c("start.yardsToEndzone"),
                td_scope_cols + ("__gained", "statYardage", "type.text", "rush", "pass"),
            )
        )
        if "statYardage" in df.columns:
            out.append(
                _row_rule(
                    f,
                    "ytg.td_start_eq_stat_yardage",
                    2,
                    "ESPN statYardage on a rushing/receiving TD equals the start yards-to-endzone",
                    scope,
                    c("statYardage") != c("start.yardsToEndzone"),
                    td_scope_cols + ("statYardage", "type.text", "rush", "pass"),
                    severity=Severity.WARN,
                )
            )
    return out


# ---------------------------------------------------------------------------
# 3. score
# ---------------------------------------------------------------------------

_SCORE = ("start.homeScore", "start.awayScore", "end.homeScore", "end.awayScore")


def header_final_score(summary: dict[str, Any] | None) -> tuple[int | None, int | None]:
    """``(home, away)`` final score from an ESPN summary header, ``(None, None)`` when absent."""
    home = away = None
    try:
        comps = summary["header"]["competitions"][0]["competitors"]  # type: ignore[index]
    except (KeyError, IndexError, TypeError):
        return None, None
    for comp in comps or []:
        try:
            score = int(float(comp.get("score")))
        except (TypeError, ValueError):
            continue
        if comp.get("homeAway") == "home":
            home = score
        elif comp.get("homeAway") == "away":
            away = score
    return home, away


def _score(df: pl.DataFrame, summary: dict[str, Any] | None) -> list[RuleResult | None]:
    if not _has(df, *_SCORE):
        return []
    f = df.with_columns(
        __dh=c("end.homeScore") - c("start.homeScore"),
        __da=c("end.awayScore") - c("start.awayScore"),
        __prev_end_home=c("end.homeScore").shift(1),
        __prev_end_away=c("end.awayScore").shift(1),
        __row=pl.int_range(pl.len()),
    ).with_columns(__delta=c("__dh") + c("__da"))
    not_first = c("__row") > 0
    scoring = _t("scoringPlay") if "scoringPlay" in df.columns else pl.lit(False)
    if "scoring_play" in df.columns:
        scoring = scoring | _t("scoring_play")
    out: list[RuleResult | None] = [
        _row_rule(
            f,
            "score.monotone",
            3,
            "a team's score never decreases from one play to the next",
            not_first,
            (c("end.homeScore") < c("__prev_end_home")) | (c("end.awayScore") < c("__prev_end_away")),
            _SCORE + ("__prev_end_home", "__prev_end_away", "homeScore", "awayScore", "scoringPlay"),
        ),
        _row_rule(
            f,
            "score.change_on_non_scoring_play",
            3,
            "the score changes only on a scoring play",
            ~scoring,
            c("__delta") != 0,
            _SCORE + ("scoringPlay", "homeScore", "awayScore"),
            severity=Severity.WARN,
        ),
        _row_rule(
            f,
            "score.scoring_play_without_change",
            3,
            "an ESPN scoring play changes the score",
            _t("scoringPlay") if "scoringPlay" in df.columns else pl.lit(False),
            c("__delta") == 0,
            _SCORE + ("scoringPlay", "homeScore", "awayScore"),
            severity=Severity.WARN,
        ),
        _row_rule(
            f,
            "score.delta_value",
            3,
            "a single play adds 1, 2, 3, 6, 7 or 8 points to one team",
            c("__delta") != 0,
            ~c("__dh").is_in([0, 1, 2, 3, 6, 7, 8])
            | ~c("__da").is_in([0, 1, 2, 3, 6, 7, 8])
            | ((c("__dh") != 0) & (c("__da") != 0)),
            _SCORE + ("__dh", "__da", "scoringPlay"),
        ),
    ]
    home, away = header_final_score(summary)
    if home is not None and away is not None and df.height:
        last = f.tail(1)
        bad = last.filter(((c("end.homeScore") != home) | (c("end.awayScore") != away)).fill_null(True))
        out.append(
            RuleResult(
                "score.final_matches_header",
                3,
                "the last play's end score equals the header final score",
                1,
                bad.height,
                [{**r, "header_home": home, "header_away": away} for r in _sample(bad, _SCORE)],
            )
        )
    return out


# ---------------------------------------------------------------------------
# 4. possession
# ---------------------------------------------------------------------------


def _possession(df: pl.DataFrame) -> list[RuleResult | None]:
    need = ("rush", "pass", "type.text", "start.pos_team.id")
    if not _has(df, *need):
        return []
    out: list[RuleResult | None] = []
    if _has(df, "drive.team.abbreviation", "homeTeamAbbrev", "awayTeamAbbrev", "homeTeamId", "awayTeamId"):
        f = df.with_columns(
            __drive_team=pl.when(c("drive.team.abbreviation") == c("homeTeamAbbrev"))
            .then(c("homeTeamId"))
            .when(c("drive.team.abbreviation") == c("awayTeamAbbrev"))
            .then(c("awayTeamId"))
            .otherwise(None)
            .cast(pl.Int64),
        )
        out.append(
            _row_rule(
                f,
                "poss.offense_matches_drive_team",
                4,
                "a scrimmage play's offense is the team ESPN credits with the drive",
                _scrimmage() & c("__drive_team").is_not_null(),
                c("start.pos_team.id").cast(pl.Int64) != c("__drive_team"),
                need + ("drive.id", "drive.team.abbreviation", "__drive_team"),
            )
        )
    if "drive.id" in df.columns:
        s = df.filter(_scrimmage()).with_columns(
            __prev_pos=c("start.pos_team.id").shift(1).over("drive.id"),
            __prev_id=c("id").shift(1).over("drive.id"),
        )
        out.append(
            _row_rule(
                s,
                "poss.offense_constant_within_drive",
                4,
                "consecutive scrimmage plays of one ESPN drive share an offense",
                c("__prev_pos").is_not_null(),
                c("start.pos_team.id") != c("__prev_pos"),
                need + ("drive.id", "__prev_id", "__prev_pos"),
            )
        )
    if _has(df, "end.pos_team.id", "period.number"):
        n = (
            df.with_columns(__scrim=_scrimmage())
            .filter(c("__scrim"))
            .with_columns(
                __next_pos=c("start.pos_team.id").shift(-1),
                __next_period=c("period.number").shift(-1),
                __next_id=c("id").shift(-1),
            )
        )
        kept_ball = (
            (c("__next_pos") == c("start.pos_team.id"))
            & (c("__next_period") == c("period.number"))
            & ~_t("scoringPlay")
            & ~_t("td_play")
        )
        out.append(
            _row_rule(
                n,
                "poss.end_team_flips_without_change",
                4,
                "a scrimmage play whose offense also runs the next scrimmage play keeps end.pos_team == start.pos_team",
                kept_ball,
                c("end.pos_team.id") != c("start.pos_team.id"),
                need + ("end.pos_team.id", "__next_id", "__next_pos", "wpa"),
            )
        )
    if _has(df, "punt", "kickoff_play", "penalty_flag", "fumble_vec", "td_play", "period.number"):
        k = (
            df.with_columns(__scrim=_scrimmage(), __row=pl.int_range(pl.len()))
            .filter(c("__scrim") | _t("punt") | _t("kickoff_play"))
            .with_columns(
                __next_pos=c("start.pos_team.id").shift(-1),
                __next_scrim=c("__scrim").shift(-1),
                __next_period=c("period.number").shift(-1),
                __next_id=c("id").shift(-1),
            )
        )
        clean = (
            ~_t("penalty_flag")
            & ~_t("fumble_vec")
            & ~_t("td_play")
            & c("__next_scrim").fill_null(False)
            & (c("__next_period") == c("period.number"))
            & ~c("text").str.contains(r"(?i)muff|recovered by|fake|onside|on-side|no play").fill_null(False)
        )
        out.append(
            _row_rule(
                k,
                "poss.punt_changes_possession",
                4,
                "the first scrimmage play after a clean punt belongs to the receiving team",
                _t("punt") & ~_t("punt_blocked") & clean if "punt_blocked" in df.columns else _t("punt") & clean,
                c("__next_pos") == c("start.pos_team.id"),
                need + ("__next_id", "__next_pos"),
            )
        )
        out.append(
            _row_rule(
                k,
                "poss.kickoff_receiver_gets_ball",
                4,
                "the first scrimmage play after a clean kickoff belongs to the kickoff row's pos_team (receiver)",
                _t("kickoff_play") & clean,
                c("__next_pos") != c("start.pos_team.id"),
                need + ("__next_id", "__next_pos"),
            )
        )
    return out


# ---------------------------------------------------------------------------
# 5. down / distance
# ---------------------------------------------------------------------------


def _down_distance(df: pl.DataFrame) -> list[RuleResult | None]:
    need = ("rush", "pass", "type.text")
    goal = (
        c("start.downDistanceText").str.contains(r"(?i)goal").fill_null(False)
        if "start.downDistanceText" in df.columns
        else pl.lit(False)
    )
    return [
        _row_rule(
            df,
            "down.scrimmage_down_1_4",
            5,
            "a scrimmage play starts on down 1-4",
            _scrimmage(),
            ~c("start.down").is_between(1, 4) | c("start.down").is_null(),
            need + ("start.down", "start.distance"),
        ),
        _row_rule(
            df,
            "down.scrimmage_distance_ge_1",
            5,
            "a scrimmage play needs at least 1 yard for a first down",
            _scrimmage(),
            (c("start.distance") < 1) | c("start.distance").is_null(),
            need + ("start.down", "start.distance", "start.yardsToEndzone"),
        ),
        _row_rule(
            df,
            "down.distance_le_ytg",
            5,
            "distance to the first-down line cannot exceed distance to the endzone",
            _scrimmage(),
            c("start.distance") > c("start.yardsToEndzone"),
            need + ("start.down", "start.distance", "start.yardsToEndzone", "start.downDistanceText"),
        ),
        _row_rule(
            df,
            "down.goal_to_go_distance_eq_ytg",
            5,
            "on goal-to-go the distance equals the yards to the endzone",
            _scrimmage() & goal,
            c("start.distance") != c("start.yardsToEndzone"),
            need + ("start.down", "start.distance", "start.yardsToEndzone", "start.downDistanceText"),
        ),
    ]


# ---------------------------------------------------------------------------
# 6. EP / WP
# ---------------------------------------------------------------------------


def home_result(summary: dict[str, Any] | None) -> float | None:
    """1.0 home win, 0.0 home loss, 0.5 tie; None when the header has no final score."""
    home, away = header_final_score(summary)
    if home is None or away is None:
        return None
    return 1.0 if home > away else 0.0 if home < away else 0.5


def _offense_td() -> pl.Expr:
    return (_t("rush_td") | _t("pass_td")) & ~c("type.text").str.contains(
        r"(?i)interception|fumble|punt|kickoff|blocked|safety|defensive"
    ).fill_null(False)


#: TD text that carries the try's result (PAT folded into the TD row).
_PAT_IN_TEXT_RE = r"(?i)extra point|kick\)|kick is|\bpat\b|two-point|two point|2-pt|2pt|conversion"
#: ...and says the try failed.
_PAT_FAILED_RE = (
    r"(?i)extra point is no good|extra point.{0,40}blocked|kick is (no good|blocked)|kick (failed|blocked|missed)"
    r"|pat (failed|missed|blocked)|(two-point|two point|2-pt|conversion).{0,60}(fail|no good|incomplete)"
)


def _ep_wp(df: pl.DataFrame, summary: dict[str, Any] | None) -> list[RuleResult | None]:
    out: list[RuleResult | None] = [
        _row_rule(
            df,
            "ep.start_range",
            6,
            "EP_start must be within [-7, 7]",
            c("EP_start").is_not_null(),
            ~c("EP_start").is_between(-7 - _EPS, 7 + _EPS),
            ("EP_start",),
        ),
    ]
    if _has(df, "EP_end", "scoring_play", "td_play", "safety"):
        out.append(
            _row_rule(
                df,
                "ep.end_range_non_scoring",
                6,
                "EP_end of a non-scoring play must be within [-7, 7]",
                c("EP_end").is_not_null() & ~_t("scoring_play") & ~_t("td_play") & ~_t("safety"),
                ~c("EP_end").is_between(-7 - _EPS, 7 + _EPS),
                ("EP_start", "EP_end", "EPA"),
            )
        )
    if _has(df, "rush_td", "pass_td", "type.text", "EP_end"):
        td = _offense_td() & c("EP_end").is_not_null()
        text = c("text").fill_null("")
        out += [
            _row_rule(
                df,
                "ep.offense_td_end_not_realized",
                6,
                "an offensive TD's EP_end is a realized value (6, 7 or 8), not a model estimate",
                td,
                ~pl.any_horizontal([(c("EP_end") - v).abs() < _FLOAT32_EPS for v in (6.0, 6.92, 7.0, 8.0)]),
                ("EP_start", "EP_end", "EPA", "rush_td", "pass_td"),
            ),
            _row_rule(
                df,
                "ep.offense_td_pat_in_text_unresolved",
                6,
                "a TD whose text carries the try result ends at 6/7/8, not the 6.92 unknown-PAT fallback",
                td & text.str.contains(_PAT_IN_TEXT_RE),
                (c("EP_end") - 6.92).abs() < _FLOAT32_EPS,
                ("EP_start", "EP_end", "EPA"),
            ),
            _row_rule(
                df,
                "ep.offense_td_failed_try_scored_as_made",
                6,
                "a TD whose try failed (text) ends at 6, not 6.92/7",
                td & text.str.contains(_PAT_FAILED_RE),
                c("EP_end") > 6 + _FLOAT32_EPS,
                ("EP_start", "EP_end", "EPA"),
            ),
        ]
    if _has(df, "EPA", "EP_end", "EP_start", "penalty_in_text", "end_of_half", "type.text"):
        out.append(
            _row_rule(
                df,
                "ep.epa_identity",
                6,
                "EPA = EP_end - EP_start outside the penalty-in-text / end-of-half / clock-stoppage overlays",
                c("EPA").is_not_null()
                & ~_t("penalty_in_text")
                & ~_t("end_of_half")
                & ~c("type.text").str.contains(r"(?i)timeout|end of|two.minute").fill_null(False),
                (c("EPA") - (c("EP_end") - c("EP_start"))).abs() > 1e-4,
                ("EP_start", "EP_end", "EPA"),
                severity=Severity.WARN,
            )
        )
    for col in ("wp_before", "wp_after"):
        out.append(
            _row_rule(
                df,
                f"wp.{col}_range",
                6,
                f"{col} must be within [0, 1]",
                c(col).is_not_null(),
                ~c(col).is_between(-_EPS, 1 + _EPS),
                (col,),
            )
        )
    if _has(df, "home_wp_before", "home_wp_after"):
        f = df.with_columns(__next_home_wp_before=c("home_wp_before").shift(-1))
        out.append(
            _row_rule(
                f,
                "wp.home_wp_continuity",
                6,
                "a play's home_wp_after equals the next play's home_wp_before (|diff| <= 0.05)",
                c("__next_home_wp_before").is_not_null() & c("home_wp_after").is_not_null(),
                (c("home_wp_after") - c("__next_home_wp_before")).abs() > 0.05,
                ("home_wp_before", "home_wp_after", "__next_home_wp_before"),
                severity=Severity.WARN,
            )
        )
        if _has(df, "start.pos_team.id", "end.pos_team.id"):
            out.append(
                _row_rule(
                    f,
                    "wp.home_wp_after_complemented",
                    6,
                    "home_wp_after is not the complement of the next play's home_wp_before (perspective flip)",
                    c("__next_home_wp_before").is_not_null()
                    & c("home_wp_after").is_not_null()
                    & (c("start.pos_team.id") != c("end.pos_team.id")),
                    ((c("home_wp_after") - (1 - c("__next_home_wp_before"))).abs() < 0.01)
                    & ((c("home_wp_after") - c("__next_home_wp_before")).abs() > 0.05),
                    ("start.pos_team.id", "end.pos_team.id", "home_wp_after", "__next_home_wp_before", "wpa"),
                )
            )
        result = home_result(summary)
        if result is not None and df.height:
            nn = df.filter(c("home_wp_after").is_not_null() & c("home_wp_before").is_not_null())
            if nn.height:
                last_after = float(nn.get_column("home_wp_after")[-1])
                first_before = float(nn.get_column("home_wp_before")[0])
                bad = abs(last_after - result) > 0.1
                out.append(
                    RuleResult(
                        "wp.final_home_wp_matches_result",
                        6,
                        "the last play's home_wp_after is within 0.1 of the game result",
                        1,
                        int(bad),
                        [{"home_wp_after_last": last_after, "home_result": result}] if bad else [],
                    )
                )
                if _has(df, "wpa", "pos_team", "homeTeamId"):
                    home_wpa = float(
                        nn.select(
                            pl.when(c("pos_team") == c("homeTeamId")).then(c("wpa")).otherwise(-c("wpa")).sum()
                        ).item()
                        or 0.0
                    )
                    drift = home_wpa - (result - first_before)
                    out.append(
                        RuleResult(
                            "wp.wpa_sums_to_result",
                            6,
                            "home-signed WPA sums to (result - first home_wp_before) within 0.1",
                            1,
                            int(abs(drift) > 0.1),
                            [
                                {
                                    "home_wpa_sum": home_wpa,
                                    "result_minus_first_wp": result - first_before,
                                    "drift": drift,
                                }
                            ]
                            if abs(drift) > 0.1
                            else [],
                            Severity.WARN,
                        )
                    )
    return out


# ---------------------------------------------------------------------------
# 7. play-type <-> flag consistency
# ---------------------------------------------------------------------------


def _flags(df: pl.DataFrame) -> list[RuleResult | None]:
    typ = c("type.text").fill_null("")
    text = c("text").fill_null("")
    orig = c("orig_play_type").fill_null("") if "orig_play_type" in df.columns else typ
    no_play = _t("penalty_no_play")
    return [
        _row_rule(
            df,
            "flags.rush_and_pass",
            7,
            "a play cannot be both rush and pass",
            pl.lit(True),
            _t("rush") & _t("pass"),
            ("rush", "pass", "type.text"),
        ),
        _row_rule(
            df,
            "flags.pass_type_without_pass",
            7,
            "an ESPN pass/interception/sack play type carries pass=True",
            typ.is_in(PASS_TYPES) & ~no_play,
            ~_t("pass"),
            ("pass", "type.text", "penalty_no_play"),
        ),
        _row_rule(
            df,
            "flags.rush_type_without_rush",
            7,
            "an ESPN rush play type carries rush=True",
            typ.is_in(RUSH_TYPES) & ~no_play,
            ~_t("rush"),
            ("rush", "type.text", "penalty_no_play"),
        ),
        _row_rule(
            df,
            "flags.sack_not_pass",
            7,
            "a sack is a pass dropback (pass=True)",
            _t("sack"),
            ~_t("pass"),
            ("sack", "pass", "type.text"),
        ),
        _row_rule(
            df,
            "flags.sack_type_without_sack",
            7,
            "an ESPN Sack play carries sack=True",
            typ == "Sack",
            ~_t("sack"),
            ("sack", "type.text"),
        ),
        _row_rule(
            df,
            "flags.sack_counted_as_pass_attempt",
            7,
            "a sack is not a pass attempt",
            _t("sack"),
            _t("pass_attempt"),
            ("sack", "pass_attempt", "type.text"),
        ),
        _row_rule(
            df,
            "flags.completion_without_attempt",
            7,
            "a completion is a pass attempt",
            _t("completion"),
            ~_t("pass_attempt"),
            ("completion", "pass_attempt", "type.text"),
        ),
        _row_rule(
            df,
            "flags.completion_on_incomplete_text",
            7,
            "a completion's text does not say the pass fell incomplete",
            _t("completion"),
            text.str.replace(REVIEW_TAIL_RE, "").str.contains(r"(?i)pass incomplete|incomplete pass"),
            ("completion", "type.text"),
        ),
        _row_rule(
            df,
            "flags.int_without_pass",
            7,
            "an interception is a pass play",
            _t("int"),
            ~_t("pass"),
            ("int", "pass", "type.text"),
        ),
        _row_rule(
            df,
            "flags.rush_td_and_pass_td",
            7,
            "a play is at most one of rush_td / pass_td",
            pl.lit(True),
            _t("rush_td") & _t("pass_td"),
            ("rush_td", "pass_td", "type.text"),
        ),
        _row_rule(
            df,
            "flags.offense_td_flag_on_return_td",
            7,
            "an interception/fumble/kick return TD is not a pass_td or rush_td for the offense",
            typ.str.contains(r"(?i)(interception|fumble|punt|kickoff|blocked).*(touchdown|return td)"),
            _t("pass_td") | _t("rush_td"),
            ("pass_td", "rush_td", "type.text"),
        ),
        _row_rule(
            df,
            "flags.no_play_yardage_credited",
            7,
            "a play wiped out by penalty credits no rushing/receiving yards",
            no_play,
            (c("yds_rushed").fill_null(0) != 0) | (c("yds_receiving").fill_null(0) != 0),
            ("penalty_no_play", "yds_rushed", "yds_receiving", "type.text"),
        ),
        _row_rule(
            df,
            "flags.no_play_counted_as_attempt",
            7,
            "a play wiped out by penalty is not a rush/pass attempt",
            no_play,
            _t("pass_attempt") | _t("rush"),
            ("penalty_no_play", "pass_attempt", "rush", "type.text"),
            severity=Severity.WARN,
        ),
        _row_rule(
            df,
            "flags.fg_type_without_fg_attempt",
            7,
            "an ESPN field-goal play carries fg_attempt=True",
            orig.str.contains(r"(?i)field goal") & ~no_play,
            ~_t("fg_attempt"),
            ("fg_attempt", "type.text", "orig_play_type"),
        ),
        _row_rule(
            df,
            "flags.fg_relabeled_extra_point",
            7,
            "an ESPN field-goal play is not relabeled as an extra point",
            orig.str.contains(r"(?i)field goal"),
            typ.str.contains(r"(?i)extra point"),
            ("fg_attempt", "type.text", "orig_play_type"),
        ),
        _row_rule(
            df,
            "flags.punt_type_without_punt",
            7,
            "an ESPN punt play carries punt=True",
            typ.str.contains(r"(?i)punt") & ~no_play,
            ~_t("punt"),
            ("punt", "type.text"),
        ),
        _row_rule(
            df,
            "flags.kickoff_type_without_kickoff",
            7,
            "an ESPN kickoff play carries kickoff_play=True",
            typ.str.contains(r"(?i)kickoff") & ~no_play,
            ~_t("kickoff_play"),
            ("kickoff_play", "type.text"),
        ),
        _row_rule(
            df,
            "flags.rush_pass_not_scrimmage",
            7,
            "a rush/pass play (not a try) is a scrimmage_play",
            _scrimmage(),
            ~_t("scrimmage_play"),
            ("rush", "pass", "scrimmage_play", "type.text"),
        ),
    ]


# ---------------------------------------------------------------------------
# 8. dropped / duplicated plays
# ---------------------------------------------------------------------------


def raw_plays(summary: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Every play in the summary's drive groups, as the processors read them (all ``drives`` keys)."""
    drives = (summary or {}).get("drives") or {}
    groups: list[Any] = []
    for value in drives.values() if isinstance(drives, dict) else [drives]:
        groups += value if isinstance(value, list) else [value]
    plays = []
    for drive in groups:
        if isinstance(drive, dict):
            plays += [p for p in drive.get("plays") or [] if isinstance(p, dict)]
    return plays


def _plays(df: pl.DataFrame, summary: dict[str, Any] | None) -> list[RuleResult | None]:
    if "id" not in df.columns:
        return []
    ids = df.get_column("id").cast(pl.Int64)
    dup = df.filter(ids.is_duplicated())
    out: list[RuleResult | None] = [
        RuleResult(
            "plays.duplicate_ids",
            8,
            "processed play ids are unique",
            df.height,
            dup.height,
            _sample(dup, ()),
        )
    ]
    raw = raw_plays(summary)
    if not raw:
        return out
    processed = set(ids.drop_nulls().to_list())
    raw_ids: list[int] = []
    unexplained: list[dict[str, Any]] = []
    documented = 0
    seen_text: dict[str, int] = {}
    for p in raw:
        try:
            pid = int(p.get("id"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        raw_ids.append(pid)
        text = str(p.get("text") or "")
        typ = str((p.get("type") or {}).get("text") or "")
        prior = seen_text.get(text)
        seen_text[text] = pid
        if pid in processed:
            continue
        if pl.Series([typ]).str.contains(DOCUMENTED_DROP_RE).item():
            documented += 1
            continue
        if text and prior is not None:  # the processor's own text-dupe filter
            documented += 1
            continue
        unexplained.append({"id": pid, "type.text": typ, "text": text[:_TEXT_CHARS]})
    extra = processed - set(raw_ids)
    out += [
        RuleResult(
            "plays.unexplained_drop",
            8,
            "every raw play survives processing unless it is a documented drop (end of period, coin toss, text dupe)",
            len(raw_ids),
            len(unexplained),
            unexplained[:_SAMPLE_N],
        ),
        RuleResult(
            "plays.rows_not_in_raw",
            8,
            "every processed play id exists in the raw summary",
            df.height,
            len(extra),
            [{"id": i} for i in sorted(extra)[:_SAMPLE_N]],
        ),
    ]
    return out


# ---------------------------------------------------------------------------
# 9. box score
# ---------------------------------------------------------------------------


def _espn_int(value: Any) -> int | None:
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None


def espn_team_stats(summary: dict[str, Any] | None) -> dict[int, dict[str, int | None]]:
    """``{team_id: {pass_yds_net, sack_yds, rush_yds, rush_att, comp, att, sacks}}`` from ESPN's summary box."""
    out: dict[int, dict[str, int | None]] = {}
    for team in ((summary or {}).get("boxscore") or {}).get("teams") or []:
        try:
            tid = int((team.get("team") or {}).get("id"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        stats = {s.get("name"): s.get("displayValue") for s in team.get("statistics") or []}
        comp_att = str(stats.get("completionAttempts") or "")
        parts = [p for p in comp_att.replace("/", "-").split("-") if p != ""]
        sacks = str(stats.get("sacksYardsLost") or "")
        sparts = [p for p in sacks.split("-") if p != ""]
        out[tid] = {
            "pass_yds_net": _espn_int(stats.get("netPassingYards")),
            "rush_yds": _espn_int(stats.get("rushingYards")),
            "rush_att": _espn_int(stats.get("rushingAttempts")),
            "comp": _espn_int(parts[0]) if len(parts) == 2 else None,
            "att": _espn_int(parts[1]) if len(parts) == 2 else None,
            "sacks": _espn_int(sparts[0]) if len(sparts) == 2 else None,
            "sack_yds": _espn_int(sparts[1]) if len(sparts) == 2 else None,
        }
    return out


def _box(
    df: pl.DataFrame, summary: dict[str, Any] | None, box: dict[str, Any] | None, league: str
) -> list[RuleResult | None]:
    espn = espn_team_stats(summary)
    need = (
        "pos_team",
        "pass",
        "rush",
        "scrimmage_play",
        "yds_receiving",
        "yds_rushed",
        "sack",
        "completion",
        "pass_attempt",
    )
    if not espn or not _has(df, *need):
        return []
    live = df.filter(~_t("penalty_no_play")) if "penalty_no_play" in df.columns else df
    agg = live.group_by(c("pos_team").cast(pl.Int64)).agg(
        comp=(_t("completion") & ~_t("sack")).sum(),
        att=(_t("pass_attempt") & ~_t("sack")).sum(),
        sacks=_t("sack").sum(),
        rush_att=(_t("rush") & _t("scrimmage_play")).sum(),
        rush_yds=pl.when(_t("rush") & _t("scrimmage_play")).then(c("yds_rushed").fill_null(0)).otherwise(0).sum(),
        pass_yds=pl.when(_t("pass") & _t("scrimmage_play")).then(c("yds_receiving").fill_null(0)).otherwise(0).sum(),
    )
    adv = {}
    for row in (box or {}).get("team") or []:
        try:
            adv[int(row.get("pos_team"))] = row
        except (TypeError, ValueError):
            continue
    comparisons: dict[str, list[dict[str, Any]]] = {}
    checked: dict[str, int] = {}

    def cmp(rule: str, tid: int, ours: float | None, theirs: float | None) -> None:
        if ours is None or theirs is None:
            return
        checked[rule] = checked.get(rule, 0) + 1
        if abs(float(ours) - float(theirs)) > 0.5:
            comparisons.setdefault(rule, []).append(
                {"team_id": tid, "processor": ours, "espn": theirs, "delta": float(ours) - float(theirs)}
            )

    for row in agg.iter_rows(named=True):
        tid = row["pos_team"]
        e = espn.get(tid)
        if e is None:
            continue
        # NFL: ESPN rushing excludes sacks and netPassingYards is net of sack
        # yardage; NCAA charges sacks (attempts and yards) to rushing.
        if league == "nfl":
            espn_pass_gross = (
                None if e["pass_yds_net"] is None or e["sack_yds"] is None else e["pass_yds_net"] + e["sack_yds"]
            )
            espn_rush_att = e["rush_att"]
        else:
            espn_pass_gross = e["pass_yds_net"]
            espn_rush_att = None if e["rush_att"] is None else e["rush_att"] - row["sacks"]
        cmp("box.plays_pass_yards_vs_espn", tid, row["pass_yds"], espn_pass_gross)
        cmp("box.plays_completions_vs_espn", tid, row["comp"], e["comp"])
        cmp("box.plays_pass_attempts_vs_espn", tid, row["att"], e["att"])
        cmp("box.plays_rush_attempts_vs_espn", tid, row["rush_att"], espn_rush_att)
        if league == "nfl":
            cmp("box.plays_rush_yards_vs_espn", tid, row["rush_yds"], e["rush_yds"])
            cmp("box.plays_sacks_vs_espn", tid, row["sacks"], e["sacks"])
        a = adv.get(tid)
        if a is not None:
            cmp("box.adv_pass_yards_vs_espn", tid, a.get("pass_yards"), espn_pass_gross)
            if league == "nfl":
                cmp("box.adv_rush_yards_vs_espn", tid, a.get("rush_yards"), e["rush_yds"])
            cmp("box.adv_pass_yards_vs_live_plays", tid, a.get("pass_yards"), row["pass_yds"])
            cmp("box.adv_rush_yards_vs_live_plays", tid, a.get("rush_yards"), row["rush_yds"])
    return [
        RuleResult(
            rule,
            9,
            f"team {rule.split('.', 1)[1].replace('_', ' ')} agree (|delta| <= 0.5)",
            n,
            len(comparisons.get(rule, [])),
            comparisons.get(rule, [])[:_SAMPLE_N],
            Severity.WARN,
        )
        for rule, n in checked.items()
    ]


# ---------------------------------------------------------------------------
# 11. play order: period / clock / type identity
# ---------------------------------------------------------------------------


def _order(df: pl.DataFrame) -> list[RuleResult | None]:
    """Period and clock move forward, and every live row carries ESPN's play type."""
    out: list[RuleResult | None] = []
    if _has(df, "period.number"):
        f = df.with_columns(__prev_period=c("period.number").shift(1))
        out.append(
            _row_rule(
                f,
                "period.monotone",
                11,
                "the period never decreases in processor row order",
                c("__prev_period").is_not_null(),
                c("period.number") < c("__prev_period"),
                ("period.number", "__prev_period"),
            )
        )
    if _has(df, "period.number", "clock.displayValue"):
        # "12:34" -> seconds; a malformed clock leaves the row undecidable, not failing
        secs = c("clock.displayValue").str.extract_groups(r"^(\d+):(\d{2})$").struct.rename_fields(["m", "s"])
        f = df.with_columns(
            __secs=secs.struct.field("m").cast(pl.Int32, strict=False) * 60
            + secs.struct.field("s").cast(pl.Int32, strict=False)
        ).with_columns(__prev_secs=c("__secs").shift(1), __prev_period=c("period.number").shift(1))
        out.append(
            _row_rule(
                f,
                "clock.monotone_within_period",
                11,
                "the game clock never runs backwards within a period",
                (c("period.number") == c("__prev_period")) & c("__secs").is_not_null() & c("__prev_secs").is_not_null(),
                c("__secs") > c("__prev_secs"),
                ("period.number", "clock.displayValue", "__prev_secs", "__secs"),
                severity=Severity.WARN,
            )
        )
    live = ~_t("penalty_no_play") if "penalty_no_play" in df.columns else pl.lit(True)
    for rule, col, sev in (
        ("type.null_id", "type.id", Severity.ERROR),
        # ESPN itself emits a null abbreviation on Sack / Pass Incompletion (sources/contract.py)
        ("type.null_abbreviation", "type.abbreviation", Severity.WARN),
    ):
        if col in df.columns:
            out.append(
                _row_rule(
                    df,
                    rule,
                    11,
                    f"every live play carries ESPN's {col}",
                    live,
                    _blank(col),
                    (col,),
                    severity=sev,
                )
            )
    return out


def _next_snap_continuity(df: pl.DataFrame) -> list[RuleResult | None]:
    """The end state of a snap is where the next snap starts -- across a drive boundary too.

    :func:`_field_position`'s ``ytg.continuity`` only looks within one drive, so a
    turnover on downs, an interception or a fumble that hands the ball over between two
    consecutive scrimmage rows is never checked. Yards-to-goal is measured from the
    offense, so the expected next start is ``100 - end`` when possession flips.
    """
    need = ("start.yardsToEndzone", "end.yardsToEndzone", "start.pos_team.id", "period.number")
    if not _has(df, *need) or not _has(df, "rush", "pass", "type.text"):
        return []
    f = (
        df.with_columns(__scrim=_scrimmage())
        .with_columns(
            __prev_scrim=c("__scrim").shift(1),
            __prev_end_ytg=c("end.yardsToEndzone").shift(1),
            __prev_pos=c("start.pos_team.id").shift(1),
            __prev_period=c("period.number").shift(1),
            __prev_score=(_t("scoringPlay") | _t("td_play")).shift(1)
            if _has(df, "scoringPlay", "td_play")
            else pl.lit(False),
        )
        .with_columns(
            __want=pl.when(c("__prev_pos") == c("start.pos_team.id"))
            .then(c("__prev_end_ytg"))
            .otherwise(100 - c("__prev_end_ytg"))
        )
    )
    return [
        _row_rule(
            f,
            "ytg.continuity_next_snap",
            2,
            "a scrimmage play starts where the previous scrimmage play ended (100 - end when possession flips)",
            c("__scrim")
            & c("__prev_scrim").fill_null(False)
            & (c("period.number") == c("__prev_period"))
            & ~c("__prev_score").fill_null(False),
            c("__want") != c("start.yardsToEndzone"),
            ("__prev_end_ytg", "__want", "start.yardsToEndzone", "start.pos_team.id"),
            severity=Severity.WARN,
        )
    ]


# ---------------------------------------------------------------------------
# 12. game-level aggregations
# ---------------------------------------------------------------------------


def _aggregations(
    df: pl.DataFrame, summary: dict[str, Any] | None, box: dict[str, Any] | None
) -> list[RuleResult | None]:
    """Per-game totals: team EPA vs the box, kickoffs vs scores, drives vs the feed."""
    out: list[RuleResult | None] = []
    team_box = (box or {}).get("team") or []
    if team_box and _has(df, "pos_team", "EPA", "scrimmage_play"):
        rows = {}
        for r in team_box:
            try:
                rows[int(r["pos_team"])] = r
            except (KeyError, TypeError, ValueError):
                continue
        live = (
            df.filter(_t("scrimmage_play"))
            .group_by("pos_team")
            .agg(pl.col("EPA").cast(pl.Float64).sum().alias("__epa"))
        )
        bad = []
        matched = 0
        for row in live.iter_rows(named=True):
            try:
                tid = int(row["pos_team"])
            except (TypeError, ValueError):
                tid = None
            want = rows.get(tid, {}).get("EPA_overall_off") if tid is not None else None
            if want is None:
                # a renamed box key or a float-origin id ("194.0") used to read as a clean
                # pass; an unreconciled team is the failure this rule exists to catch
                bad.append({"pos_team": row["pos_team"], "plays_EPA": round(float(row["__epa"]), 3), "box_EPA": None})
                continue
            matched += 1
            # the box rounds each team total to 2 dp (Float32 before that)
            if abs(float(row["__epa"]) - float(want)) > 0.05:
                bad.append({"pos_team": tid, "plays_EPA": round(float(row["__epa"]), 3), "box_EPA": float(want)})
        out.append(
            RuleResult(
                "epa.team_sum_matches_box",
                12,
                "the sum of row EPA over a team's scrimmage plays equals its advBoxScore EPA_overall_off",
                matched,
                len(bad),
                bad[:_SAMPLE_N],
            )
        )
    kickoffs = (
        int(df.filter(_t("kickoff_play")).height)
        if _has(df, "kickoff_play", "period.number", "scoring_play", "type.text")
        else 0
    )
    # a feed that emits no kickoff rows at all (ESPN's pre-2010 CFB drives) says nothing
    # about whether the kickoffs it does emit are complete
    if kickoffs:
        # One kickoff opens each half, plus one after every score that restarts play.
        # An overtime score restarts from the 25 and a try (PAT / two-point / defensive
        # conversion) rides the touchdown's kickoff, so neither adds one -- counting them
        # made every overtime game a false positive (cfb 401628439: 11 kickoffs vs 19
        # "expected"; 401858224: 11 vs 16 -- the only two firings on the 105-game sweep).
        halves = 2
        scores = int(
            df.filter(_t("scoring_play") & ~_t("kickoff_play") & (c("period.number") <= 4) & ~_is_try()).height
        )
        expected = scores + halves
        out.append(
            RuleResult(
                "plays.kickoff_count_matches_scores",
                12,
                "kickoffs equal the scores that restart play plus one per half (onside recoveries and "
                "score-ending halves make this approximate)",
                kickoffs,
                int(abs(kickoffs - expected) > max(2, expected // 5)),
                [{"kickoffs": kickoffs, "expected": expected, "scoring_plays": scores}]
                if abs(kickoffs - expected) > max(2, expected // 5)
                else [],
                Severity.WARN,
            )
        )
    feed_drives = [d for d in ((summary or {}).get("drives") or {}).get("previous") or [] if isinstance(d, dict)]
    if feed_drives and "drive.id" in df.columns:
        processed = df.get_column("drive.id").drop_nulls().n_unique()
        out.append(
            RuleResult(
                "drive.count_matches_feed",
                12,
                "the processed frame keeps one drive per drive in the feed",
                len(feed_drives),
                int(processed != len(feed_drives)),
                [{"processed_drives": processed, "feed_drives": len(feed_drives)}]
                if processed != len(feed_drives)
                else [],
                Severity.WARN,
            )
        )
    return out


# ---------------------------------------------------------------------------
# 10. attribution coverage
# ---------------------------------------------------------------------------


def _blank(col: str) -> pl.Expr:
    return c(col).is_null() | (c(col).cast(pl.Utf8).str.strip_chars() == "")


def _attribution(df: pl.DataFrame) -> list[RuleResult | None]:
    live = ~_t("penalty_no_play")
    text = c("text").fill_null("")
    returned_ko = (
        _t("kickoff_play")
        & ~_t("kickoff_tb")
        & ~_t("kickoff_onside")
        & ~_t("kickoff_fair_catch")
        & ~_t("kickoff_oob")
        & ~text.str.contains(r"(?i)touchback|out of bounds|out-of-bounds|fair catch")
    )
    returned_punt = (
        _t("punt")
        & ~_t("punt_tb")
        & ~_t("punt_fair_catch")
        & ~_t("punt_oob")
        & ~_t("punt_downed")
        & ~_t("punt_blocked")
        & ~text.str.contains(r"(?i)touchback|out of bounds|out-of-bounds|fair catch|downed|no return")
    )
    # a returner is only expected where the text describes a return
    returned_ko = returned_ko & text.str.contains(r"(?i)return")
    returned_punt = returned_punt & text.str.contains(r"(?i)return")
    named_sacker = text.str.contains(r"sacked[^.]*\(|(?i)sacked by|(?i)sack by")
    named_kicker = text.str.contains(r"(?i) kicks | kickoff |kickoff by|kicked by")
    specs: tuple[tuple[str, pl.Expr, str, tuple[str, ...]], ...] = (
        ("attr.passer", _t("pass_attempt") & ~_t("sack") & live, "passer_player_name", ("pass_attempt",)),
        ("attr.receiver", _t("completion") & ~_t("sack") & live, "receiver_player_name", ("completion",)),
        ("attr.rusher", _t("rush") & live, "rusher_player_name", ("rush",)),
        ("attr.sacker", _t("sack") & live & named_sacker, "sack_player_name", ("sack",)),
        ("attr.interceptor", _t("int") & live, "interception_player_name", ("int",)),
        ("attr.fg_kicker", _t("fg_attempt") & live, "fg_kicker_player_name", ("fg_attempt",)),
        ("attr.punter", _t("punt") & live, "punter_player_name", ("punt",)),
        ("attr.kickoff_kicker", _t("kickoff_play") & live & named_kicker, "kickoff_player_name", ("kickoff_play",)),
        ("attr.kickoff_returner", returned_ko & live, "kickoff_return_player_name", ("kickoff_play", "kickoff_tb")),
        ("attr.punt_returner", returned_punt & live, "punt_return_player_name", ("punt", "punt_tb", "punt_fair_catch")),
    )
    out: list[RuleResult | None] = []
    for rule, scope, col, cols in specs:
        out.append(
            _row_rule(
                df,
                rule,
                10,
                f"{col} is populated",
                scope,
                _blank(col),
                cols + (col, "penalty_no_play"),
                severity=Severity.INFO,
            )
        )
    return out


# ---------------------------------------------------------------------------
# entry points
# ---------------------------------------------------------------------------


def evaluate(
    plays: pl.DataFrame,
    *,
    summary: dict[str, Any] | None = None,
    box: dict[str, Any] | None = None,
    league: str = "nfl",
) -> list[RuleResult]:
    """Evaluate every applicable invariant on one game's processed plays.

    Args:
        plays: The processor's enriched plays frame (``proc.plays_frame``), in
            processor row order.
        summary: The ESPN summary the game was processed from; enables the
            header-score, WP-result, dropped-play and ESPN box comparisons.
        box: The processor's ``advBoxScore`` dict; enables the team-box rules
            and the ``box.*`` section reconciliations
            (:mod:`sportsdataverse.validation.box_reconcile`).
        league: ``"nfl"`` or ``"cfb"`` -- selects the box-score sack convention.

    Returns:
        One RuleResult per rule whose columns (and optional inputs) are present,
        including rules that did not fire (``n_violations == 0``).
    """
    groups: list[RuleResult | None] = []
    groups += _timeouts(plays)
    groups += _field_position(plays)
    groups += _score(plays, summary)
    groups += _possession(plays)
    groups += _down_distance(plays)
    groups += _ep_wp(plays, summary)
    groups += _flags(plays)
    groups += _plays(plays, summary)
    groups += _box(plays, summary, box, league)
    groups += _attribution(plays)
    groups += _order(plays)
    groups += _next_snap_continuity(plays)
    groups += _aggregations(plays, summary, box)
    groups += evaluate_box(plays, box, league)
    return [r for r in groups if r is not None]


def run(
    dataset: str,
    frame: pl.DataFrame,
    ctx: CheckContext,
    *,
    summary: dict[str, Any] | None = None,
    box: dict[str, Any] | None = None,
) -> list[Finding]:
    """Harness wrapper: one Finding per fired rule (INFO coverage rules included).

    Args:
        dataset: Dataset identifier recorded on each finding.
        frame: One game's processed plays frame.
        ctx: Check context; ``ctx.domain`` (``nfl`` / ``cfb``) selects the league.
        summary: Optional ESPN summary for the header / raw / box comparisons.
        box: Optional processor ``advBoxScore``.

    Returns:
        Findings whose ``metric`` is the violation count and whose locator
        carries the rule, invariant group and denominator.
    """
    findings = []
    for r in evaluate(frame, summary=summary, box=box, league=ctx.domain):
        if not r.n_violations:
            continue
        findings.append(
            Finding(
                "pbp_invariants",
                r.severity,
                ctx.domain,
                dataset,
                f"{r.rule}: {r.n_violations}/{r.n_checked} violate -- {r.description}",
                locator={"rule": r.rule, "invariant": r.invariant, "n_checked": r.n_checked},
                metric=float(r.n_violations),
                needs_judgment=r.severity is not Severity.ERROR,
                sample=r.samples,
            )
        )
    return findings
