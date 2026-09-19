"""``advBoxScore`` reconciliation: every derived section against the plays frame (V1b).

The invariants in :mod:`~sportsdataverse.validation.pbp_invariants` judge the
processed play rows. These judge what both processors *derive* from them --
the 21 ``advBoxScore`` sections Game on Paper renders and the ``espn_*_adv_*``
dataset families release -- against the rows themselves:

* a team total is the sum of the rows that qualify for it (``box.team_totals_match_plays``);
* a per-player section sums to the same rows (``box.player_sums_match_team``);
* a rate is its own numerator over its own denominator (``box.rates_recompute_from_counts``);
* an offensive total is the opponent's defensive total (``box.sections_mirror_off_def``);
* the turnover section counts the turnover rows (``box.turnovers_match_flags``);
* the drive sections count the frame's drives (``box.drives_match_drive_rows``);
* a usage share is the player's count over the team's (``box.usage_shares_sum_to_one``);
* no section carries a team the game was not played by (``box.team_ids_in_game``).

**The filters are the builders'.** The usage sections (``football.usage_box``)
are reconciled through the builder's own
:func:`~sportsdataverse.football.usage_box._standing_scrimmage` and
:func:`~sportsdataverse.football.usage_box._drive_frame`, imported rather than
restated. ``create_box_score`` builds its sections inline, so its filters are
mirrored here in :data:`_FILTERS` and each check names the builder frame it
mirrors; only columns whose filter and aggregation are **identical in both
processors** are checked, which is why the league-divergent totals
(``total_yards`` / ``off_yards``, which CFB zeroes on interception rows, and
``penalty_first_downs_created``, which CFB restricts to accepted penalties)
are absent.

**Fail closed.** A team the frame has rows for but the box has no row for is a
violation, not a skip -- the #553 review's finding 4 (a failed id join read as
a clean pass). A team id that is not a clean integer never matches, so the
float-origin ``"194.0"`` id fails rather than reconciling. A section missing
from a *full* box (one carrying ``team``, ``situational`` and ``drives``) is a
violation; a caller who passes a partial box (the offline sweep passes
``{"team": ...}``) is only judged on what it passed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import polars as pl

from sportsdataverse.validation.findings import Severity

_SAMPLE_N = 5
#: Box sections round Float32 columns to 2 dp; a sum of them can drift a little further.
_TOL = 0.06
#: A rate is a ratio of two already-rounded numbers.
_RATE_TOL = 0.02
#: Every section ``create_box_score`` emits (``football.usage_box.SECTIONS`` included).
_SECTIONS = frozenset(
    {
        "pass",
        "rush",
        "receiver",
        "team",
        "situational",
        "defensive",
        "defensive_players",
        "specialists",
        "turnover",
        "drives",
        "espn_team",
        "espn_players",
        "player_usage",
        "position_group_usage",
        "tackles",
        "position_group_tackles",
        "team_usage",
        "drive_scripting",
        "st_kickers",
        "st_punters",
        "st_returners",
        "st_blocks",
        "st_team",
    }
)
#: A box carrying this many sections is a whole ``advBoxScore``, so a section MISSING from it
#: is a defect. Counting sections rather than naming three of them keeps the marker valid when
#: the missing section is one of the named ones. A caller passing a slice (the offline sweep
#: passes ``{"team": ...}``) stays below it and is judged only on what it passed.
_FULL_BOX_SECTIONS = 6

c = pl.col


def _t(name: str) -> pl.Expr:
    """A flag column read as strictly True (null -> False)."""
    return (c(name) == True).fill_null(False)  # noqa: E712


#: ``create_box_score``'s inline filters, by the builder frame each one feeds.
_FILTERS: dict[str, tuple[Callable[[], pl.Expr] | None, tuple[str, ...]]] = {
    # name: (predicate, the columns the predicate itself needs)
    "all": (None, ()),  # team_base_box
    "scrimmage": (lambda: _t("scrimmage_play"), ("scrimmage_play",)),  # team_scrimmage_box
    "sp": (lambda: _t("sp"), ("sp",)),  # team_sp_box
    "pass": (lambda: _t("pass") & _t("scrimmage_play"), ("pass", "scrimmage_play")),  # team_scrimmage_box_pass
    "rush": (lambda: _t("rush") & _t("scrimmage_play"), ("rush", "scrimmage_play")),  # team_scrimmage_box_rush
    "penalty": (lambda: _t("penalty_flag"), ("penalty_flag",)),  # team_pen_box
    "rz": (lambda: _t("rz_play") & _t("scrimmage_play"), ("rz_play", "scrimmage_play")),  # situation_box_rz
    "early": (lambda: _t("early_down") & _t("scrimmage_play"), ("early_down", "scrimmage_play")),
    "late": (lambda: _t("late_down") & _t("scrimmage_play"), ("late_down", "scrimmage_play")),
    "standard": (lambda: _t("standard_down") & _t("scrimmage_play"), ("standard_down", "scrimmage_play")),
    "passing_down": (lambda: _t("passing_down") & _t("scrimmage_play"), ("passing_down", "scrimmage_play")),
    "middle8": (lambda: _t("middle_8") & _t("scrimmage_play"), ("middle_8", "scrimmage_play")),
}


@dataclass(frozen=True)
class _Check:
    """One scalar reconciliation: ``box[section][team][column] == agg(frame[filter])``."""

    section: str
    team_key: str
    column: str
    filter: str
    expr: Callable[[], pl.Expr]
    needs: tuple[str, ...]
    tol: float = _TOL


def _sum(col: str) -> Callable[[], pl.Expr]:
    return lambda: c(col).cast(pl.Float64, strict=False).fill_null(0.0).sum()


def _count_true(col: str) -> Callable[[], pl.Expr]:
    return lambda: _t(col).sum()


def _rows() -> Callable[[], pl.Expr]:
    return lambda: pl.len()


def _ck(section: str, team_key: str, column: str, flt: str, expr: Callable[[], pl.Expr], *needs: str) -> _Check:
    return _Check(section, team_key, column, flt, expr, tuple(needs))


#: Team-level totals. Each row names the ``create_box_score`` frame it mirrors.
_TEAM_CHECKS: tuple[_Check, ...] = (
    # team_base_box (whole frame)
    _ck("team", "pos_team", "EPA_plays", "all", _sum("play"), "play"),
    _ck("team", "pos_team", "EPA_overall_total", "all", _sum("EPA"), "EPA"),
    # team_scrimmage_box
    _ck("team", "pos_team", "scrimmage_plays", "scrimmage", _count_true("scrimmage_play"), "scrimmage_play"),
    _ck("team", "pos_team", "EPA_overall_offense", "scrimmage", _sum("EPA"), "EPA"),
    _ck("team", "pos_team", "EPA_explosive", "scrimmage", _sum("EPA_explosive"), "EPA_explosive"),
    _ck("team", "pos_team", "EPA_non_explosive", "scrimmage", _sum("EPA_non_explosive"), "EPA_non_explosive"),
    # team_rush_base_box
    _ck("team", "pos_team", "first_downs_created", "scrimmage", _sum("first_down_created"), "first_down_created"),
    # team_sp_box
    _ck("team", "pos_team", "special_teams_plays", "sp", _count_true("sp"), "sp"),
    _ck("team", "pos_team", "EPA_special_teams", "sp", _sum("EPA_sp"), "EPA_sp"),
    _ck("team", "pos_team", "field_goals", "sp", _sum("fg_attempt"), "fg_attempt"),
    _ck("team", "pos_team", "kickoff_plays", "sp", _sum("kickoff_play"), "kickoff_play"),
    _ck("team", "pos_team", "punt_plays", "sp", _sum("punt_play"), "punt_play"),
    # team_scrimmage_box_pass
    _ck("team", "pos_team", "passes", "pass", _sum("pass"), "pass"),
    _ck("team", "pos_team", "pass_yards", "pass", _sum("yds_receiving"), "yds_receiving"),
    _ck("team", "pos_team", "EPA_passing_overall", "pass", _sum("EPA"), "EPA"),
    _ck(
        "team",
        "pos_team",
        "passing_first_downs_created",
        "pass",
        _sum("first_down_created"),
        "first_down_created",
    ),
    # team_scrimmage_box_rush
    _ck("team", "pos_team", "rushes", "rush", _sum("rush"), "rush"),
    _ck("team", "pos_team", "rush_yards", "rush", _sum("yds_rushed"), "yds_rushed"),
    _ck("team", "pos_team", "EPA_rushing_overall", "rush", _sum("EPA"), "EPA"),
    _ck(
        "team",
        "pos_team",
        "rushing_first_downs_created",
        "rush",
        _sum("first_down_created"),
        "first_down_created",
    ),
    # team_pen_box
    _ck("team", "pos_team", "total_pen_yards", "penalty", _sum("statYardage"), "statYardage"),
    _ck("team", "pos_team", "EPA_penalty", "penalty", _sum("EPA_penalty"), "EPA_penalty"),
    # situation_box_*
    _ck("situational", "pos_team", "EPA_success", "scrimmage", _sum("EPA_success"), "EPA_success"),
    _ck("situational", "pos_team", "EPA_success_rz", "rz", _sum("EPA_success"), "EPA_success"),
    _ck("situational", "pos_team", "EPA_success_pass", "pass", _sum("EPA_success"), "EPA_success"),
    _ck("situational", "pos_team", "EPA_success_rush", "rush", _sum("EPA_success"), "EPA_success"),
    _ck("situational", "pos_team", "early_downs", "early", _sum("early_down"), "early_down"),
    _ck("situational", "pos_team", "EPA_early_down", "early", _sum("EPA"), "EPA"),
    _ck("situational", "pos_team", "late_downs", "late", _sum("late_down"), "late_down"),
    _ck("situational", "pos_team", "EPA_late_down", "late", _sum("EPA"), "EPA"),
    _ck("situational", "pos_team", "standard_downs", "standard", _sum("standard_down"), "standard_down"),
    _ck("situational", "pos_team", "EPA_standard_down", "standard", _sum("EPA"), "EPA"),
    _ck("situational", "pos_team", "passing_downs", "passing_down", _sum("passing_down"), "passing_down"),
    _ck("situational", "pos_team", "EPA_passing_down", "passing_down", _sum("EPA"), "EPA"),
    _ck("situational", "pos_team", "middle_8", "middle8", _sum("middle_8"), "middle_8"),
    _ck("situational", "pos_team", "EPA_middle_8", "middle8", _sum("EPA"), "EPA"),
    # def_base_box / def_box_havoc_pass (grouped by the DEFENDING team)
    _ck("defensive", "def_pos_team", "scrimmage_plays", "scrimmage", _count_true("scrimmage_play"), "scrimmage_play"),
    _ck("defensive", "def_pos_team", "TFL", "scrimmage", _sum("TFL"), "TFL"),
    _ck("defensive", "def_pos_team", "havoc_total", "scrimmage", _sum("havoc"), "havoc"),
    _ck("defensive", "def_pos_team", "def_int", "scrimmage", _sum("int"), "int"),
    _ck("defensive", "def_pos_team", "fumbles", "scrimmage", _sum("forced_fumble"), "forced_fumble"),
    _ck("defensive", "def_pos_team", "num_pass_plays", "pass", _sum("pass"), "pass"),
    _ck("defensive", "def_pos_team", "sacks", "pass", _sum("sack_vec"), "sack_vec"),
    _ck("defensive", "def_pos_team", "pass_breakups", "pass", _sum("pass_breakup"), "pass_breakup"),
)

#: ``usage_box._team_rows`` totals, reconciled through the builder's own standing-scrimmage filter.
_USAGE_TEAM_CHECKS: tuple[tuple[str, Callable[[], pl.Expr], tuple[str, ...]], ...] = (
    ("plays", _rows(), ()),
    ("rushes", lambda: c("u_rush").sum(), ()),
    ("targets", lambda: c("u_target").sum(), ()),
    ("completions", lambda: c("u_completion").sum(), ()),
    ("first_downs", lambda: c("u_first_down_created").sum(), ()),
    ("touchdowns", lambda: c("u_touchdown").sum(), ()),
    ("explosive_plays", lambda: c("u_EPA_explosive").sum(), ()),
    ("successful_plays", lambda: c("u_EPA_success").sum(), ()),
    ("epa", lambda: c("u_EPA").cast(pl.Float64).fill_null(0.0).sum(), ()),
)

#: Per-player sections against the same rows their builder aggregates.
_PLAYER_CHECKS: tuple[tuple[str, str, str, Callable[[], pl.Expr], tuple[str, ...]], ...] = (
    # (section, box column summed over the section's players, filter, frame expr, needs)
    ("rush", "Car", "rush", _sum("rush"), ("rush",)),
    ("rush", "Yds", "rush", _sum("yds_rushed"), ("yds_rushed",)),
    ("pass", "Att", "pass", _sum("pass_attempt"), ("pass_attempt",)),
    ("pass", "Comp", "pass", _sum("completion"), ("completion",)),
    ("receiver", "Tar", "pass", _sum("target"), ("target",)),
    ("receiver", "Rec", "pass", _sum("completion"), ("completion",)),
)

#: ``rate == numerator / denominator`` inside one row of one section.
#: Only ratios whose denominator is the count the builder actually divided by: a ``.mean()``
#: over a null-sparse column divides by its own non-null count, not by the section's play
#: count (``EPA_non_explosive_per_play`` is 19.12/46 non-explosive plays, not /49 scrimmage
#: plays), so those columns are reconciled as sums only.
_RATE_CHECKS: tuple[tuple[str, str, str, str], ...] = (
    ("team", "EPA_per_play", "EPA_overall_off", "scrimmage_plays"),
    ("team", "EPA_explosive_rate", "EPA_explosive", "scrimmage_plays"),
    ("team", "first_downs_created_rate", "first_downs_created", "scrimmage_plays"),
    ("team", "yards_per_pass", "pass_yards", "passes"),
    ("team", "EPA_passing_per_play", "EPA_passing_overall", "passes"),
    ("team", "passing_first_downs_created_rate", "passing_first_downs_created", "passes"),
    ("team", "yards_per_rush", "rush_yards", "rushes"),
    ("team", "EPA_rushing_per_play", "EPA_rushing_overall", "rushes"),
    ("team", "rushing_first_downs_created_rate", "rushing_first_downs_created", "rushes"),
    ("defensive", "havoc_total_rate", "havoc_total", "scrimmage_plays"),
    ("defensive", "havoc_total_pass_rate", "havoc_total_pass", "num_pass_plays"),
    ("defensive", "sacks_rate", "sacks", "num_pass_plays"),
    ("team_usage", "epa_per_play", "epa", "plays"),
    ("team_usage", "success_rate", "successful_plays", "plays"),
    ("team_usage", "explosive_rate", "explosive_plays", "plays"),
    ("team_usage", "third_down_rate", "third_down_conversions", "third_down_opportunities"),
    ("player_usage", "epa_per_opportunity", "epa", "opportunities"),
    ("player_usage", "success_rate", "successful_plays", "opportunities"),
    ("player_usage", "explosive_rate", "explosive_plays", "opportunities"),
    ("player_usage", "fd_td_rate", "fd_or_td", "opportunities"),
    ("drive_scripting", "epa_per_play", "epa", "plays"),
    ("drive_scripting", "success_rate", "successes", "plays"),
    ("drive_scripting", "yards_per_play", "yards", "plays"),
    ("drive_scripting", "points_per_drive", "points", "drives"),
    ("drive_scripting", "touchdown_rate", "touchdowns", "drives"),
    ("drive_scripting", "scoring_opp_rate", "scoring_opps", "drives"),
    ("st_kickers", "fg_pct", "fg_made", "fg_attempts"),
    ("st_punters", "punt_inside_20_rate", "punt_inside_20", "punts"),
    ("st_returners", "kick_return_avg", "kick_return_yards", "kick_returns"),
    ("st_returners", "punt_return_avg", "punt_return_yards", "punt_returns"),
)

#: ``a[column] == b[mirror]`` across the two teams.
_MIRROR_CHECKS: tuple[tuple[str, str, str, str, str, float], ...] = (
    # (section_a, key_a, column_a, section_b, column_b, sign)
    ("team", "pos_team", "scrimmage_plays", "defensive", "scrimmage_plays", 1.0),
    ("team", "pos_team", "passes", "defensive", "num_pass_plays", 1.0),
    ("turnover", "pos_team", "turnovers", "turnover", "takeaways", 1.0),
    ("turnover", "pos_team", "fumbles_lost", "turnover", "fumble_recoveries_gained", 1.0),
    ("turnover", "pos_team", "st_turnovers_lost", "turnover", "st_turnovers_gained", 1.0),
    ("turnover", "pos_team", "turnover_margin", "turnover", "turnover_margin", -1.0),
)

#: ``(share column, its numerator, the team_usage column that is its denominator)``.
_SHARE_CHECKS: tuple[tuple[str, str, str], ...] = (
    ("target_share", "targets", "targets"),
    ("first_down_share", "first_downs", "first_downs"),
    ("touch_share", "touches", "touches"),
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _tid(value: Any) -> int | None:
    """A team id as a clean integer, or None.

    A float-origin id (``194.0`` / ``"194.0"``) is deliberately NOT coerced: it
    must fail to reconcile rather than quietly match (#553 finding 4).
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.lstrip("-").isdigit():
            return int(text)
    return None


def _num(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _section(box: dict[str, Any] | None, name: str) -> list[dict[str, Any]] | None:
    """The section's rows, or None when the box does not carry the section at all."""
    rows = (box or {}).get(name)
    if rows is None:
        return None
    return [r for r in rows if isinstance(r, dict)]


def _by_team(rows: list[dict[str, Any]], key: str) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for row in rows:
        tid = _tid(row.get(key))
        if tid is not None:
            out[tid] = row
    return out


def _is_full_box(box: dict[str, Any] | None) -> bool:
    if not box:
        return False
    return len(_SECTIONS & set(box)) >= _FULL_BOX_SECTIONS


def _frame_totals(df: pl.DataFrame, team_key: str, expr: pl.Expr) -> dict[int, float]:
    if team_key not in df.columns or df.height == 0:
        return {}
    grouped = df.group_by(team_key).agg(expr.alias("__v"))
    out: dict[int, float] = {}
    for row in grouped.iter_rows(named=True):
        tid = _tid(row[team_key])
        value = _num(row["__v"])
        if tid is not None and value is not None:
            out[tid] = value
    return out


class _Tally:
    """Collects one rule's comparisons: how many were attempted, which disagreed."""

    def __init__(self) -> None:
        self.checked = 0
        self.bad: list[dict[str, Any]] = []

    def compare(self, tol: float, sample: dict[str, Any], ours: float | None, theirs: float | None) -> None:
        self.checked += 1
        if ours is None or theirs is None or abs(ours - theirs) > tol:
            self.bad.append({**sample, "plays": ours, "box": theirs})

    def miss(self, sample: dict[str, Any]) -> None:
        """A comparison the box could not supply -- counted and failed, never skipped."""
        self.checked += 1
        self.bad.append(sample)

    def result(self, rule: str, description: str, severity: Severity = Severity.ERROR) -> Any:
        from sportsdataverse.validation.pbp_invariants import RuleResult

        return RuleResult(rule, 12, description, self.checked, len(self.bad), self.bad[:_SAMPLE_N], severity)


def _filtered(plays: pl.DataFrame, name: str) -> pl.DataFrame | None:
    """``plays`` narrowed by one of ``create_box_score``'s filters; None when a column is missing."""
    predicate, needs = _FILTERS[name]
    if any(col not in plays.columns for col in needs):
        return None
    return plays if predicate is None else plays.filter(predicate())


# ---------------------------------------------------------------------------
# rules
# ---------------------------------------------------------------------------


def _team_totals(plays: pl.DataFrame, box: dict[str, Any] | None, standing: pl.DataFrame | None) -> Any:
    """``box.team_totals_match_plays`` -- team / situational / defensive / team_usage totals."""
    full = _is_full_box(box)
    tally = _Tally()
    cache: dict[str, pl.DataFrame | None] = {}
    for check in _TEAM_CHECKS:
        if any(col not in plays.columns for col in check.needs):
            continue
        if check.filter not in cache:
            cache[check.filter] = _filtered(plays, check.filter)
        df = cache[check.filter]
        if df is None:
            continue
        ours = _frame_totals(df, check.team_key, check.expr())
        if not ours:
            continue
        rows = _section(box, check.section)
        if rows is None:
            if full:
                tally.miss({"section": check.section, "column": check.column, "reason": "section missing"})
            continue
        indexed = _by_team(rows, check.team_key)
        for tid, value in sorted(ours.items()):
            sample = {"section": check.section, "column": check.column, "team_id": tid}
            row = indexed.get(tid)
            if row is None:
                tally.miss({**sample, "reason": "no box row for this team", "plays": value})
                continue
            if check.column not in row:
                tally.miss({**sample, "reason": "column absent from the box row", "plays": value})
                continue
            tally.compare(check.tol, sample, value, _num(row[check.column]))
    if standing is not None and standing.height:
        rows = _section(box, "team_usage")
        if rows is None:
            if full:
                tally.miss({"section": "team_usage", "reason": "section missing"})
        else:
            indexed = _by_team(rows, "pos_team")
            for column, expr, needs in _USAGE_TEAM_CHECKS:
                if any(col not in standing.columns for col in needs):
                    continue
                ours = _frame_totals(standing, "pos_team", expr())
                for tid, value in sorted(ours.items()):
                    sample = {"section": "team_usage", "column": column, "team_id": tid}
                    row = indexed.get(tid)
                    if row is None:
                        tally.miss({**sample, "reason": "no box row for this team", "plays": value})
                    elif column not in row:
                        tally.miss({**sample, "reason": "column absent from the box row", "plays": value})
                    else:
                        tally.compare(_TOL, sample, value, _num(row[column]))
    return tally.result(
        "box.team_totals_match_plays",
        "every advBoxScore team total equals the sum of the plays that qualify for it",
    )


def _player_sums(plays: pl.DataFrame, box: dict[str, Any] | None) -> Any:
    """``box.player_sums_match_team`` -- a per-player section sums to the same rows."""
    full = _is_full_box(box)
    tally = _Tally()
    cache: dict[str, pl.DataFrame | None] = {}
    for section, column, flt, expr, needs in _PLAYER_CHECKS:
        if any(col not in plays.columns for col in needs):
            continue
        if flt not in cache:
            cache[flt] = _filtered(plays, flt)
        df = cache[flt]
        if df is None:
            continue
        ours = _frame_totals(df, "pos_team", expr())
        if not ours:
            continue
        rows = _section(box, section)
        if rows is None:
            if full:
                tally.miss({"section": section, "column": column, "reason": "section missing"})
            continue
        sums: dict[int, float] = {}
        for row in rows:
            tid = _tid(row.get("pos_team"))
            value = _num(row.get(column))
            if tid is None or value is None:
                continue
            sums[tid] = sums.get(tid, 0.0) + value
        for tid, value in sorted(ours.items()):
            sample = {"section": section, "column": column, "team_id": tid}
            if tid not in sums:
                tally.miss({**sample, "reason": "no box rows for this team", "plays": value})
                continue
            tally.compare(_TOL, sample, value, sums[tid])
    return tally.result(
        "box.player_sums_match_team",
        "a per-player advBoxScore section sums, per team, to the plays it aggregates",
    )


def _rates(box: dict[str, Any] | None) -> Any:
    """``box.rates_recompute_from_counts`` -- a rate is its own numerator over its own denominator."""
    tally = _Tally()
    for section, rate_col, num_col, den_col in _RATE_CHECKS:
        rows = _section(box, section)
        if rows is None:
            continue
        for row in rows:
            if rate_col not in row or num_col not in row or den_col not in row:
                continue
            rate, num, den = _num(row.get(rate_col)), _num(row.get(num_col)), _num(row.get(den_col))
            if num is None or den is None or den == 0:
                continue  # a zero denominator has no rate to recompute
            sample = {
                "section": section,
                "column": rate_col,
                "team_id": _tid(row.get("pos_team")) or _tid(row.get("def_pos_team")),
                "numerator": num,
                "denominator": den,
            }
            # a rounded numerator over a small denominator moves the quotient
            tol = _RATE_TOL + 0.005 / abs(den)
            tally.compare(tol, sample, num / den, rate)
    return tally.result(
        "box.rates_recompute_from_counts",
        "every advBoxScore rate equals its own numerator over its own denominator (within rounding)",
    )


def _mirror(box: dict[str, Any] | None, teams: set[int]) -> Any:
    """``box.sections_mirror_off_def`` -- one team's offence is the other's defence."""
    tally = _Tally()
    if len(teams) != 2:
        return tally.result("box.sections_mirror_off_def", "the two teams' mirrored box totals agree")
    for section_a, key_a, column_a, section_b, column_b, sign in _MIRROR_CHECKS:
        rows_a, rows_b = _section(box, section_a), _section(box, section_b)
        if rows_a is None or rows_b is None:
            continue
        key_b = "def_pos_team" if section_b == "defensive" else "pos_team"
        a = _by_team(rows_a, key_a)
        b = _by_team(rows_b, key_b)
        for tid in sorted(teams):
            other = next(t for t in teams if t != tid)
            row_a, row_b = a.get(tid), b.get(other)
            if row_a is None or row_b is None or column_a not in row_a or column_b not in row_b:
                continue
            sample = {"section": f"{section_a}.{column_a}", "mirror": f"{section_b}.{column_b}", "team_id": tid}
            ours, theirs = _num(row_a[column_a]), _num(row_b[column_b])
            tally.compare(_TOL, sample, ours, None if theirs is None else sign * theirs)
    return tally.result(
        "box.sections_mirror_off_def",
        "a team's offensive box total equals the opponent's mirrored defensive total",
    )


def _turnovers(plays: pl.DataFrame, box: dict[str, Any] | None) -> Any:
    """``box.turnovers_match_flags`` -- the turnover section counts the frame's turnover rows.

    Compared against the ``*_pbp`` keys: ``turnovers`` / ``Int`` / ``fumbles_lost``
    are overwritten from ESPN's official box when it covers the game, so they are
    not a statement about the plays frame.
    """
    tally = _Tally()
    need = ("is_pos_team_turnover", "is_def_pos_team_turnover", "int_turnover", "pos_team", "def_pos_team")
    if any(col not in plays.columns for col in need):
        return tally.result("box.turnovers_match_flags", "the turnover section counts the frame's turnover rows")
    rows = _section(box, "turnover")
    if rows is None:
        return tally.result("box.turnovers_match_flags", "the turnover section counts the frame's turnover rows")
    # to_events: a lost turnover is charged to pos_team on an offensive row and to
    # def_pos_team on a defensive one (create_box_score's pos_ev / def_ev)
    lost: dict[int, float] = {}
    ints: dict[int, float] = {}
    pos = plays.filter(_t("is_pos_team_turnover"))
    for tid, value in _frame_totals(pos, "pos_team", pl.len()).items():
        lost[tid] = lost.get(tid, 0.0) + value
    for tid, value in _frame_totals(pos, "pos_team", _t("int_turnover").sum()).items():
        ints[tid] = ints.get(tid, 0.0) + value
    dfn = plays.filter(_t("is_def_pos_team_turnover"))
    for tid, value in _frame_totals(dfn, "def_pos_team", pl.len()).items():
        lost[tid] = lost.get(tid, 0.0) + value
    indexed = _by_team(rows, "pos_team")
    for tid in sorted(indexed):
        row = indexed[tid]
        for column, ours in (
            ("turnovers_pbp", lost.get(tid, 0.0)),
            ("Int_pbp", ints.get(tid, 0.0)),
            ("fumbles_lost_pbp", lost.get(tid, 0.0) - ints.get(tid, 0.0)),
        ):
            sample = {"section": "turnover", "column": column, "team_id": tid}
            if column not in row:
                tally.miss({**sample, "reason": "column absent from the box row", "plays": ours})
                continue
            tally.compare(0.5, sample, ours, _num(row[column]))
    return tally.result(
        "box.turnovers_match_flags",
        "the turnover section's play-by-play counts equal the frame's turnover rows",
    )


def _drives(plays: pl.DataFrame, box: dict[str, Any] | None, standing: pl.DataFrame | None) -> Any:
    """``box.drives_match_drive_rows`` -- the drive sections count the frame's drives."""
    full = _is_full_box(box)
    tally = _Tally()
    scrimmage = _filtered(plays, "scrimmage")
    if scrimmage is not None and "drive.id" in scrimmage.columns and "pos_team" in scrimmage.columns:
        rows = _section(box, "drives")
        checks = (
            ("drives", c("drive.id").n_unique(), ("drive.id",)),
            ("drive_total_gained_yards", _sum("drive.yards")(), ("drive.yards",)),
            ("drive_total_available_yards", _sum("drive_start")(), ("drive_start",)),
        )
        if rows is None:
            if full:
                tally.miss({"section": "drives", "reason": "section missing"})
        else:
            indexed = _by_team(rows, "pos_team")
            for column, expr, needs in checks:
                if any(col not in scrimmage.columns for col in needs):
                    continue
                for tid, value in sorted(_frame_totals(scrimmage, "pos_team", expr).items()):
                    sample = {"section": "drives", "column": column, "team_id": tid}
                    row = indexed.get(tid)
                    if row is None:
                        tally.miss({**sample, "reason": "no box row for this team", "plays": value})
                    elif column not in row:
                        tally.miss({**sample, "reason": "column absent from the box row", "plays": value})
                    else:
                        tally.compare(_TOL, sample, value, _num(row[column]))
    # drive_scripting: the builder's own per-drive frame, summed back per team.
    # _drive_frame needs a half (or a period to derive it) -- its own precondition, so a
    # frame without one skips the section rather than raising inside the gate.
    if standing is not None and standing.height and {"half", "period"} & set(standing.columns):
        from sportsdataverse.football.usage_box import _drive_frame

        drv = _drive_frame(standing)
        rows = _section(box, "drive_scripting")
        if rows is None:
            if full:
                tally.miss({"section": "drive_scripting", "reason": "section missing"})
        elif drv.height and "pos_team" in drv.columns:
            sums: dict[int, dict[str, float]] = {}
            for row in rows:
                row_team = _tid(row.get("pos_team"))
                if row_team is None:
                    continue
                bucket = sums.setdefault(row_team, {"drives": 0.0, "plays": 0.0})
                for column in bucket:
                    cell = _num(row.get(column))
                    if cell is not None:
                        bucket[column] += cell
            for column, expr in (("drives", pl.len()), ("plays", c("plays").sum())):
                for tid, value in sorted(_frame_totals(drv, "pos_team", expr).items()):
                    sample = {"section": "drive_scripting", "column": column, "team_id": tid}
                    if tid not in sums:
                        tally.miss({**sample, "reason": "no box rows for this team", "plays": value})
                    else:
                        tally.compare(_TOL, sample, value, sums[tid][column])
    return tally.result(
        "box.drives_match_drive_rows",
        "the drives and drive_scripting sections equal the frame grouped by drive",
    )


def _shares(box: dict[str, Any] | None) -> Any:
    """``box.usage_shares_sum_to_one`` -- a usage share is the player's count over the team's.

    ``usage_box._team_totals`` takes its denominator from EVERY standing scrimmage
    play, so an unattributed target leaves the shares summing to *less* than one;
    the invariant is therefore ``share == player / team`` and ``sum <= 1``.
    """
    tally = _Tally()
    players, teams = _section(box, "player_usage"), _section(box, "team_usage")
    if not players and teams and _is_full_box(box):
        # the usage box is built under a try/except that empties all eleven sections on a
        # failure: an empty player_usage against a team_usage that counted plays is that
        # failure, not "this game had no rushers or receivers"
        for row in teams:
            if any(_num(row.get(col)) for col in ("targets", "rushes", "touches")):
                tally.miss(
                    {
                        "section": "player_usage",
                        "team_id": _tid(row.get("pos_team")),
                        "reason": "no player rows although team_usage counted plays",
                    }
                )
    if not players or not teams:
        return tally.result(
            "box.usage_shares_sum_to_one",
            "each player's usage share is his own count over the team's total, and the team's shares sum to at most one",
            Severity.WARN,
        )
    totals = _by_team(teams, "pos_team")
    for share_col, num_col, team_col in _SHARE_CHECKS:
        running: dict[int, float] = {}
        for row in players:
            tid = _tid(row.get("pos_team"))
            share, num = _num(row.get(share_col)), _num(row.get(num_col))
            den = _num((totals.get(tid) or {}).get(team_col)) if tid is not None else None
            if tid is None or num is None or den is None or den == 0:
                continue
            tally.compare(
                _RATE_TOL,
                {"section": "player_usage", "column": share_col, "team_id": tid, "player": row.get("player_name")},
                num / den,
                share,
            )
            if share is not None:
                running[tid] = running.get(tid, 0.0) + share
        for tid, total in sorted(running.items()):
            tally.checked += 1
            if total > 1.0 + _RATE_TOL:
                tally.bad.append({"section": "player_usage", "column": share_col, "team_id": tid, "sum": total})
    return tally.result(
        "box.usage_shares_sum_to_one",
        "each player's usage share is his own count over the team's total, and the team's shares sum to at most one",
        Severity.WARN,
    )


def _team_ids(box: dict[str, Any] | None, teams: set[int]) -> Any:
    """``box.team_ids_in_game`` -- no section carries a team the game was not played by."""
    tally = _Tally()
    if len(teams) != 2 or not box:
        return tally.result("box.team_ids_in_game", "every advBoxScore row names one of the game's two teams")
    for name, rows in sorted(box.items()):
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in ("pos_team", "def_pos_team", "team_id"):
                if key not in row or row[key] is None:
                    continue
                tally.checked += 1
                tid = _tid(row[key])
                if tid is None or tid not in teams:
                    tally.bad.append({"section": name, "column": key, "value": row[key], "game_teams": sorted(teams)})
    return tally.result("box.team_ids_in_game", "every advBoxScore row names one of the game's two teams")


def game_teams(plays: pl.DataFrame) -> set[int]:
    """The two team ids the frame was played by (``pos_team`` and ``def_pos_team``)."""
    teams: set[int] = set()
    for column in ("pos_team", "def_pos_team"):
        if column in plays.columns:
            for value in plays.get_column(column).drop_nulls().unique().to_list():
                tid = _tid(value)
                if tid is not None:
                    teams.add(tid)
    return teams


def evaluate_box(plays: pl.DataFrame, box: dict[str, Any] | None, league: str = "nfl") -> list[Any]:
    """Reconcile every supplied ``advBoxScore`` section against the plays frame.

    Args:
        plays: one game's processed plays frame.
        box: the processor's ``advBoxScore`` dict (``None`` / ``{}`` -> no rules).
        league: ``"nfl"`` or ``"cfb"``; the checked columns are the ones both
            processors build identically, so the league is currently carried for
            symmetry with :func:`~sportsdataverse.validation.pbp_invariants.evaluate`.

    Returns:
        One ``RuleResult`` per ``box.*`` reconciliation rule that could run,
        including rules that did not fire.
    """
    del league  # every checked column is league-identical; see the module docstring
    if not isinstance(plays, pl.DataFrame) or plays.height == 0 or not box:
        return []
    try:
        from sportsdataverse.football.usage_box import _standing_scrimmage

        standing = _standing_scrimmage(plays)
    except Exception:  # noqa: BLE001 -- the builder's own guard; a slim frame is not a finding
        standing = None
    teams = game_teams(plays)
    return [
        _team_totals(plays, box, standing),
        _player_sums(plays, box),
        _rates(box),
        _mirror(box, teams),
        _turnovers(plays, box),
        _drives(plays, box, standing),
        _shares(box),
        _team_ids(box, teams),
    ]
