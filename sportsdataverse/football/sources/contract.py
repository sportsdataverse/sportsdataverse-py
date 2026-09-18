"""The ESPN-summary contract an alternate-source adapter must satisfy (private, experimental).

An adapter turns a Shield / CBS / Yahoo / Fox / NCAA payload into a dict shaped like the
ESPN ``summary`` endpoint so the unmodified ``NFLPlayProcess`` / ``CFBPlayProcess`` can
consume it through ``espn_{nfl,cfb}_pbp(summary=)``. This module is the checkable half
of that contract: the field lists (with the reason each field matters) and a validator
that returns a structured report instead of letting the processor crash or, worse, run
to completion on garbage.

Field levels:

* ``required`` -- the processor raises or yields zero plays without it
  (``ColumnNotFoundError`` / ``KeyError`` / ``NoDataError``).
* ``value`` -- the processor runs but the output is wrong (possession, field position,
  clock, timeouts). Checked for presence and for an all-null column.
* ``repaired`` -- a ``value`` field the processors *reconstruct* when it is absent or null, so
  its absence is a warning, never a contract failure. Only ``plays[].end.team.id`` qualifies:
  both processors fill it from the next play's start team (``cfb_pbp.py:1501-1535``,
  ``nfl_pbp.py:703``), and ESPN's own pre-2010 CFB feeds ship no ``end.team`` at all -- game
  252532751 (2005) processes to 163 plays with zero null ``end.team.id`` and zero null ``EPA``.
* ``gop`` -- Game on Paper dereferences it unguarded (``python/app.py:199-322``
  bracket reads, ``GamePage.astro:72-95`` header fields); missing means an HTTP 404 or a
  blank header, not a processor failure.

Evidence for every level lives in
``background-research/2026-09-16-cfb-alt-sources/A_cfbplayprocess_contract.md`` (§1b, §1c,
§4b) and ``2026-09-16-nfl-shield-live/{A_nflplayprocess_contract,D_gop_nfl_consumption}.md``.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

LEAGUES = ("nfl", "cfb")

#: Header paths (dotted; ``[i]`` indexes a list). Level per the module docstring.
HEADER_FIELDS: tuple[tuple[str, str], ...] = (
    ("header.id", "gop"),
    ("header.season.year", "required"),
    ("header.week", "required"),  # key must exist; the value may be null
    ("header.competitions[0].date", "gop"),
    ("header.competitions[0].neutralSite", "gop"),
    ("header.competitions[0].playByPlaySource", "required"),
    ("header.competitions[0].boxscoreSource", "required"),
    ("header.competitions[0].status.type.completed", "required"),
    ("header.competitions[0].status.type.name", "gop"),
    ("header.competitions[0].status.type.state", "gop"),
    ("header.competitions[0].status.type.detail", "gop"),
    ("header.competitions[0].status.type.description", "gop"),
    ("drives.previous", "required"),
)

#: Per-competitor paths (``header.competitions[0].competitors[i]``).
COMPETITOR_FIELDS: tuple[tuple[str, str], ...] = (
    ("homeAway", "required"),
    ("team.id", "required"),
    ("team.location", "required"),
    ("team.abbreviation", "required"),
    ("team.name", "value"),  # the mascot; "" matches every Timeout row (see _check_values)
    ("team.displayName", "gop"),
    ("team.color", "gop"),
    ("score", "gop"),
    ("linescores", "gop"),
)

#: Per-play paths (``drives.previous[].plays[]`` and ``drives.current.plays[]``).
PLAY_FIELDS: tuple[tuple[str, str], ...] = (
    ("id", "required"),
    ("sequenceNumber", "value"),
    ("period.number", "value"),
    ("clock.displayValue", "value"),
    ("type.text", "value"),
    ("type.id", "gop"),
    ("type.abbreviation", "gop"),  # key only: ESPN itself emits null on Sack / Pass Incompletion
    ("text", "value"),
    ("start.team.id", "value"),
    ("start.down", "value"),
    ("start.distance", "value"),
    ("start.yardLine", "value"),  # hidden gate: yardsToEndzone is dropped when yardLine is null
    ("start.yardsToEndzone", "value"),
    ("end.team.id", "repaired"),  # both processors fill it from the next play's start team
    ("end.down", "required"),
    ("end.distance", "required"),
    ("end.yardLine", "value"),
    ("end.yardsToEndzone", "required"),
    ("homeScore", "required"),
    ("awayScore", "required"),
    ("scoringPlay", "value"),
    ("statYardage", "required"),
)

#: Per-drive meta paths (``drives.previous[]``).
DRIVE_FIELDS: tuple[tuple[str, str], ...] = (
    ("id", "required"),
    ("start.yardLine", "value"),
    ("team.shortDisplayName", "gop"),  # Drives panel / Latest strip
    ("displayResult", "gop"),
    ("result", "gop"),
    ("description", "gop"),
)

#: Columns the processor derives from text only; every alternate source loses some of
#: them (scorecards, "Lost vs ESPN"). Stage 3 measures these per source x season.
KNOWN_LOSSY: dict[tuple[str, str], tuple[str, ...]] = {
    ("nfl", "shield"): (),  # ESPN's NFL feed IS the GSIS feed re-skinned; nothing lost by construction
    ("nfl", "cbs"): ("air_yards", "yards_after_catch", "cp", "cpoe", "shotgun", "*_player_id"),
    ("nfl", "yahoo"): (
        "air_yards",
        "yards_after_catch",
        "cp",
        "cpoe",
        "shotgun",
        "no_huddle",
        "punt_fair_catch",
        "pass_breakup_player_name",
        "punt_return_player_name",
        "*_player_id",
    ),
    ("nfl", "fox"): (
        "rusher_player_name",
        "receiver_player_name",
        "sack_player_name",
        "air_yards",
        "cp",
        "cpoe",
        "*_player_id",
        "advBoxScore.espn_team",
        "advBoxScore.espn_players",
    ),
    ("cfb", "cbs"): (
        "yds_receiving",
        "yds_rushed",
        "yds_kickoff",
        "yds_punted",
        "receiver_player_name",
        "kickoff_player_name",
        "pass_breakup_player_name",
        "fumble_*_player_name",
        "firstD_by_penalty",
        "*_player_id",
    ),
    ("cfb", "yahoo"): (
        "yds_rushed",
        "yds_receiving",
        "air_yards",
        "yards_after_catch",
        "yds_fg",
        "fg_kicker_player_name",
        "yds_kickoff",
        "kickoff_player_name",
        "punt_return_player_name",
        "interception_player_name",
        "penalty_no_play",
        "penalty_declined",
        "firstD_by_penalty",
        "xp_kicker_player_name",
        "fumble_forced_player_name",
        "fumble_recovered_player_name",
        "pass_breakup_player_name",
        "*_player_id",
    ),
    ("cfb", "ncaa"): (
        "sack_player_name",
        "fg_kicker_player_name",
        "kickoff_return_player_name",
        "yds_receiving",  # classic grammar only
        "*_player_id",
    ),
    ("cfb", "fox"): (
        "receiver_player_name",
        "passer_player_name",  # half the rows
        "yds_punted",
        "yds_kickoff",
        "kickoff_player_name",
        "pass_breakup_player_name",
        "kickoff_return_player_name",
        "*_player_id",
        "scoringType.*",
        "pointAfterAttempt.*",
    ),
}

_CLOCK = re.compile(r"^\d{1,2}:\d{2}$")
_MISSING = object()


@dataclass
class ContractReport:
    """Result of :func:`_validate_summary`.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | league | str | ``"nfl"`` or ``"cfb"`` |
        | n_plays | int | plays across ``drives.previous`` + ``drives.current`` |
        | n_drives | int | drives across both groupings |
        | missing | list[str] | ``required``- and ``value``-level paths absent from every row (both fail ``ok``) |
        | (``repaired``-level absence goes to ``warnings``: the processor reconstructs the field) | | |
        | invalid | list[str] | value rules violated (empty mascot, foreign team ids, bad clock, all-null column, ...) |
        | gop_missing | list[str] | ``gop``-level paths absent (page 404 / blank header, processor unaffected) |
        | warnings | list[str] | degradations that do not fail the contract (sparse feed, competitor order, late-inserted or duplicate play ids) |
        | null_rate | dict[str, float] | per row-collection path present: rows whose value is absent or null, over all rows |
    """

    league: str
    n_plays: int = 0
    n_drives: int = 0
    missing: list[str] = field(default_factory=list)
    invalid: list[str] = field(default_factory=list)
    gop_missing: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    null_rate: dict[str, float] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """True when the processor can run and its output is not known-garbage."""
        return not self.missing and not self.invalid

    @property
    def gop_ok(self) -> bool:
        """True when, additionally, Game on Paper renders the result without a 404."""
        return self.ok and not self.gop_missing

    def summary(self) -> dict[str, Any]:
        """Compact JSON-able view for provenance stamps and logs."""
        out = asdict(self)
        out["ok"] = self.ok
        out["gop_ok"] = self.gop_ok
        out["null_rate"] = {k: round(v, 4) for k, v in self.null_rate.items() if v > 0}
        return out


def _get(obj: Any, path: str) -> Any:
    """Resolve a dotted path with ``[i]`` list indexes; ``_MISSING`` when any hop is absent."""
    cur = obj
    for token in path.split("."):
        m = re.fullmatch(r"([^\[]+)(?:\[(\d+)\])?", token)
        if m is None:
            return _MISSING
        key, idx = m.group(1), m.group(2)
        if not isinstance(cur, dict) or key not in cur:
            return _MISSING
        cur = cur[key]
        if idx is not None:
            if not isinstance(cur, list) or int(idx) >= len(cur):
                return _MISSING
            cur = cur[int(idx)]
    return cur


def _iter_drives(summary: dict) -> list[dict]:
    drives = summary.get("drives")
    if not isinstance(drives, dict):
        return []
    out = [d for d in drives.get("previous") or [] if isinstance(d, dict)]
    cur = drives.get("current")
    if isinstance(cur, dict) and cur:
        out.append(cur)
    return out


def _iter_plays(drives: list[dict]) -> list[dict]:
    return [p for d in drives for p in (d.get("plays") or []) if isinstance(p, dict)]


def _bucket(report: ContractReport, level: str, path: str) -> None:
    if level == "gop":
        report.gop_missing.append(path)
    elif level == "repaired":
        # absent on every row is the normal shape of a pre-2010 ESPN CFB feed; the
        # processor reconstructs it, so this must not fail the contract
        report.warnings.append(f"{path} absent: the processor fills it from the next play's start team")
    else:
        report.missing.append(path)


def _check_rows(report: ContractReport, rows: list[dict], fields: tuple[tuple[str, str], ...], prefix: str) -> None:
    """Row-collection check: a path is *missing* when no row carries it; a null rate is recorded per path.

    An all-null column is ``invalid`` at both ``required`` and ``value`` level: the key exists
    so the processor does not raise, it just computes on nothing (an all-null ``statYardage``
    zeroes every yardage column, an all-null ``homeScore`` erases the score after the play).
    """
    if not rows:
        return
    n = len(rows)
    for path, level in fields:
        values = [_get(r, path) for r in rows]
        present = [v for v in values if v is not _MISSING]
        if not present:
            _bucket(report, level, f"{prefix}.{path}")
            continue
        if level in ("value", "required", "repaired"):
            nulls = n - sum(v is not None for v in present)
            rate = nulls / n
            report.null_rate[f"{prefix}.{path}"] = rate
            if rate >= 1.0 and level != "repaired":
                report.invalid.append(f"{prefix}.{path}: present but null on every row")


def _check_values(report: ContractReport, summary: dict, plays: list[dict]) -> None:
    """Value rules the processor silently depends on."""
    competitors = _get(summary, "header.competitions[0].competitors")
    team_ids: set[str] = set()
    if isinstance(competitors, list):
        sides = [c.get("homeAway") for c in competitors if isinstance(c, dict)]
        if sorted(s for s in sides if s) != ["away", "home"]:
            report.invalid.append(f"competitors.homeAway must be exactly {{home, away}}, got {sides}")
        elif sides[0] != "home":
            report.warnings.append("competitors[0] is not the home team; GOP assumes home-first (GamePage.astro:74-77)")
        for i, c in enumerate(competitors):
            team = c.get("team") if isinstance(c, dict) else None
            if not isinstance(team, dict):
                continue
            tid = team.get("id")
            try:
                if int(tid) < 0:
                    report.invalid.append(f"competitors[{i}].team.id {tid!r} is negative (ESPN TBD placeholder)")
            except (TypeError, ValueError):
                report.invalid.append(f"competitors[{i}].team.id {tid!r} is not int-castable")
            else:
                team_ids.add(str(tid))
            if "name" in team and not team.get("name"):
                report.invalid.append(f"competitors[{i}].team.name is empty: every Timeout row would match both teams")

    pbp_source = _get(summary, "header.competitions[0].playByPlaySource")
    if pbp_source == "none":
        report.warnings.append('playByPlaySource == "none": the processor skips all processing')

    completed = _get(summary, "header.competitions[0].status.type.completed")
    if completed is True and 0 < len(plays) < 50:
        report.warnings.append(f"completed game with {len(plays)} plays: corrupt_pbp_check() short-circuits below 50")

    ids: list[int] = []
    for p in plays:
        try:
            ids.append(int(p["id"]))
        except (KeyError, TypeError, ValueError):
            report.invalid.append(f"play id {p.get('id')!r} is not int-castable")
            break
    if ids:
        # ESPN's own feeds carry late-inserted plays (larger id, earlier slot); the processor
        # sorts by id and reorders them (CFB season >= 2014). Report, don't fail.
        violations = sum(b <= a for a, b in zip(ids, ids[1:]))
        if violations:
            report.warnings.append(f"play ids not increasing in feed order on {violations} rows (late inserts?)")
        dupes = len(ids) - len(set(ids))
        if dupes:
            report.warnings.append(f"{dupes} duplicate play ids (the dedupe step keeps the first)")

    bad_clock = sum(
        1 for p in plays if (v := _get(p, "clock.displayValue")) not in (_MISSING, None) and not _CLOCK.match(str(v))
    )
    if bad_clock:
        report.invalid.append(f'clock.displayValue not "MM:SS" on {bad_clock} plays')

    if team_ids:
        foreign = {
            str(v)
            for p in plays
            for side in ("start", "end")
            if (v := _get(p, f"{side}.team.id")) not in (_MISSING, None) and str(v) not in team_ids
        }
        if foreign:
            report.invalid.append(f"play team ids not in header competitors: {sorted(foreign)}")


def _validate_summary(summary: Any, league: str) -> ContractReport:
    """Check a candidate ESPN-shaped summary against the processor + GOP contract.

    Args:
        summary: The dict an adapter produced (or a real ESPN summary payload).
        league: ``"nfl"`` or ``"cfb"``.

    Returns:
        :class:`ContractReport`. ``report.ok`` means the processor can run it; ``report.gop_ok``
        means Game on Paper can render the result. Field lists: :data:`HEADER_FIELDS`,
        :data:`COMPETITOR_FIELDS`, :data:`PLAY_FIELDS`, :data:`DRIVE_FIELDS`.

    Raises:
        ValueError: unknown league.
    """
    if league not in LEAGUES:
        raise ValueError(f"league must be one of {LEAGUES}, got {league!r}")
    report = ContractReport(league=league)
    if not isinstance(summary, dict):
        report.missing.append("<summary is not a dict>")
        return report

    for path, level in HEADER_FIELDS:
        if _get(summary, path) is _MISSING:
            _bucket(report, level, path)

    competitors = _get(summary, "header.competitions[0].competitors")
    if not isinstance(competitors, list) or not competitors:
        report.missing.append("header.competitions[0].competitors")
    else:
        _check_rows(report, competitors, COMPETITOR_FIELDS, "competitors[]")

    drives = _iter_drives(summary)
    plays = _iter_plays(drives)
    report.n_drives, report.n_plays = len(drives), len(plays)
    if drives:
        _check_rows(report, drives, DRIVE_FIELDS, "drives[]")
    if not plays:
        report.missing.append("drives[].plays[]")
    else:
        _check_rows(report, plays, PLAY_FIELDS, "plays[]")

    _check_values(report, summary, plays)
    return report
