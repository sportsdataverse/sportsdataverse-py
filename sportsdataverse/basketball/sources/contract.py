"""The ESPN-summary contract a basketball source adapter must satisfy (private, experimental).

An adapter turns a cdn.nba.com / stats.nba.com / stats.ncaa.org payload into the dict
``espn_<league>_pbp(game_id, raw=True)`` returns (ESPN's ``summary`` with its top-level keys
defaulted), so the unmodified ``helper_<league>_pbp`` parser and
:func:`~sportsdataverse.nba.nba_game_page.game_page_frames` consume it. This module is the
checkable half of that contract: the field lists and a validator that names the missing or
wrong fields instead of letting the parser crash or, worse, run to completion on garbage.
Mirrors :mod:`sportsdataverse.football.sources.contract`.

Field levels (each measured by deleting the field from a real ESPN capture and running
``helper_<league>_pbp`` + ``game_page_frames`` on all four leagues):

* ``required`` -- the parser raises without it (``KeyError`` / ``ColumnNotFoundError``).
* ``value`` -- the parser runs but the output is wrong: no team rates without ``header.id``,
  ``boxscore.teams`` or the competitor ``id`` / ``score``; a home-court prior at a neutral site
  without ``neutralSite``; no scoring or player ids without ``scoringPlay`` / ``participants``.
* ``repaired`` -- reconstructed when absent, so absence is a warning. Only
  ``format.regulation.periods`` qualifies: ``game_regulation_periods`` falls back to the
  league's season era.
* ``gop`` -- Game on Paper dereferences it unguarded (``python/nba_game.py`` ``int(side["id"])``,
  ``astro/src/resources/nbaGame.ts`` ``ti.displayName``); missing means a broken page, not a
  parser failure.
* ``gop_soft`` -- read with a fallback, so absence costs a label or the shot chart: status,
  linescores, and the shot coordinates ESPN itself never ships for pre-2016 WBB games.

Competitor ``winner`` is deliberately not checked: the team box needs it, but an in-progress
game may not carry it yet, and the contract must not fail ESPN's own live feed.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

# ponytail: the dotted-path resolver is sport-neutral; reused rather than copied
from sportsdataverse.football.sources.contract import _MISSING, _get

#: Bumped whenever a field or rule changes, so a provenance stamp says which contract passed.
CONTRACT_VERSION = "1"

LEAGUES = ("nba", "wnba", "mbb", "wbb")

_TOP_LEVEL = (
    "header",
    "boxscore",
    "format",
    "gameInfo",
    "leaders",
    "broadcasts",
    "predictor",
    "pickcenter",
    "againstTheSpread",
    "odds",
    "winprobability",
    "article",
    "videos",
    "standings",
    "espnWP",
)

#: Top-level keys ``helper_<league>_pbp`` reads by bracket: each must exist (the value may be empty).
#: ``espn_<league>_pbp(raw=True)`` defaults them; the NBA and WNBA parsers also read ``seasonseries``.
TOP_LEVEL_KEYS: dict[str, tuple[str, ...]] = {
    lg: _TOP_LEVEL + (("seasonseries",) if lg in ("nba", "wnba") else ()) for lg in LEAGUES
}

#: Header and box paths (dotted; ``[i]`` indexes a list). Level per the module docstring.
HEADER_FIELDS: tuple[tuple[str, str], ...] = (
    ("header.id", "value"),
    ("header.season.year", "required"),
    ("header.season.type", "required"),
    ("header.competitions[0].date", "required"),
    ("header.competitions[0].neutralSite", "value"),
    ("header.competitions[0].playByPlaySource", "required"),
    ("header.competitions[0].boxscoreSource", "required"),
    ("header.competitions[0].status.type.completed", "gop_soft"),
    ("format.regulation.periods", "repaired"),
    ("boxscore.teams", "value"),
    ("boxscore.players", "required"),
)

#: Per-competitor paths (``header.competitions[0].competitors[i]``).
COMPETITOR_FIELDS: tuple[tuple[str, str], ...] = (
    ("homeAway", "required"),
    ("id", "value"),
    ("score", "value"),
    ("team.id", "required"),
    ("team.name", "required"),
    ("team.location", "required"),
    ("team.abbreviation", "required"),
    ("team.displayName", "gop"),
    ("linescores", "gop_soft"),
)

#: Per-play paths (``plays[]``).
PLAY_FIELDS: tuple[tuple[str, str], ...] = (
    ("id", "required"),
    ("sequenceNumber", "required"),
    ("period.number", "required"),
    ("clock.displayValue", "required"),
    ("type.text", "required"),
    ("text", "required"),
    ("homeScore", "required"),
    ("awayScore", "required"),
    ("shootingPlay", "required"),
    ("team.id", "required"),
    ("type.id", "value"),
    ("scoringPlay", "value"),
    ("scoreValue", "value"),
    ("participants", "value"),
    ("coordinate.x", "gop_soft"),
    ("coordinate.y", "gop_soft"),
)

#: ``game_page_frames`` columns / keys (``fnmatch`` patterns) whose value an alternate source does not
#: reproduce against ESPN on the same game, by ``(league, source)``; stamped in provenance. Measured,
#: never guessed: ``tests/basketball/test_sources_live.py`` recomputes the set on the fixture games and
#: fails unless every pattern matches a measured gap and every gap is covered. Shot coordinates are not
#: listed: liveData's are finer than ESPN's whole feet (checked within 1 ft).
_LIVE_LOSSY: tuple[str, ...] = (
    # no odds on liveData: the prior is home court, never ESPN's spread
    "game_spread",
    "game_spread_available",
    "home_favorite",
    "home_team_spread",
    "prior",
    "prior_source",
    "pregame_home_prob",
    "home_win_prob",
    "wp_delta",
    # no neutral-site flag (the NBA Cup final in Las Vegas reads as a home game)
    "neutral_site",
    # the NBA's own stat corrections (a rebound or block credited to another player) and id-map misses
    "athlete_id_*",
    "team_id",
    # ESPN rows liveData does not carry (replay reviews), and the NBA's split double fouls
    "game_play_number",
    "end_*_seconds_remaining",
    # wording and presentation
    "text",
    "short_description",
    "type_id",
    "type_text",
    "wallclock",
    "reason",
    "athlete_short_name",
    "athlete_headshot_href",
    "athlete_position_*",
    "*_team_abbrev",
    "*team_abbreviation",
    "*_color",
    "*_logo",
    "*_slug",
    "*_uid",
    "flagrant_fouls",
    "lead_percentage",
)
KNOWN_LOSSY: dict[tuple[str, str], tuple[str, ...]] = {("nba", "live"): _LIVE_LOSSY, ("wnba", "live"): _LIVE_LOSSY}

#: ``MM:SS`` with optional tenths, or ESPN's bare ``SS.t`` under a minute (the parser prefixes ``0:``).
_CLOCK = re.compile(r"^\d{1,2}(:[0-5]\d)?(\.\d+)?$")


@dataclass
class ContractReport:
    """Result of :func:`_validate_summary`.

    Returns:
        A dataclass with these fields.

        | field | type | description |
        |---|---|---|
        | league | str | ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` |
        | version | str | :data:`CONTRACT_VERSION` the summary was checked against |
        | n_plays | int | rows in ``plays`` |
        | missing | list[str] | ``required``- and ``value``-level paths absent from every row (both fail ``ok``) |
        | invalid | list[str] | value rules violated (foreign team ids, bad clock, all-null column, ...) |
        | gop_missing | list[str] | ``gop``-level paths absent (broken page, parser unaffected) |
        | warnings | list[str] | degradations that do not fail the contract |
        | null_rate | dict[str, float] | per row path present: rows whose value is absent or null, over all rows |
    """

    league: str
    version: str = CONTRACT_VERSION
    n_plays: int = 0
    missing: list[str] = field(default_factory=list)
    invalid: list[str] = field(default_factory=list)
    gop_missing: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    null_rate: dict[str, float] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """True when the parser can run and its output is not known-garbage."""
        return not self.missing and not self.invalid

    @property
    def gop_ok(self) -> bool:
        """True when, additionally, Game on Paper renders the result."""
        return self.ok and not self.gop_missing

    def summary(self) -> dict[str, Any]:
        """Compact JSON-able view for provenance stamps and logs."""
        out = asdict(self)
        out["ok"] = self.ok
        out["gop_ok"] = self.gop_ok
        out["null_rate"] = {k: round(v, 4) for k, v in self.null_rate.items() if v > 0}
        return out


def _bucket(report: ContractReport, level: str, path: str) -> None:
    if level == "gop":
        report.gop_missing.append(path)
    elif level == "gop_soft":
        report.warnings.append(f"{path} absent: Game on Paper falls back to a placeholder")
    elif level == "repaired":
        report.warnings.append(f"{path} absent: the regulation falls back to the league's season era")
    else:
        report.missing.append(path)


def _check_rows(report: ContractReport, rows: list[dict], fields: tuple[tuple[str, str], ...], prefix: str) -> None:
    """A path is *missing* when no row carries it; present but null on every row is *invalid*."""
    n = len(rows)
    for path, level in fields:
        present = [v for v in (_get(r, path) for r in rows) if v is not _MISSING]
        if not present:
            _bucket(report, level, f"{prefix}.{path}")
        elif level in ("required", "value"):
            rate = (n - sum(v is not None for v in present)) / n
            report.null_rate[f"{prefix}.{path}"] = rate
            if rate >= 1.0:
                report.invalid.append(f"{prefix}.{path}: present but null on every row")


def _check_competitors(report: ContractReport, competitors: list) -> set[str]:
    """Competitor value rules; returns the header team ids every play team id must belong to."""
    sides = [c.get("homeAway") for c in competitors if isinstance(c, dict)]
    if sorted(str(s) for s in sides if s) != ["away", "home"]:
        report.invalid.append(f"competitors.homeAway must be exactly {{home, away}}, got {sides}")
    team_ids: set[str] = set()
    for i, c in enumerate(competitors):
        tid = _get(c, "team.id")
        if tid is _MISSING:
            continue
        try:
            if int(tid) < 0:
                report.invalid.append(f"competitors[{i}].team.id {tid!r} is negative (ESPN TBD placeholder)")
        except (TypeError, ValueError):
            report.invalid.append(f"competitors[{i}].team.id {tid!r} is not int-castable")
        else:
            team_ids.add(str(tid))
    return team_ids


def _check_plays(report: ContractReport, team_ids: set[str], plays: list[dict]) -> None:
    """Play value rules the parser and the win-probability model silently depend on."""
    ids: list[int] = []
    for p in plays:
        try:
            ids.append(int(p["id"]))
        except (KeyError, TypeError, ValueError):
            report.invalid.append(f"play id {p.get('id')!r} is not int-castable")
            break
    else:
        # ESPN's own feeds carry late-inserted plays (a larger id in an earlier slot): report, don't fail
        if violations := sum(b <= a for a, b in zip(ids, ids[1:], strict=False)):
            report.warnings.append(f"play ids not increasing in feed order on {violations} rows (late inserts?)")
        if dupes := len(ids) - len(set(ids)):
            report.warnings.append(f"{dupes} duplicate play ids")

    bad_clock = sum(
        1 for p in plays if (v := _get(p, "clock.displayValue")) not in (_MISSING, None) and not _CLOCK.match(str(v))
    )
    if bad_clock:
        report.invalid.append(f'clock.displayValue not "MM:SS" or "SS.t" on {bad_clock} plays')

    if team_ids:
        # the win-probability model needs the offensive team and the home team in one id space
        foreign = {
            str(v) for p in plays if (v := _get(p, "team.id")) not in (_MISSING, None) and str(v) not in team_ids
        }
        if foreign:
            report.invalid.append(f"play team ids not in header competitors: {sorted(foreign)}")


def _validate_summary(summary: Any, league: str) -> ContractReport:
    """Check a candidate ESPN-shaped basketball summary against the parser + Game on Paper contract.

    Args:
        summary: The dict an adapter produced, or ``espn_<league>_pbp(game_id, raw=True)``.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.

    Returns:
        :class:`ContractReport`. ``report.ok`` means ``helper_<league>_pbp`` and
        ``game_page_frames`` can run it; ``report.gop_ok`` means Game on Paper can render the
        result. Field lists: :data:`TOP_LEVEL_KEYS`, :data:`HEADER_FIELDS`,
        :data:`COMPETITOR_FIELDS`, :data:`PLAY_FIELDS`.

    Raises:
        ValueError: unknown league.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_pbp import espn_nba_pbp
            from sportsdataverse.basketball.sources.contract import _validate_summary
            report = _validate_summary(espn_nba_pbp(401766458, raw=True), "nba")
            report.ok, report.missing, report.invalid
    """
    if league not in LEAGUES:
        raise ValueError(f"league must be one of {LEAGUES}, got {league!r}")
    report = ContractReport(league=league)
    if not isinstance(summary, dict):
        report.missing.append("<summary is not a dict>")
        return report

    report.missing.extend(k for k in TOP_LEVEL_KEYS[league] if k not in summary)
    for path, level in HEADER_FIELDS:
        value = _get(summary, path)
        if value is _MISSING:
            _bucket(report, level, path)
        elif value is None and level in ("required", "value"):
            # e.g. a null neutralSite reads as "not neutral" and gives a neutral-site game home court
            report.invalid.append(f"{path}: present but null")

    competitors = _get(summary, "header.competitions[0].competitors")
    if not isinstance(competitors, list) or not competitors:
        report.missing.append("header.competitions[0].competitors")
        competitors = []
    else:
        _check_rows(report, [c for c in competitors if isinstance(c, dict)], COMPETITOR_FIELDS, "competitors[]")

    raw_plays = summary.get("plays")
    plays = [p for p in raw_plays if isinstance(p, dict)] if isinstance(raw_plays, list) else []
    report.n_plays = len(plays)
    if not plays:
        report.missing.append("plays[]")
    else:
        _check_rows(report, plays, PLAY_FIELDS, "plays[]")

    pbp_source = _get(summary, "header.competitions[0].playByPlaySource")
    if pbp_source == "none":
        report.warnings.append('playByPlaySource == "none": the parser skips every play')
    _check_plays(report, _check_competitors(report, competitors), plays)
    return report
