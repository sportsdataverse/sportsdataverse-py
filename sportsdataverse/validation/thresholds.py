"""Validation tolerances, importable without PyYAML.

``thresholds.yaml`` ships in the wheel next to the rules, but PyYAML is a
**dev-only** dependency of sdv-py -- so a data repo running the V2 gate off the
published wheel could not read it and hard-coded its own ``NULL_RATE_WARN`` /
``MEAN_SHIFT_WARN``. This module reads the packaged file with the standard
library alone and exposes the values, so the YAML stays the one human-edited
source of truth and nothing is duplicated:

* :data:`DEFAULTS` -- the ``default`` section.
* :func:`for_league` -- ``default`` with a league's own section overlaid.
* :data:`NULL_RATE_WARN`, :data:`MEAN_SHIFT_WARN`,
  :data:`EXTRACTION_COVERAGE_FLOOR` -- the three a caller usually wants by name.

The reader handles exactly the grammar ``thresholds.yaml`` uses -- comments,
blank lines, a top-level ``section:`` (or ``section: {}``) and indented
``name: <number>`` entries -- and **raises** on anything else rather than
guessing, so a YAML edit it cannot read is a loud failure instead of a silently
wrong tolerance. ``tests/validation/test_thresholds.py`` pins it against
PyYAML's own parse of the same file.

Example:
    Read a league's tolerances::

        from sportsdataverse.validation import thresholds
        thresholds.for_league("nfl")["null_rate_warn"]
        thresholds.NULL_RATE_WARN
"""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path

__all__ = [
    "DEFAULTS",
    "EXTRACTION_COVERAGE_FLOOR",
    "MEAN_SHIFT_WARN",
    "NULL_RATE_WARN",
    "THRESHOLDS",
    "for_league",
    "parse",
]

#: The packaged file, read through ``importlib.resources`` so it resolves in a wheel.
PATH = Path(str(files("sportsdataverse.validation") / "thresholds.yaml"))


def _number(text: str, line_no: int) -> float:
    try:
        return float(text)
    except ValueError:
        raise ValueError(f"thresholds.yaml line {line_no}: {text!r} is not a number") from None


def parse(text: str) -> dict[str, dict[str, float]]:
    """``{section: {name: value}}`` from the flat YAML the tolerances are written in.

    Args:
        text: the file's contents.

    Returns:
        dict[str, dict[str, float]]: one entry per top-level section.

    Raises:
        ValueError: If a line is not a comment, a blank, a top-level
            ``section:`` / ``section: {}`` or an indented ``name: <number>`` --
            the reader never guesses at a construct it was not written for.
    """
    out: dict[str, dict[str, float]] = {}
    section: dict[str, float] | None = None
    for line_no, raw in enumerate(text.splitlines(), 1):
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if ":" not in line:
            raise ValueError(f"thresholds.yaml line {line_no}: expected 'key: value'; got {raw!r}")
        key, _, value = line.partition(":")
        key, value = key.strip(), value.strip()
        if line[:1].strip():  # top-level: a section header, optionally an inline empty map
            if value not in ("", "{}"):
                raise ValueError(f"thresholds.yaml line {line_no}: top-level {key!r} must be a mapping; got {value!r}")
            section = out.setdefault(key, {})
            continue
        if section is None:
            raise ValueError(f"thresholds.yaml line {line_no}: indented {key!r} before any section")
        section[key] = _number(value, line_no)
    return out


#: Every section of the packaged file, as written.
THRESHOLDS: dict[str, dict[str, float]] = parse(PATH.read_text(encoding="utf-8"))

#: The ``default`` section -- the tolerances that apply to every league.
DEFAULTS: dict[str, float] = dict(THRESHOLDS.get("default", {}))


def for_league(league: str) -> dict[str, float]:
    """The tolerances for one league: :data:`DEFAULTS` with its own section overlaid.

    Args:
        league: ``"nfl"``, ``"cfb"``, or any other section of the file. A league
            with no section of its own gets the defaults.

    Returns:
        dict[str, float]: threshold name -> value, the league's values winning.

    Example:
        The gate's null-rate tolerance for NFL::

            from sportsdataverse.validation.thresholds import for_league
            for_league("nfl")["null_rate_warn"]
    """
    merged = dict(DEFAULTS)
    merged.update(THRESHOLDS.get(str(league).lower(), {}))
    return merged


#: A published column may be null on at most this share of rows before the sweep warns.
NULL_RATE_WARN: float = DEFAULTS["null_rate_warn"]
#: A column's mean may move by at most this share against the prior release before the sweep warns.
MEAN_SHIFT_WARN: float = DEFAULTS["mean_shift_warn"]
#: An extracted column must be populated on at least this share of the rows it applies to.
EXTRACTION_COVERAGE_FLOOR: float = DEFAULTS["extraction_coverage_floor"]
