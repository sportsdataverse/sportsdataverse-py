"""Parsers for the PFF Premium Stats (``premium.pff.com/api/v1``) response envelopes.

Every PFF response is a single-key envelope whose key is the report slug:

* **facet reports** -> ``{report_slug: [rows]}`` (a leaderboard) -> one tidy frame,
* **matrix reports** -> ``{receiving_coverage_stats: {defenders, receivers, versus}}`` ->
  a dict of three sub-frames (never inlined into one flat table),
* **player-detail reports** -> ``{report_slug: {subject, week_totals, weeks}}`` -> the
  per-week long frame (:func:`parse_pff_player_detail`),
* **meta singletons** -> ``{leagues|games|players|...: [rows]}``; the ``teams`` payload is
  multi-key (``{franchise_groups, games, teams}``) -> a dict of frames.

Contract (shared with every other sdv-py parser): polars by default, pandas via
``return_as_pandas=True``; empty / malformed payloads return a zero-row frame rather than
raising; columns are snake-cased via :func:`sportsdataverse.dl_utils.underscore`.

**ID / dtype discipline:** ``player_id`` / ``franchise_id`` / ``league_id`` / ``game_id`` are
integers (``Int64``) and are cast as such; ``jersey_number`` is a zero-padded string and is
kept ``Utf8`` (``"09"`` must not collapse to ``9``). List/dict-valued cells are JSON-stringified
so polars accepts a uniform schema.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import polars as pl

from sportsdataverse.dl_utils import underscore

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import
    import pandas as pd

__all__ = ["parse_pff_report", "parse_pff_player_detail", "parse_pff_matrix"]

_MATRIX_KEYS = {"defenders", "receivers", "versus"}

# columns that are integer join keys / ids and must be kept Int64 (never float, never str)
_ID_COLS = (
    "player_id",
    "franchise_id",
    "league_id",
    "season_id",
    "game_id",
    "away_franchise_id",
    "home_franchise_id",
    "player_franchise_id",
    "coverage_player_id",
    "defender_player_id",
    "receiver_player_id",
    "stadium_id",
    "id",
)


def _scalarize(value: Any) -> Any:
    """JSON-stringify a list/dict cell; pass scalars through unchanged."""
    return json.dumps(value, default=str, sort_keys=True) if isinstance(value, (list, dict)) else value


def _frame(rows: Any) -> pl.DataFrame:
    """Build a tidy polars frame from a list of row dicts (or scalars).

    Applies the id/jersey dtype discipline and snake-cases columns. Returns a zero-row
    frame for an empty / falsy ``rows``.
    """
    if not rows:
        return pl.DataFrame()
    if not isinstance(rows, list):
        return pl.DataFrame()
    if not isinstance(rows[0], dict):
        # a list of scalars (e.g. player seasons -> [2025, 2024, ...])
        return pl.DataFrame({"value": [_scalarize(r) for r in rows]})
    norm = [{k: _scalarize(v) for k, v in row.items()} for row in rows]
    df = pl.DataFrame(norm, infer_schema_length=None)
    df = df.rename({c: underscore(c) for c in df.columns})
    casts = [
        pl.col(idc).cast(pl.Int64, strict=False) for idc in _ID_COLS if idc in df.columns and df.schema[idc] != pl.Utf8
    ]
    if "jersey_number" in df.columns:
        casts.append(pl.col("jersey_number").cast(pl.Utf8))
    if casts:
        df = df.with_columns(casts)
    return df


def _maybe_pandas(df: pl.DataFrame, return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    return df.to_pandas() if return_as_pandas else df


def parse_pff_report(
    raw: dict,
    report: Optional[str] = None,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame", dict]:
    """Parse a PFF facet / matrix / singleton response envelope into tidy frame(s).

    Args:
        raw: Raw JSON response dict from premium.pff.com. Malformed / empty payloads
            return a zero-row frame rather than raising.
        report: Envelope key to select. When ``None`` and the payload has exactly one
            top-level key, that key is unwrapped automatically; when the single value is a
            matrix object, the three sub-frames are returned; when the payload is multi-key
            (``teams``), a ``dict`` of frames over every list-valued key is returned.
        return_as_pandas: Return pandas frame(s) instead of polars. Defaults to ``False``.

    Returns:
        * ``pl.DataFrame`` (or pandas) for a single flat report,
        * ``dict[str, pl.DataFrame]`` for matrix reports and multi-key singletons,
        * a zero-row frame on empty / malformed input.

    Example:
        Quick start::

            import json
            from sportsdataverse.nfl.pff_parsers import parse_pff_report

            raw = json.load(open("passing_summary.json"))
            df = parse_pff_report(raw)
            print(df.select(["player_id", "grades_offense"]).head())

        Multi-key singleton (teams)::

            frames = parse_pff_report(json.load(open("teams.json")))
            teams = frames["teams"]

        See Also:
            * `nflfastR`_ -- NFL play-by-play + EPA/WPA context for the grades
            * `nflverse`_ -- NFL data ecosystem

        .. _nflfastR: https://www.nflfastr.com
        .. _nflverse: https://nflverse.nflverse.com
    """
    if not isinstance(raw, dict) or not raw:
        return _maybe_pandas(pl.DataFrame(), return_as_pandas)

    if report is not None:
        val = raw.get(report)
        if isinstance(val, dict) and _MATRIX_KEYS <= set(val):
            return parse_pff_matrix({report: val}, return_as_pandas=return_as_pandas)
        return _maybe_pandas(_frame(val if isinstance(val, list) else []), return_as_pandas)

    keys = list(raw.keys())
    if len(keys) == 1:
        val = raw[keys[0]]
        if isinstance(val, dict) and _MATRIX_KEYS <= set(val):
            return parse_pff_matrix(raw, return_as_pandas=return_as_pandas)
        if isinstance(val, list):
            return _maybe_pandas(_frame(val), return_as_pandas)
        # a single non-list, non-matrix dict value (e.g. a player-detail envelope routed to
        # the generic parser) -> zero-row frame; use parse_pff_player_detail for those.
        return _maybe_pandas(pl.DataFrame(), return_as_pandas)

    # multi-key singleton (teams -> {franchise_groups, games, teams})
    out = {k: _maybe_pandas(_frame(v), return_as_pandas) for k, v in raw.items() if isinstance(v, list)}
    if not out:
        return _maybe_pandas(pl.DataFrame(), return_as_pandas)
    return out


def parse_pff_matrix(
    raw: dict,
    report: Optional[str] = None,
    *,
    return_as_pandas: bool = False,
) -> Dict[str, Union[pl.DataFrame, "pd.DataFrame"]]:
    """Parse a PFF coverage-matrix report into its three sub-frames.

    The ``receiving_coverage_stats`` report (from ``/defense/coverage_matchup`` and
    ``/receiving/coverage``) is a ``defenders`` x ``receivers`` coverage grid plus a ``versus``
    long form. It is surfaced as-is -- three sub-frames -- never collapsed into one table.

    Args:
        raw: Raw JSON response dict (single matrix-key envelope).
        report: Envelope key to select; auto-detected when ``None``.
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"defenders": frame, "receivers": frame, "versus": frame}`` (each possibly zero-row).

    Example:
        Quick start::

            from sportsdataverse.nfl.pff_parsers import parse_pff_matrix

            out = parse_pff_matrix(json.load(open("coverage_matrix.json")))
            print(out["versus"].columns)
    """
    obj: Dict[str, Any] = {}
    if isinstance(raw, dict) and raw:
        if report is not None and isinstance(raw.get(report), dict):
            obj = raw[report]
        else:
            for val in raw.values():
                if isinstance(val, dict) and _MATRIX_KEYS <= set(val):
                    obj = val
                    break
    return {
        name: _maybe_pandas(_frame(obj.get(name) or []), return_as_pandas)
        for name in ("defenders", "receivers", "versus")
    }


def parse_pff_player_detail(
    raw: dict,
    *,
    career: bool = False,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse a PFF player-detail envelope into a per-week (or per-season) long frame.

    The envelope is ``{report_slug: {subject, week_totals, weeks}}`` (or ``seasons`` when
    ``career=True``). Each row is flattened: the nested ``game`` object is exploded into
    ``game_*`` columns, and ``player_id`` / ``league_id`` / ``season`` are injected from
    ``subject`` when a row lacks them. ``game_id`` is retained as an integer join key.

    Args:
        raw: Raw JSON response dict from a ``/player/...`` endpoint.
        career: Parse the ``seasons`` rollup instead of per-week rows.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        A tidy polars frame (or pandas), one row per week (or season). Zero-row on empty input.

    Example:
        Quick start::

            from sportsdataverse.nfl.pff_parsers import parse_pff_player_detail

            df = parse_pff_player_detail(json.load(open("player_passing_summary.json")))
            print(df.select(["player_id", "game_id", "grades_offense"]).head())

        See Also:
            * `nflfastR`_ -- NFL play-by-play + EPA/WPA context

        .. _nflfastR: https://www.nflfastr.com
    """
    if not isinstance(raw, dict) or not raw:
        return _maybe_pandas(pl.DataFrame(), return_as_pandas)
    keys = list(raw.keys())
    obj = raw[keys[0]] if len(keys) == 1 and isinstance(raw[keys[0]], dict) else raw
    if not isinstance(obj, dict):
        return _maybe_pandas(pl.DataFrame(), return_as_pandas)

    subject = obj.get("subject") or {}
    rows = obj.get("seasons") if career else obj.get("weeks")
    if not rows:
        rows = obj.get("week_totals") or obj.get("career") or []
    if isinstance(rows, dict):
        rows = [rows]

    flat: List[dict] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        r = dict(row)
        game = r.pop("game", None)
        if isinstance(game, dict):
            for gk, gv in game.items():
                r[f"game_{gk}"] = gv
        for sk in ("player_id", "league_id", "season"):
            r.setdefault(sk, subject.get(sk))
        flat.append(r)

    return _maybe_pandas(_frame(flat), return_as_pandas)
