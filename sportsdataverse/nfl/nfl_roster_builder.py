"""SDV-native NFL season-roster builder (public Shield tier only).

:func:`build_nfl_rosters` assembles a tidy, one-row-per-player season roster
frame **directly from the public NFL Shield API** (``api.nfl.com``'s
``/football/v2/rosters`` endpoint, reached through the anonymous-bearer wrapper
:func:`sportsdataverse.nfl.nfl_rosters`). It is the *self-sufficient* half of
sdv-py's roster story:

* :func:`sportsdataverse.nfl.load_nfl_rosters` reads nflverse's **published**
  roster parquet, which is itself the union of three upstream tiers — NFL Next
  Gen Stats (2016+), the credentialed NFL Data Exchange (2002-2015), and the
  public Shield endpoint (all seasons). That is the richest data and should be
  preferred whenever a network round trip to nflverse is acceptable.
* :func:`build_nfl_rosters` rebuilds an equivalent frame from **only the public
  Shield tier**, which sdv-py can reach without credentials. It is therefore a
  *partial* mirror of the nflverse frame: Shield supplies ``gsis_id`` densely
  across all seasons, but the cross-system identifier columns (``espn_id``,
  ``sportradar_id``, ``yahoo_id``, …) and ``college`` are best-effort
  enrichments joined from :func:`sportsdataverse.nfl.load_nfl_players`. That
  enrichment is dense for modern players and **sparse for pre-2016 seasons**,
  by design — Shield does not itself carry the cross-walk IDs.

Use :func:`load_nfl_rosters` for the richest roster data; use
:func:`build_nfl_rosters` when you need an SDV-native roster frame that depends
only on the live NFL Shield API (no nflverse release dependency).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Union, overload

import polars as pl

from sportsdataverse.nfl.nfl_api import nfl_rosters

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import (PEP 563 defers eval)
    import pandas as pd

__all__ = ["build_nfl_rosters"]

# ---------------------------------------------------------------------------
# SDV-native season-roster schema. Mirrors the nflverse ``load_nfl_rosters``
# column set so the two surfaces are drop-in comparable. Columns Shield does
# not supply (most cross-system IDs) are still emitted (null / enriched) so the
# frame carries a stable, documented column set even when a season is empty.
# ---------------------------------------------------------------------------
_SCHEMA: Dict[str, pl.DataType] = {
    "season": pl.Int64,
    "team": pl.Utf8,
    "position": pl.Utf8,
    "depth_chart_position": pl.Utf8,
    "jersey_number": pl.Int64,
    "status": pl.Utf8,
    "full_name": pl.Utf8,
    "first_name": pl.Utf8,
    "last_name": pl.Utf8,
    "birth_date": pl.Utf8,
    "height": pl.Float64,
    "weight": pl.Int64,
    "college": pl.Utf8,
    "gsis_id": pl.Utf8,
    "espn_id": pl.Utf8,
    "sportradar_id": pl.Utf8,
    "yahoo_id": pl.Utf8,
    "rotowire_id": pl.Utf8,
    "pff_id": pl.Utf8,
    "pfr_id": pl.Utf8,
    "fantasy_data_id": pl.Utf8,
    "sleeper_id": pl.Utf8,
    "years_exp": pl.Int64,
    "headshot_url": pl.Utf8,
    "esb_id": pl.Utf8,
    "smart_id": pl.Utf8,
    "football_name": pl.Utf8,
    "ngs_position": pl.Utf8,
    "entry_year": pl.Int64,
    "rookie_year": pl.Int64,
}

# Cross-system ID + enrichment columns sourced from load_nfl_players() (joined
# on gsis_id). Shield itself supplies none of these; left-null when unmatched.
_PLAYER_ENRICH: Dict[str, str] = {
    "espn_id": "espn_id",
    "pfr_id": "pfr_id",
    "pff_id": "pff_id",
    "smart_id": "smart_id",
    "college": "college_name",
    "ngs_position": "ngs_position",
    "rookie_year": "rookie_season",
}


def _relocate_team(abbr: Optional[str], season: int) -> Optional[str]:
    """Fold a Shield team abbreviation onto the nflverse-standard abbreviation
    for *season*.

    Relocations are applied **season-aware** so a historical roster keeps its
    era-correct identity: ``OAK`` only becomes ``LV`` from 2020, ``SD`` becomes
    ``LAC`` from 2017, ``STL`` becomes ``LA`` from 2016. The spelling-only
    fixes (``LAR`` -> ``LA``, ``JAC`` -> ``JAX``) are unconditional.
    """
    if abbr is None:
        return None
    if abbr == "OAK" and season >= 2020:
        return "LV"
    if abbr == "SD" and season >= 2017:
        return "LAC"
    if abbr == "STL" and season >= 2016:
        return "LA"
    if abbr == "LAR":
        return "LA"
    if abbr == "JAC":
        return "JAX"
    return abbr


def _empty_frame(return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Zero-row frame carrying the full documented schema (never raises)."""
    frame = pl.DataFrame(schema=_SCHEMA)
    return frame.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else frame


def _person_row(person: Dict, season: int, team: Optional[str]) -> Dict:
    """Map one Shield ``persons[]`` entry onto the SDV-native roster schema.

    Cross-system IDs (``espn_id`` etc.) are left ``None`` here; they are filled
    by the :func:`load_nfl_players` enrichment join. ``entry_year`` is derived
    best-effort as ``season - nflExperience`` (Shield exposes years of NFL
    experience but not an explicit entry year).
    """
    colleges = person.get("collegeNames") or []
    college = colleges[0] if colleges else None
    years_exp = person.get("nflExperience")
    entry_year = season - years_exp if isinstance(years_exp, int) else None
    return {
        "season": season,
        "team": team,
        "position": person.get("position"),
        "depth_chart_position": person.get("positionGroup"),
        "jersey_number": person.get("jerseyNumber"),
        "status": person.get("status"),
        "full_name": person.get("displayName"),
        "first_name": person.get("firstName"),
        "last_name": person.get("lastName"),
        "birth_date": person.get("birthDate"),
        "height": person.get("height"),
        "weight": person.get("weight"),
        "college": college,
        "gsis_id": person.get("gsisId"),
        "espn_id": None,
        "sportradar_id": None,
        "yahoo_id": None,
        "rotowire_id": None,
        "pff_id": None,
        "pfr_id": None,
        "fantasy_data_id": None,
        "sleeper_id": None,
        "years_exp": years_exp,
        "headshot_url": person.get("headshot"),
        "esb_id": person.get("esbId"),
        "smart_id": None,
        "football_name": person.get("commonFirstName"),
        "ngs_position": None,
        "entry_year": entry_year,
        "rookie_year": None,
    }


def _enrich_cross_ids(frame: pl.DataFrame) -> pl.DataFrame:
    """Left-join :func:`load_nfl_players` cross-system IDs + college on gsis_id.

    Best-effort: missing players (or a failed players load) leave the enrichment
    columns untouched (Shield-supplied values / null). The join only *fills*
    columns Shield left null — it never overwrites a Shield-supplied value.
    """
    from sportsdataverse.nfl.nfl_loaders import load_nfl_players

    try:
        players = load_nfl_players()
    except Exception:  # noqa: BLE001 -- enrichment is strictly best-effort
        return frame
    if players.is_empty() or "gsis_id" not in players.columns:
        return frame

    have = [src for src in set(_PLAYER_ENRICH.values()) if src in players.columns]
    if not have:
        return frame
    lookup = players.select(["gsis_id", *have]).unique(subset=["gsis_id"], keep="first")
    rename = {src: f"_pl_{src}" for src in have}
    lookup = lookup.rename(rename)

    frame = frame.join(lookup, on="gsis_id", how="left")
    # Coalesce Shield value (kept) with the players-table fill, per target col.
    fills = []
    for target, src in _PLAYER_ENRICH.items():
        joined = f"_pl_{src}"
        if joined in frame.columns:
            fills.append(
                pl.coalesce([pl.col(target), pl.col(joined).cast(pl.Utf8)]).alias(target)
                if target != "rookie_year"
                else pl.coalesce([pl.col(target), pl.col(joined).cast(pl.Int64)]).alias(target)
            )
    if fills:
        frame = frame.with_columns(fills)
    drop = [c for c in frame.columns if c.startswith("_pl_")]
    return frame.drop(drop)


@overload
def build_nfl_rosters(seasons: List[int]) -> pl.DataFrame: ...
@overload
def build_nfl_rosters(seasons: List[int], *, return_as_pandas: bool = ...) -> Union[pl.DataFrame, "pd.DataFrame"]: ...


def build_nfl_rosters(
    seasons: List[int],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build SDV-native NFL season rosters from the public Shield API.

    For each ``(season, team)`` the public NFL Shield endpoint
    ``/football/v2/rosters`` returns (reached through
    :func:`sportsdataverse.nfl.nfl_rosters`), every player in the ``persons[]``
    array is flattened onto the SDV-native season-roster schema, team
    abbreviations are folded to the nflverse standard (season-aware
    relocations), and cross-system IDs + college are enriched by a best-effort
    left join against :func:`sportsdataverse.nfl.load_nfl_players` on
    ``gsis_id``.

    This is the **public Shield tier only** — a partial mirror of nflverse's
    full three-tier roster product. Shield supplies ``gsis_id`` densely across
    all seasons, but the cross-system IDs (``espn_id``, ``sportradar_id``,
    ``yahoo_id``, ``rotowire_id``, ``pff_id``, ``pfr_id``, ``fantasy_data_id``,
    ``sleeper_id``) and ``college`` are only as dense as the players-table
    cross-walk, which is **sparse for pre-2016 seasons**. For the richest roster
    data prefer :func:`sportsdataverse.nfl.load_nfl_rosters` (reads nflverse's
    published parquet); use :func:`build_nfl_rosters` when you need an
    SDV-native frame that depends only on the live NFL Shield API.

    Args:
        seasons: Seasons to build (e.g. ``[2023]`` or ``range(2020, 2025)``).
            A single ``int`` is accepted and wrapped. A season Shield returns no
            data for contributes no rows rather than raising.
        return_as_pandas: If ``True``, return a ``pandas.DataFrame``; otherwise a
            ``polars.DataFrame`` (default).

    Returns:
        A one-row-per-player season-roster ``DataFrame`` with the documented
        schema. An empty / missing season yields a zero-row frame carrying the
        same column set (never a raise).

    Raises:
        TypeError: If ``seasons`` is not an ``int`` or an iterable of ``int``.

    Example:
        Quick start::

            from sportsdataverse.nfl import build_nfl_rosters
            rosters = build_nfl_rosters([2023])
            print(rosters.shape)

        Multi-season build, pandas output::

            df = build_nfl_rosters(range(2021, 2024), return_as_pandas=True)

        Pipeline next step (one line)::

            import polars as pl
            build_nfl_rosters([2023]).filter(pl.col("team") == "KC").head()

        See Also:
            * `nflverse`_ -- full three-tier roster product (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings (load_rosters)

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if isinstance(seasons, int):
        seasons = [seasons]
    try:
        season_list = [int(s) for s in seasons]
    except TypeError as exc:  # not iterable / not int-coercible
        raise TypeError("seasons must be an int or an iterable of ints") from exc

    rows: List[Dict] = []
    for season in season_list:
        raw = nfl_rosters(season=season, return_parsed=False)
        rosters = raw.get("rosters", []) if isinstance(raw, dict) else []
        for roster in rosters or []:
            team_abbr = (roster.get("team") or {}).get("abbreviation")
            team = _relocate_team(team_abbr, season)
            for person in roster.get("persons", []) or []:
                rows.append(_person_row(person, season, team))

    if not rows:
        return _empty_frame(return_as_pandas)

    frame = pl.DataFrame(rows, schema=_SCHEMA)
    frame = _enrich_cross_ids(frame)
    # Re-assert column order (the enrichment join can reorder).
    frame = frame.select(list(_SCHEMA.keys()))
    return frame.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else frame
