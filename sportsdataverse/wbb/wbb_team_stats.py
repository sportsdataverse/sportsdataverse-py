"""ESPN women's-college-basketball team season-stats scraper.

Single ESPN endpoint:
    site.web.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/statistics?season={year}

ESPN ships team season stats as a multi-table payload — the ``categories``
array under ``results.stats.categories`` carries one bucket per stat family
(currently ``General``, ``Offensive``, ``Defensive``). The wrapper returns
one polars DataFrame per category, keyed by a canonical category name.

The canonical category keys (``"Averages"``, ``"Totals"``, ``"Misc"``) are
always present in the return dict, even when ESPN omits one (the missing
slot is filled with an empty frame carrying the documented schema). Any
category whose ESPN ``displayName`` / ``name`` does not map onto one of
those three is collected under an additional ``"Other"`` key — that key is
only added when there is at least one un-mapped category, so callers
shouldn't unconditionally index into it.

ESPN's current team-stats response uses ``General`` / ``Offensive`` /
``Defensive`` rather than the player-stats triad of ``Averages`` /
``Totals`` / ``Misc``, so the default lookup table maps both shapes onto
the canonical keys. ``General`` rolls up to ``Averages`` (the
games-played-style aggregates ESPN ships there are per-game numbers),
``Offensive`` rolls up to ``Totals``, and ``Defensive`` rolls up to
``Misc``. If ESPN reverts or expands the category set, the new names will
surface under ``"Other"`` until the mapping table here is updated.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download

_LEAGUE_SLUG: str = "womens-college-basketball"

# Canonical category keys, in the order the docstring promises. ``Other`` is
# appended dynamically only when ESPN ships a category that doesn't map onto
# one of these three.
_CANONICAL_CATEGORIES: tuple[str, ...] = (
    "Averages",
    "Totals",
    "Misc",
)

# ESPN's ``name`` / ``displayName`` (lower-cased, trimmed) -> canonical key.
# Both the legacy player-stats triad (averages/totals/misc) and the current
# team-stats triad (general/offensive/defensive) are mapped so callers see a
# stable keyset regardless of which shape ESPN is shipping.
_CATEGORY_NAME_MAP: dict[str, str] = {
    "averages": "Averages",
    "totals": "Totals",
    "misc": "Misc",
    "miscellaneous": "Misc",
    "general": "Averages",
    "offensive": "Totals",
    "defensive": "Misc",
}

# Per-category schema. Used to construct empty frames for missing categories
# so callers always get a stable column set regardless of upstream omissions.
_PER_CATEGORY_SCHEMA: dict[str, type[pl.DataType] | pl.DataType] = {
    "stat_name": pl.Utf8,
    "abbreviation": pl.Utf8,
    "display_value": pl.Utf8,
    "value": pl.Float64,
    "description": pl.Utf8,
    "category": pl.Utf8,
    "team_id": pl.Int64,
    "season": pl.Int32,
}


@overload
def espn_wbb_team_stats(
    team_id: int,
    season: int,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wbb_team_stats(
    team_id: int,
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> dict[str, pd.DataFrame]: ...
@overload
def espn_wbb_team_stats(
    team_id: int,
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> dict[str, pl.DataFrame]: ...
def espn_wbb_team_stats(
    team_id: int,
    season: int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]:
    """Pull ESPN team season stats for a women's-college-basketball team.

    Args:
        team_id: ESPN team identifier (e.g. ``2509`` for UConn).
        season: Season year, forwarded to ESPN as ``?season=YYYY``.
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a dict of pandas DataFrames;
            otherwise polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Dict with one DataFrame per stat category. The canonical keys
        ``"Averages"``, ``"Totals"``, ``"Misc"`` are ALWAYS present;
        missing categories come back as empty frames carrying the
        documented schema. Any ESPN-shipped category whose name does not
        match one of the three canonical keys is collected under an
        additional ``"Other"`` key (only added if non-empty).

        Per-category column set (one row per stat):

        * ``stat_name`` (Utf8)
        * ``abbreviation`` (Utf8)
        * ``display_value`` (Utf8)
        * ``value`` (Float64)
        * ``description`` (Utf8)
        * ``category`` (Utf8, constant per frame)
        * ``team_id`` (Int64, constant)
        * ``season`` (Int32, constant)

        If ``raw=True``, returns the raw response dict.

    Raises:
        sportsdataverse.errors.NoDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after
            retries.

    Example:
        Quick start (UConn ``team_id=2509``)::

            from sportsdataverse.wbb import espn_wbb_team_stats
            frames = espn_wbb_team_stats(team_id=2509, season=2025)
            print(sorted(frames.keys()))

        Index into a specific table::

            averages = frames["Averages"]
            print(averages.shape)
            averages.select(["stat_name", "display_value", "value"]).head()

        Iterate the canonical categories::

            for cat in ("Averages", "Totals", "Misc"):
                print(cat, frames[cat].shape)

        ``Other`` fallback bucket (only present when ESPN ships a category
        that does not map onto one of the three canonical keys)::

            if "Other" in frames:
                frames["Other"].select(["category", "stat_name", "value"])

        Pandas round-trip::

            frames_pd = espn_wbb_team_stats(
                team_id=2579, season=2025, return_as_pandas=True
            )  # team_id 2579 = South Carolina
            frames_pd["Averages"].head()

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    return _espn_basketball_team_stats(
        league=_LEAGUE_SLUG,
        team_id=team_id,
        season=season,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def _espn_basketball_team_stats(
    league: str,
    team_id: int,
    season: int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]:
    """Shared implementation for ``espn_wbb_team_stats`` / ``espn_wnba_team_stats``.

    Builds the ESPN site-v2 team statistics URL for the supplied ``league``
    slug, downloads, walks the ``results.stats.categories`` array,
    normalizes each category into a per-stat frame, and ensures all three
    canonical category keys (``"Averages"``, ``"Totals"``, ``"Misc"``)
    exist in the output.
    """
    url = (
        "https://site.web.api.espn.com/apis/site/v2/sports/basketball/"
        f"{league}/teams/{team_id}/statistics?season={season}"
    )
    resp = download(url, **kwargs)
    payload: dict[str, Any] = resp.json()

    if raw:
        return payload

    # Categories live under ``results.stats.categories``; fall back to
    # ``stats.categories`` if ESPN flattens the wrapper, and to a top-level
    # ``categories`` if they ever rename further.
    categories_raw = (
        (payload.get("results") or {}).get("stats", {}).get("categories")
        or (payload.get("stats") or {}).get("categories")
        or payload.get("categories")
    )

    parsed: dict[str, pl.DataFrame] = {}

    if isinstance(categories_raw, list):
        for idx, cat in enumerate(categories_raw):
            if not isinstance(cat, dict):
                continue
            key, frame = _parse_category(cat, idx, team_id, season)
            if frame.is_empty() and key in parsed and not parsed[key].is_empty():
                continue
            parsed[key] = frame

    empty = pl.DataFrame(schema=_PER_CATEGORY_SCHEMA)
    out: dict[str, pl.DataFrame] = {}
    for key in _CANONICAL_CATEGORIES:
        out[key] = parsed.pop(key, empty)

    if parsed:
        leftovers = [f for f in parsed.values() if not f.is_empty()]
        if leftovers:
            out["Other"] = pl.concat(leftovers, how="vertical")

    if return_as_pandas:
        return {k: v.to_pandas() for k, v in out.items()}
    return out


def _parse_category(
    cat: dict[str, Any],
    idx: int,
    team_id: int,
    season: int,
) -> tuple[str, pl.DataFrame]:
    """Turn one ESPN ``categories[i]`` dict into a per-stat polars frame.

    ESPN's team-stats payload uses the object-array shape: ``stats[]`` of
    dicts each carrying ``displayName``, ``displayValue``, ``value``,
    ``abbreviation``, ``description``. Parallel-arrays shape (labels[],
    totals[], etc.) is also accepted as a forward-compat fallback.
    """
    raw_name = cat.get("displayName") or cat.get("name") or f"Category{idx}"
    key = _CATEGORY_NAME_MAP.get(str(raw_name).strip().lower(), str(raw_name))

    rows = _rows_from_stats_objects(cat)
    if not rows:
        rows = _rows_from_parallel_arrays(cat) or []

    if not rows:
        return key, pl.DataFrame(schema=_PER_CATEGORY_SCHEMA)

    n = len(rows)
    frame = pl.DataFrame(
        {
            "stat_name": pl.Series("stat_name", [r["stat_name"] for r in rows], dtype=pl.Utf8),
            "abbreviation": pl.Series("abbreviation", [r["abbreviation"] for r in rows], dtype=pl.Utf8),
            "display_value": pl.Series("display_value", [r["display_value"] for r in rows], dtype=pl.Utf8),
            "value": pl.Series("value", [r["value"] for r in rows], dtype=pl.Float64),
            "description": pl.Series("description", [r["description"] for r in rows], dtype=pl.Utf8),
            "category": pl.Series("category", [key] * n, dtype=pl.Utf8),
            "team_id": pl.Series("team_id", [team_id] * n, dtype=pl.Int64),
            "season": pl.Series("season", [season] * n, dtype=pl.Int32),
        },
    )
    return key, frame


def _rows_from_stats_objects(cat: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse the stats-objects shape: stats[] of {displayName,...} dicts."""
    stats_raw = cat.get("stats")
    if not isinstance(stats_raw, list):
        statistics_raw = cat.get("statistics")
        stats_raw = statistics_raw if isinstance(statistics_raw, list) else []
    stats: list[Any] = stats_raw
    rows: list[dict[str, Any]] = []
    for s in stats:
        if not isinstance(s, dict):
            continue
        stat_name = s.get("displayName") or s.get("name") or s.get("abbreviation") or ""
        abbreviation = s.get("abbreviation") or s.get("shortDisplayName") or ""
        display_value = s.get("displayValue")
        value = _coerce_float(s.get("value"))
        description = s.get("description") or ""
        rows.append(
            {
                "stat_name": str(stat_name),
                "abbreviation": str(abbreviation),
                "display_value": None if display_value is None else str(display_value),
                "value": value,
                "description": str(description),
            },
        )
    return rows


def _rows_from_parallel_arrays(cat: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Parse the parallel-arrays shape: labels[]/names[]/totals[] aligned by index."""
    labels_raw = cat.get("labels")
    if not isinstance(labels_raw, list) or not labels_raw:
        return None
    labels: list[Any] = labels_raw

    display_names_raw = cat.get("displayNames")
    display_names: list[Any] = display_names_raw if isinstance(display_names_raw, list) else []
    names_raw = cat.get("names")
    names: list[Any] = names_raw if isinstance(names_raw, list) else []
    descriptions_raw = cat.get("descriptions")
    descriptions: list[Any] = descriptions_raw if isinstance(descriptions_raw, list) else []
    abbreviations_raw = cat.get("abbreviations")
    abbreviations: list[Any] = abbreviations_raw if isinstance(abbreviations_raw, list) else []
    totals_raw = cat.get("totals")
    if not isinstance(totals_raw, list):
        totals_raw = cat.get("stats")
    if not isinstance(totals_raw, list):
        return []
    totals: list[Any] = totals_raw

    n = max(len(labels), len(display_names), len(names), len(descriptions), len(totals))
    rows: list[dict[str, Any]] = []
    for i in range(n):
        stat_name = _safe_index(display_names, i) or _safe_index(names, i) or _safe_index(labels, i) or f"stat_{i}"
        abbreviation = _safe_index(abbreviations, i) or ""
        display_value = _safe_index(totals, i)
        value = _coerce_float(display_value)
        description = _safe_index(descriptions, i) or ""
        rows.append(
            {
                "stat_name": str(stat_name),
                "abbreviation": str(abbreviation),
                "display_value": None if display_value is None else str(display_value),
                "value": value,
                "description": str(description),
            },
        )
    return rows


def _safe_index(xs: list[Any], i: int) -> Any:
    if 0 <= i < len(xs):
        return xs[i]
    return None


def _coerce_float(v: Any) -> float | None:
    """Coerce ``v`` to ``float``, returning ``None`` if not convertible."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None
