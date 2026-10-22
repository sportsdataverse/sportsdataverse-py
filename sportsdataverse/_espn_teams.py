"""Request and frame shared by the hand-written ``espn_<lg>_teams`` functions (task 2.11).

The eight Site v2 ``teams`` bodies (mbb, wbb, nba, wnba, nfl, nhl, cfb, mlb) differed only in the
URL and the ``groups`` default, so each league module keeps its public name, signature and
docstring and hands the request to :func:`teams_frame`:

* no ``functools.lru_cache``: it raised ``TypeError: unhashable type`` on ``headers={...}``,
  handed every caller the same frame object, and never expired (B-M23). Each call fetches; the
  package-wide TTL response cache (``sportsdataverse.cache``) is the opt-in cache;
* a payload without ``sports[0].leagues[0].teams`` returns a zero-row frame with the documented
  columns instead of ``TypeError`` / ``IndexError`` (B-L1);
* the payload is never mutated;
* ``team_id`` is ``Int64``, cast from the raw integer (task 5.2, B-M15).

``TEAMS_SCHEMA`` mirrors ``tools/codegen/schemas/autodoc/<lg>/espn_<lg>_teams.yaml`` (the same 14
columns in every league); the teams family tests assert the two agree.
"""

from __future__ import annotations

from typing import Any, Callable, TypeVar

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64, _download_json
from sportsdataverse._deprecation import warn_deprecated
from sportsdataverse.dl_utils import download, underscore

_F = TypeVar("_F", bound=Callable[..., Any])

_LOGO = pl.Struct(
    {"alt": pl.String, "height": pl.Int64, "href": pl.String, "rel": pl.List(pl.String), "width": pl.Int64}
)

#: The documented teams columns, in documented order, with the dtypes a real capture produces.
TEAMS_SCHEMA: dict[str, pl.DataType] = {
    "team_abbreviation": pl.String(),
    "team_alternate_color": pl.String(),
    "team_color": pl.String(),
    "team_display_name": pl.String(),
    "team_id": pl.Int64(),
    "team_is_active": pl.Boolean(),
    "team_is_all_star": pl.Boolean(),
    "team_location": pl.String(),
    "team_logos": pl.List(_LOGO),
    "team_name": pl.String(),
    "team_nickname": pl.String(),
    "team_short_display_name": pl.String(),
    "team_slug": pl.String(),
    "team_uid": pl.String(),
}

_DROPPED = ("record", "links")


def _first(value: Any) -> dict[str, Any]:
    """The first element of a list of mappings, ``{}`` when there is none."""
    return value[0] if isinstance(value, list) and value and isinstance(value[0], dict) else {}


def teams_frame(
    url: str, params: dict[str, Any], return_as_pandas: bool = False, **kwargs: Any
) -> pl.DataFrame | pd.DataFrame:
    """Fetch a Site v2 ``teams`` resource and flatten it to one ``team_*`` row per team.

    Raises:
        NoDataError: ESPN answered 404.
        AssetFetchError: the fetch failed (403, 429, 5xx, a dropped connection).
    """
    payload = _download_json(download, url, params=params, **kwargs)
    league = _first(_first(payload.get("sports") if isinstance(payload, dict) else None).get("leagues"))
    teams = [
        {"team": {k: v for k, v in t["team"].items() if k not in _DROPPED}}
        for t in league.get("teams") or []
        if isinstance(t, dict) and isinstance(t.get("team"), dict)
    ]
    if not teams:
        empty = pl.DataFrame(schema=TEAMS_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    frame = pd.json_normalize(teams, sep="_")
    frame.columns = [underscore(c) for c in frame.columns.tolist()]
    out = _cast_ids_int64(pl.from_pandas(frame), ["team_id"])
    return out.to_pandas() if return_as_pandas else out


def cache_clear_shim(fn: _F) -> _F:
    """Give ``fn`` the ``cache_clear()`` its ``lru_cache`` used to provide, for one deprecation cycle."""

    def cache_clear() -> None:
        warn_deprecated(
            f"{fn.__name__}.cache_clear",
            removed_in="0.2.0",
            extra=f"{fn.__name__} no longer caches: every call fetches. "
            "For a TTL cache turn on the package response cache (sportsdataverse.cache).",
        )

    fn.__dict__["cache_clear"] = cache_clear  # a function attribute; mypy cannot see `fn.cache_clear =`
    return fn
