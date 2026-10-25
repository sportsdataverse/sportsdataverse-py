from __future__ import annotations

import re as _re
import warnings
from typing import Any

import polars as pl

from sportsdataverse import _espn_game_rosters
from sportsdataverse._codegen_runtime import _download_json, _pandas_ids
from sportsdataverse.dl_utils import download, underscore  # noqa: F401 -- underscore leaks via `import *` (coverage_allowlist.yaml)

_COLUMNS = (
    "game_id",
    "team_id",
    "home_away",
    "athlete_id",
    "athlete_full_name",
    "athlete_jersey",
    "athlete_position_id",
    "athlete_position_abbreviation",
    "athlete_position_name",
    "game_position_id",
    "athlete_starter",
)

# The shared athlete step's columns -> this chain's documented names.
_ATHLETE_COLUMNS = {
    "athlete_id": "athlete_id",
    "full_name": "athlete_full_name",
    "jersey": "athlete_jersey",
    "position_id": "athlete_position_id",
    "position_abbreviation": "athlete_position_abbreviation",
    "position_name": "athlete_position_name",
}

# The ids a Core v2 ``$ref`` carries in its path: the athlete (``.../athletes/39832?...``) and the position the
# entry was listed at in this game (``.../positions/10?...``).
_ATHLETE_ID_RE = _re.compile(r"/athletes/(\d+)")
_POSITION_ID_RE = _re.compile(r"/positions/(\d+)")


def espn_mlb_game_rosters(game_id: int, raw: bool = False, return_as_pandas: bool = False, **kwargs):
    """espn_mlb_game_rosters - pull the active game rosters for both teams.

    Wraps the Core v2 endpoint::

        https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{game_id}/competitions/{game_id}/competitors

    Each competitor's ``roster.$ref`` is dereferenced to the per-team entry list, and each entry's
    athlete ``$ref`` to the athlete record (the same step the other Core v2 roster chains use), so the
    id, full name, jersey and position come from the athlete; one row per (game × team × athlete).
    ``athlete_position_*`` is the athlete record's position, i.e. his listed (season) position; the
    position he was entered at in this game is ``game_position_id``, read from the entry's own position
    ``$ref``. An entry that carries the athlete inline (no ``$ref``) uses that record. An entry with
    neither, or whose athlete ``$ref`` 404s, is left out with a warning; a side whose roster 404s is
    skipped. When roster entries exist but no athlete can be read the game raises ``NoDataError``, as
    the other Core v2 roster chains do.

    Args:
        game_id (int): ESPN game id.
        raw (bool): When True, returns ``{"competitors": [...], "rows": [...]}``: the competitor items and
            one dict per roster entry with the documented columns, read from the entry only (no athlete
            fetch; ``athlete_id`` is the entry's ``playerId``, else the id in its athlete ``$ref``; it and
            ``game_position_id`` are ints).
        return_as_pandas (bool): When True, returns a pandas dataframe; otherwise polars.
        **kwargs: Passed to every download (competitors, rosters, athletes).

    Returns:
        pl.DataFrame: One row per (game × team × athlete) with columns
        ``game_id, team_id, home_away, athlete_id, athlete_full_name, athlete_jersey,
        athlete_position_id, athlete_position_abbreviation, athlete_position_name, game_position_id,
        athlete_starter``; zero rows with those columns when ESPN lists no competitor roster entry.

    Raises:
        AssetFetchError: a competitors, roster or athlete fetch failed (not a 404).
        NoDataError: the competitors resource answered 404, or roster entries exist but no athlete
            record could be read (every athlete ``$ref`` 404'd, or no entry carries one).

    Example:
        Pull both lineups for a single game::

            from sportsdataverse.mlb import espn_mlb_game_rosters
            ros = espn_mlb_game_rosters(game_id=401569461)
            print(ros.shape)
            ros.group_by("home_away").len()
    """
    # Local: this module has no __all__, so a top-level import would be star-exported
    # from sportsdataverse.mlb as an undocumented public name.
    from sportsdataverse.errors import NoDataError

    competitors_url = (
        "https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/"
        f"events/{game_id}/competitions/{game_id}/competitors"
    )
    comp_payload = _download_json(download, competitors_url, **kwargs)
    items = comp_payload.get("items") or []

    rows = []
    for c in items:
        team_id = (c.get("team") or {}).get("id") or c.get("id")
        home_away = c.get("homeAway")
        roster_ref = (c.get("roster") or {}).get("$ref")
        if not roster_ref:
            continue
        try:
            roster_payload = _download_json(download, roster_ref, **kwargs)
        except NoDataError:  # one side's roster 404s: keep the other; a FAILED fetch still raises
            continue
        for e in roster_payload.get("entries") or []:
            rows.append(_entry_row(e, game_id, team_id, home_away))

    if raw:  # the entry fields only, one dict per entry with the documented keys (no athlete fetch)
        return {"competitors": items, "rows": [{k: r[k] for k in _COLUMNS} for r in rows]}
    out = _resolve_athletes(rows, **kwargs)
    return _pandas_ids(out) if return_as_pandas else out


def _ref_id(ref: Any, pattern: _re.Pattern) -> Any:
    """The id a Core v2 ``$ref`` carries in its path (``pattern``'s group) as an ``int``, or ``None``."""
    match = pattern.search(ref) if isinstance(ref, str) else None
    return int(match.group(1)) if match else None


def _int_id(value: Any) -> Any:
    """An id as an ``int`` when it is one (ESPN ships ``playerId`` as a number, an inline ``id`` as a string), so a
    raw row's ``athlete_id`` has one type whichever field it came from; anything else is returned unchanged."""
    if isinstance(value, str) and value.strip().isdigit():
        return int(value)
    return value


def _entry_row(e: dict, game_id: Any, team_id: Any, home_away: Any) -> dict:
    """One roster entry's documented fields, read from the entry alone.

    The Core v2 entry carries the athlete as a bare ``$ref`` (no inline id, name or position); its
    ``playerId`` is the athlete id (R1002-1), and when it is absent the id in the ``$ref`` path is
    (PRE-1008 M-2). The entry's own position ``$ref`` is the position for this game
    (``game_position_id``, PRE-1008 M-1). An older inline athlete record supplies its own fields.
    ``_href`` keeps the ``$ref`` and ``_inline`` marks an inline record, for :func:`_resolve_athletes`.
    """
    athlete = e.get("athlete") if isinstance(e.get("athlete"), dict) else {}
    href = athlete.get("$ref")
    inline = {} if href else athlete
    position = inline.get("position") or e.get("position") or {}
    position = position if isinstance(position, dict) and "$ref" not in position else {}
    entry_position = e.get("position") if isinstance(e.get("position"), dict) else {}
    player_id = e.get("playerId") if e.get("playerId") is not None else _ref_id(href, _ATHLETE_ID_RE)
    return {
        "game_id": str(game_id),
        "team_id": str(team_id) if team_id else None,
        "home_away": home_away,
        "athlete_id": _int_id(inline.get("id") if inline.get("id") is not None else player_id),
        "athlete_full_name": inline.get("fullName") or inline.get("displayName"),
        "athlete_jersey": inline.get("jersey"),
        "athlete_position_id": position.get("id"),
        "athlete_position_abbreviation": position.get("abbreviation"),
        "athlete_position_name": position.get("name"),
        "game_position_id": _ref_id(entry_position.get("$ref"), _POSITION_ID_RE),
        "athlete_starter": e.get("starter"),
        "_href": href,
        "_inline": inline.get("id") is not None,
    }


def _resolve_athletes(rows: list, **kwargs: Any) -> pl.DataFrame:
    """The documented frame: inline athletes as they came, ``$ref`` athletes read through the shared step.

    An entry with neither an inline athlete nor a ``$ref``, whose ``$ref`` 404s, or whose athlete
    matches no record read, is left out with a warning (WB-consolidation-1, PRE-1008 M-2). When entries
    exist but none can be read the game raises ``NoDataError``, as the sibling chains do (PRE-1008 D-3);
    no entry at all is the documented zero-row frame. A failed fetch still raises ``AssetFetchError``.
    """
    from sportsdataverse.errors import NoDataError

    schema = {c: pl.Boolean if c == "athlete_starter" else pl.String for c in _COLUMNS}
    unreadable = [r for r in rows if not r["_href"] and not r["_inline"]]
    if unreadable:
        warnings.warn(
            f"{len(unreadable)} ESPN MLB roster entries carry no athlete record or $ref; they are left out",
            UserWarning,
            stacklevel=3,
        )
    parts = []
    inline = [{**{k: r[k] for k in _COLUMNS}, "_order": i} for i, r in enumerate(rows) if r["_inline"]]
    if inline:
        parts.append(
            pl.DataFrame(inline, schema={**schema, "_order": pl.Int64}, strict=False).with_columns(
                pl.col(c).cast(pl.String) for c in _COLUMNS if c != "athlete_starter"
            )
        )
    refs = [(i, r) for i, r in enumerate(rows) if r["_href"] and not r["_inline"]]
    if refs:
        entries = pl.DataFrame(
            [{"player_id": r["athlete_id"], "athlete_href": r["_href"], "_order": i} for i, r in refs],
            schema={"player_id": pl.Int64, "athlete_href": pl.String, "_order": pl.Int64},
            strict=False,
        )
        try:
            athletes = _espn_game_rosters.athlete_items(entries, **kwargs)
        except NoDataError:  # every $ref 404'd: each was already warned about and left out
            athletes = pl.DataFrame(schema={"athlete_id": pl.Int64})
        athletes = (
            athletes.with_columns(
                [pl.lit(None, dtype=pl.String).alias(c) for c in _ATHLETE_COLUMNS if c not in athletes.columns]
            )
            .select([pl.col(src).alias(dst) for src, dst in _ATHLETE_COLUMNS.items()])
            .unique("athlete_id", keep="first", maintain_order=True)  # one record per athlete: no row multiplies
        )
        if entries.schema["player_id"] != athletes.schema["athlete_id"]:
            raise TypeError(
                f"athlete join keys disagree: entry player_id {entries.schema['player_id']} vs "
                f"athlete_id {athletes.schema['athlete_id']}"
            )
        keep = ("game_id", "team_id", "home_away", "game_position_id", "athlete_starter")
        fields = pl.DataFrame(
            [{k: r[k] for k in keep} for _, r in refs], schema={k: schema[k] for k in keep}, strict=False
        )
        joined = pl.concat([fields, entries.select("player_id", "_order")], how="horizontal").join(
            athletes, left_on="player_id", right_on="athlete_id", how="inner", maintain_order="left"
        )
        # An entry left out by the join whose athlete was not read at all 404'd (already warned by athlete_items);
        # one whose athlete WAS read, or that has no id to join on, is a silent loss without this warning (M-2).
        read = set(athletes["athlete_id"].drop_nulls().to_list())
        joined_orders = set(joined["_order"].to_list())
        unmatched = 0
        for i, r in refs:
            if i in joined_orders:
                continue
            href_id = _ref_id(r["_href"], _ATHLETE_ID_RE)
            if r["athlete_id"] is None or href_id is None or href_id in read:
                unmatched += 1
        if unmatched:
            warnings.warn(
                f"{unmatched} ESPN MLB roster entries match no athlete record read (no athlete id, or an id that "
                "differs from the record's); they are left out",
                UserWarning,
                stacklevel=3,
            )
        if joined.height:
            parts.append(
                joined.rename({"player_id": "athlete_id"})
                .select(*_COLUMNS, "_order")
                .with_columns(pl.col(c).cast(pl.String) for c in _COLUMNS if c != "athlete_starter")
            )
    if not parts:
        if rows:  # entries exist but no athlete is readable: the siblings' NoDataError (PRE-1008 D-3)
            raise NoDataError("NoDataError: no athlete resource found for any rostered MLB player")
        # B-L25: no competitors or no roster entry -> the documented columns, zero rows
        return _espn_game_rosters.roster_ids(pl.DataFrame(schema=schema))
    stacked = pl.concat(parts, how="vertical").sort("_order")
    return _espn_game_rosters.roster_ids(stacked.select(_COLUMNS))
