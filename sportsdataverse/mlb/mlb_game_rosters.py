from __future__ import annotations

import re as _re

import pandas as pd
import polars as pl

from sportsdataverse import _espn_game_rosters
from sportsdataverse._codegen_runtime import _download_json, _pandas_ids
from sportsdataverse.dl_utils import download, underscore

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
    "athlete_starter",
)


def _ref_id(obj: dict, kind: str) -> str | None:
    """``obj["id"]``, else the id at the end of ``obj["$ref"]`` (``.../athletes/39832?lang=en``).

    The real Core v2 roster entry carries the athlete and the position as a bare ``{"$ref": ...}`` with no
    inline id, so reading ``id`` alone left ``athlete_id`` / ``athlete_position_id`` null on every game.
    """
    if obj.get("id") is not None:
        return str(obj["id"])
    match = _re.search(rf"/{kind}/(\d+)", str(obj.get("$ref") or ""))
    return match.group(1) if match else None


def espn_mlb_game_rosters(game_id: int, raw: bool = False, return_as_pandas: bool = False, **kwargs):
    """espn_mlb_game_rosters - pull the active game rosters for both teams.

    Wraps the Core v2 endpoint::

        https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{game_id}/competitions/{game_id}/competitors

    Each competitor's ``roster.$ref`` is dereferenced to the per-team athlete list,
    then athletes are flattened to one row per (game × team × athlete).

    Args:
        game_id (int): ESPN game id.
        raw (bool): When True, returns the merged competitor + roster payload dict.
        return_as_pandas (bool): When True, returns a pandas dataframe; otherwise polars.

    Returns:
        pl.DataFrame: One row per (game × team × athlete) with columns
        ``game_id, team_id, home_away, athlete_id, athlete_full_name, athlete_jersey,
        athlete_position_id, athlete_position_abbreviation, athlete_position_name,
        athlete_starter``; zero rows with those columns when ESPN lists no competitor
        roster.

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
        entries = roster_payload.get("entries") or []
        for e in entries:
            athlete = e.get("athlete") or {}
            position = athlete.get("position") or e.get("position") or {}
            rows.append(
                {
                    "game_id": str(game_id),
                    "team_id": str(team_id) if team_id else None,
                    "home_away": home_away,
                    # the entry carries the athlete as a $ref only; its playerId is the same id
                    "athlete_id": _ref_id(athlete, "athletes") or (str(e["playerId"]) if e.get("playerId") else None),
                    "athlete_full_name": athlete.get("fullName") or athlete.get("displayName") or e.get("displayName"),
                    "athlete_jersey": athlete.get("jersey"),
                    "athlete_position_id": _ref_id(position, "positions"),
                    "athlete_position_abbreviation": position.get("abbreviation"),
                    "athlete_position_name": position.get("name"),
                    "athlete_starter": e.get("starter"),
                },
            )

    if raw:
        return {"competitors": items, "rows": rows}
    if not rows:  # B-L25: no competitors (or no roster) -> the documented columns, zero rows; was None
        schema = {c: pl.Boolean if c == "athlete_starter" else pl.String for c in _COLUMNS}
        empty = _espn_game_rosters.roster_ids(pl.DataFrame(schema=schema))
        return _pandas_ids(empty) if return_as_pandas else empty
    df = pd.DataFrame(rows)
    df.columns = [underscore(c) for c in df.columns.tolist()]
    out = _espn_game_rosters.roster_ids(pl.from_pandas(df))
    return _pandas_ids(out) if return_as_pandas else out
