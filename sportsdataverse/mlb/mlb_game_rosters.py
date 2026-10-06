from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _download_json
from sportsdataverse.dl_utils import download, underscore
from sportsdataverse.errors import NoDataError


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
        athlete_position_id, athlete_position_abbreviation, athlete_starter``.

    Example:
        Pull both lineups for a single game::

            from sportsdataverse.mlb import espn_mlb_game_rosters
            ros = espn_mlb_game_rosters(game_id=401569461)
            print(ros.shape)
            ros.group_by("home_away").len()
    """
    competitors_url = (
        "https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/"
        f"events/{game_id}/competitions/{game_id}/competitors"
    )
    comp_payload = _download_json(download, competitors_url, **kwargs)
    items = comp_payload.get("items") or []
    if not items:
        return None

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
                    "athlete_id": athlete.get("id"),
                    "athlete_full_name": athlete.get("fullName") or athlete.get("displayName"),
                    "athlete_jersey": athlete.get("jersey"),
                    "athlete_position_id": position.get("id"),
                    "athlete_position_abbreviation": position.get("abbreviation"),
                    "athlete_position_name": position.get("name"),
                    "athlete_starter": e.get("starter"),
                },
            )

    if raw:
        return {"competitors": items, "rows": rows}
    df = pd.DataFrame(rows)
    df.columns = [underscore(c) for c in df.columns.tolist()]
    return df if return_as_pandas else pl.from_pandas(df)
