from __future__ import annotations

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


def espn_mlb_game_rosters(game_id: int, raw: bool = False, return_as_pandas: bool = False, **kwargs):
    """espn_mlb_game_rosters - pull the active game rosters for both teams.

    Wraps the Core v2 endpoint::

        https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{game_id}/competitions/{game_id}/competitors

    Each competitor's ``roster.$ref`` is dereferenced to the per-team entry list, and each entry's
    athlete ``$ref`` to the athlete record (the same step the other Core v2 roster chains use), so the
    id, full name, jersey and position come from the athlete; one row per (game × team × athlete).
    An athlete whose ``$ref`` 404s is left out with a warning; a failed fetch raises.

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
            # the Core v2 entry carries the athlete as a bare $ref (no inline id, name or position); its
            # playerId is the athlete id, the key the athlete records join back on (R1002-1)
            rows.append(
                {
                    "game_id": str(game_id),
                    "team_id": str(team_id) if team_id else None,
                    "home_away": home_away,
                    "player_id": e.get("playerId"),
                    "athlete_href": (e.get("athlete") or {}).get("$ref"),
                    "athlete_starter": e.get("starter"),
                },
            )

    if raw:
        return {"competitors": items, "rows": rows}
    if not rows:  # B-L25: no competitors (or no roster) -> the documented columns, zero rows; was None
        schema = {c: pl.Boolean if c == "athlete_starter" else pl.String for c in _COLUMNS}
        empty = _espn_game_rosters.roster_ids(pl.DataFrame(schema=schema))
        return _pandas_ids(empty) if return_as_pandas else empty
    entries = pl.DataFrame(rows, infer_schema_length=None).with_columns(pl.col("player_id").cast(pl.Int64))
    athletes = _espn_game_rosters.athlete_items(entries.filter(pl.col("athlete_href").is_not_null()), **kwargs)
    athletes = athletes.with_columns(
        [pl.lit(None, dtype=pl.String).alias(c) for c in _ATHLETE_COLUMNS if c not in athletes.columns]
    ).select([pl.col(src).alias(dst) for src, dst in _ATHLETE_COLUMNS.items()])
    # siblings' rule: an athlete whose $ref 404s (dropped by athlete_items) leaves its entry out
    assert entries.schema["player_id"] == athletes.schema["athlete_id"], "athlete join keys disagree"
    joined = entries.join(athletes, left_on="player_id", right_on="athlete_id", how="inner", maintain_order="left")
    out = _espn_game_rosters.roster_ids(joined.rename({"player_id": "athlete_id"}).select(_COLUMNS))
    return _pandas_ids(out) if return_as_pandas else out
