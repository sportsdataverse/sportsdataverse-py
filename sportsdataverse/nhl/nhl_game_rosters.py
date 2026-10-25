from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse import _espn_game_rosters
from sportsdataverse._codegen_runtime import _download_json, _pandas_ids
from sportsdataverse.dl_utils import (  # noqa: F401 -- normalize_team_roster_columns leaks via `import *` (coverage_allowlist.yaml)
    download,
    normalize_team_roster_columns,
    underscore,
)


def espn_nhl_game_rosters(
    game_id: int, raw=False, return_as_pandas=False, **kwargs
) -> pl.DataFrame | pd.DataFrame | dict:
    """espn_nhl_game_rosters() - Pull the game by id.

    Args:
        game_id (int): Unique game_id, can be obtained from espn_nhl_schedule().
        raw (bool): If True, returns the Core v2 competitors payload (dict) as ESPN sent it;
            its roster, team and athlete ``$ref`` links are not followed.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of game roster data with columns:
        'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type',
        'first_name', 'last_name', 'full_name', 'athlete_display_name',
        'short_name', 'weight', 'display_weight', 'height', 'display_height',
        'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active',
        'alternate_ids_sdr', 'birth_place_city', 'birth_place_state',
        'birth_place_country', 'headshot_href', 'headshot_alt',
        'experience_years', 'experience_display_value',
        'experience_abbreviation', 'status_id', 'status_name', 'status_type',
        'status_abbreviation', 'hand_type', 'hand_abbreviation',
        'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year',
        'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play',
        'display_name', 'ejected', 'athlete_href', 'position_href',
        'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug',
        'team_location', 'team_name', 'team_abbreviation',
        'team_display_name', 'team_short_display_name', 'team_color',
        'team_alternate_color', 'is_active', 'is_all_star',
        'logo_href', 'logo_dark_href', 'game_id'
        plus the athlete's ``position_id``, ``position_name``, ``position_display_name``,
        ``position_abbreviation`` and ``position_leaf`` (a leaf position also ``position_parent_href``).
        With ``raw=True``, the competitors payload (dict).

    Raises:
        NoDataError: ESPN has no competitors, roster or athlete resource for the game (404), or every
            rostered athlete's ``$ref`` answered 404. A single 404 athlete is left out with a
            ``UserWarning`` and the rest of the roster is returned.
        AssetFetchError: a fetch failed (403, 429, 5xx, a dropped connection); never treated as a
            missing athlete.

    Example:
        Pull both teams' rosters for a single game (Stanley Cup Final 2023)::

            from sportsdataverse.nhl import espn_nhl_game_rosters
            rosters = espn_nhl_game_rosters(game_id=401559395)
            print(rosters.shape)
            rosters.select(["athlete_display_name", "jersey", "team_abbreviation", "starter"]).head(10)

        Just the starters::

            import polars as pl
            rosters.filter(pl.col("starter") == True).select(["athlete_display_name", "team_abbreviation"])

        Pandas round-trip::

            rosters_pd = espn_nhl_game_rosters(game_id=401559395, return_as_pandas=True)
            rosters_pd[["athlete_display_name", "team_abbreviation", "did_not_play"]].head()

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    # summary endpoint for pickcenter array
    summary_url = (
        "https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events/{x}/competitions/{x}/competitors".format(
            x=game_id,
        )
    )
    summary = _download_json(download, summary_url, **kwargs)
    if raw:
        return summary
    items = helper_nhl_game_items(summary)
    team_rosters = helper_nhl_roster_items(items=items, summary_url=summary_url, **kwargs)
    team_rosters = team_rosters.join(items[["team_id", "order", "home_away", "winner"]], how="left", on="team_id")
    teams_df = helper_nhl_team_items(items=items, **kwargs)
    teams_rosters = team_rosters.join(teams_df, how="left", on="team_id")
    athletes = helper_nhl_athlete_items(teams_rosters=team_rosters, **kwargs)
    rosters = athletes.join(teams_rosters, how="left", left_on="athlete_id", right_on="player_id")
    rosters = rosters.with_columns(game_id=pl.lit(game_id).cast(pl.Int64))
    rosters.columns = [underscore(c) for c in rosters.columns]
    rosters = _espn_game_rosters.roster_ids(rosters)
    return _pandas_ids(rosters) if return_as_pandas else rosters


def helper_nhl_game_items(summary):
    items = pl.from_pandas(pd.json_normalize(summary, record_path="items", sep="_"))
    items.columns = [col.replace("$ref", "href") for col in items.columns]

    items.columns = [underscore(c) for c in items.columns]
    # Older games (e.g. pre-2021) sometimes omit the team-level ``statistics``
    # ``$ref`` in the competitors payload, so ``statistics_href`` is absent and a
    # strict rename raises ColumnNotFoundError (the renamed ``team_statistics_href``
    # is not used downstream). Rename only the keys actually present.
    rename_map = {"id": "team_id", "uid": "team_uid", "statistics_href": "team_statistics_href"}
    items = items.rename({k: v for k, v in rename_map.items() if k in items.columns})
    items = items.with_columns(team_id=pl.col("team_id").cast(pl.Int32))

    return items


def helper_nhl_team_items(items, **kwargs):
    return _espn_game_rosters.team_items(items, **kwargs)


def helper_nhl_roster_items(items, summary_url, **kwargs):
    from sportsdataverse.errors import NoDataError

    team_ids = list(items["team_id"])
    game_rosters = pl.DataFrame()
    for tm in team_ids:
        team_roster_url = "{x}/{t}/roster".format(x=summary_url, t=tm)
        try:
            entries = _download_json(download, team_roster_url, **kwargs).get("entries", [])
        except NoDataError:
            # ESPN has no roster resource for this team in this game (a 404 —
            # common for older games). Skip it so the other team's roster is
            # still recovered instead of failing the whole game.
            continue
        if not entries:
            continue
        team_roster = pl.from_pandas(pd.json_normalize(entries, sep="_"))
        team_roster.columns = [col.replace("$ref", "href") for col in team_roster.columns]
        team_roster.columns = [underscore(c) for c in team_roster.columns]
        team_roster = team_roster.with_columns(team_id=pl.lit(tm).cast(pl.Int32))
        game_rosters = pl.concat([game_rosters, team_roster], how="diagonal")
    if game_rosters.is_empty():
        # No team in this game exposes a roster resource — genuinely no data.
        raise NoDataError(f"NoDataError: No roster data found for any team at {summary_url}")
    game_rosters = game_rosters.with_columns(
        player_id=pl.col("player_id").cast(pl.Int64),
        team_id=pl.col("team_id").cast(pl.Int32),
    )
    return game_rosters


def helper_nhl_athlete_items(teams_rosters, **kwargs):
    return _espn_game_rosters.athlete_items(teams_rosters, **kwargs)
