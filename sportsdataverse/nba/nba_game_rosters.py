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


def espn_nba_game_rosters(
    game_id: int, raw=False, return_as_pandas=False, **kwargs
) -> pl.DataFrame | pd.DataFrame | dict:
    """espn_nba_game_rosters() - Pull the game by id.

    Args:
        game_id (int): Unique game_id, can be obtained from espn_nba_schedule().
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
        ``position_abbreviation`` and ``position_leaf``. With ``raw=True``, the competitors
        payload (dict).

    Raises:
        NoDataError: ESPN has no competitors, roster or athlete resource for the game (404), or every
            rostered athlete's ``$ref`` answered 404. A single 404 athlete is left out with a
            ``UserWarning`` and the rest of the roster is returned.
        AssetFetchError: a fetch failed (403, 429, 5xx, a dropped connection); never treated as a
            missing athlete.

    Example:
        Quick start::

            from sportsdataverse.nba import espn_nba_game_rosters
            rosters = espn_nba_game_rosters(game_id=401585183)
            print(rosters.shape)

        Pandas round-trip::

            rosters_pd = espn_nba_game_rosters(game_id=401585183, return_as_pandas=True)
            rosters_pd.head()

        Pipeline next step (filter to game starters)::

            import polars as pl
            starters = espn_nba_game_rosters(game_id=401585183).filter(
                pl.col("starter") == True
            )

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA rosters
            * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball parallel
            * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API
    """
    # summary endpoint for pickcenter array
    summary_url = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{x}/competitions/{x}/competitors".format(
        x=game_id,
    )
    summary = _download_json(download, summary_url, **kwargs)
    if raw:
        return summary
    items = helper_nba_game_items(summary)
    team_rosters = helper_nba_roster_items(items=items, summary_url=summary_url, **kwargs)
    team_rosters = team_rosters.join(items[["team_id", "order", "home_away", "winner"]], how="left", on="team_id")
    teams_df = helper_nba_team_items(items=items, **kwargs)
    teams_rosters = team_rosters.join(teams_df, how="left", on="team_id")
    athletes = helper_nba_athlete_items(teams_rosters=team_rosters, **kwargs)
    rosters = athletes.join(teams_rosters, how="left", left_on="athlete_id", right_on="player_id")
    rosters = rosters.with_columns(game_id=pl.lit(game_id).cast(pl.Int64))
    rosters.columns = [underscore(c) for c in rosters.columns]
    rosters = _espn_game_rosters.roster_ids(rosters)
    return _pandas_ids(rosters) if return_as_pandas else rosters


helper_nba_game_items = _espn_game_rosters.game_items


helper_nba_team_items = _espn_game_rosters.team_items


helper_nba_roster_items = _espn_game_rosters.roster_items


helper_nba_athlete_items = _espn_game_rosters.athlete_items


def helper_nba_game_rosters(payload: dict, *, season: int, game_id: int | str) -> pl.DataFrame:
    """Parse one game's rosters sidecar into the released game-rosters frame.

    Faithful polars port of the script-local parsers in
    ``hoopR-nba-data/R/espn_nba_09_game_rosters_creation.R`` -- byte-identical
    to the WBB parser after league normalization (a single comment-typo diff),
    so this delegates to the shared implementation (NOT the live
    ``espn_nba_game_rosters`` pipeline above; this one consumes the stored
    ``nba/game_rosters/json/{game_id}.json`` sidecar). The R-released
    ``espn_nba_game_rosters`` parquet is the parity oracle.

    Args:
        payload: One game's ``nba/game_rosters/json/{game_id}.json`` as a dict.
        season: Season year the sidecar belongs to.
        game_id: ESPN game id the sidecar belongs to (released dtype String).

    Returns:
        pl.DataFrame: One row per rostered athlete; empty (zero-column) frame
        for degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_game_rosters
            payload = json.load(open("401766128.json", encoding="utf-8"))
            df = helper_nba_game_rosters(payload, season=2025, game_id=401766128)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    from sportsdataverse.wbb.wbb_game_rosters import helper_wbb_game_rosters

    return helper_wbb_game_rosters(payload, season=season, game_id=game_id)
