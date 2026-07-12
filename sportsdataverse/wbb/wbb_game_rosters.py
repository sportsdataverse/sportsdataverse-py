from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, normalize_team_roster_columns, underscore


def espn_wbb_game_rosters(game_id: int, raw=False, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_wbb_game_rosters() - Pull the game by id.

    Args:
        game_id (int): Unique game_id, can be obtained from wbb_schedule().
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
        'team_location', 'team_name', 'team_nickname', 'team_abbreviation',
        'team_display_name', 'team_short_display_name', 'team_color',
        'team_alternate_color', 'is_active', 'is_all_star',
        'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'

    Example:
        Quick start (2024 NCAA W championship game)::

            from sportsdataverse.wbb import espn_wbb_game_rosters
            roster = espn_wbb_game_rosters(game_id=401587902)
            print(roster.shape)

        Identify starters::

            import polars as pl
            starters = roster.filter(pl.col("starter") == True).select(
                ["full_name", "jersey", "team_display_name"]
            )

        Pandas round-trip::

            roster_pd = espn_wbb_game_rosters(game_id=401587902, return_as_pandas=True)
            roster_pd.head()

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    # summary endpoint for pickcenter array
    summary_url = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events/{x}/competitions/{x}/competitors".format(
        x=game_id,
    )
    summary_resp = download(summary_url, **kwargs)
    summary = summary_resp.json()
    items = helper_wbb_game_items(summary)
    team_rosters = helper_wbb_roster_items(items=items, summary_url=summary_url, **kwargs)
    team_rosters = team_rosters.join(items[["team_id", "order", "home_away", "winner"]], how="left", on="team_id")
    teams_df = helper_wbb_team_items(items=items, **kwargs)
    teams_rosters = team_rosters.join(teams_df, how="left", on="team_id")
    athletes = helper_wbb_athlete_items(teams_rosters=team_rosters, **kwargs)
    rosters = athletes.join(teams_rosters, how="left", left_on="athlete_id", right_on="player_id")
    rosters = rosters.with_columns(game_id=pl.lit(game_id).cast(pl.Int32))
    rosters.columns = [underscore(c) for c in rosters.columns]
    return rosters.to_pandas() if return_as_pandas else rosters


def helper_wbb_game_items(summary):
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


def helper_wbb_team_items(items, **kwargs):
    pop_cols = [
        "$ref",
        "record",
        "athletes",
        "venue",
        "groups",
        "ranks",
        "statistics",
        "leaders",
        "links",
        "notes",
        "againstTheSpreadRecords",
        "franchise",
        "events",
        "college",
    ]
    teams_df = pl.DataFrame()
    for x in items["team_href"]:
        team = download(x, **kwargs).json()
        for k in pop_cols:
            team.pop(k, None)
        team_row = pl.from_pandas(pd.json_normalize(team, sep="_"))
        teams_df = pl.concat([teams_df, team_row], how="diagonal")

    teams_df = normalize_team_roster_columns(teams_df)
    if "logos" in teams_df.columns:
        try:
            teams_df = teams_df.with_columns(
                logo_href=pl.col("logos").list.get(0).struct.field("href").fill_null(""),
                logo_dark_href=pl.col("logos").list.get(1).struct.field("href").fill_null(""),
            ).drop("logos")
        except Exception:
            teams_df = teams_df.with_columns(logo_href=pl.lit(""), logo_dark_href=pl.lit("")).drop(["logos"])
    else:
        teams_df = teams_df.with_columns(logo_href=pl.lit(""), logo_dark_href=pl.lit(""))
    teams_df = teams_df.with_columns(team_id=pl.col("team_id").cast(pl.Int32))
    return teams_df


def helper_wbb_roster_items(items, summary_url, **kwargs):
    from sportsdataverse.errors import NoESPNDataError

    team_ids = list(items["team_id"])
    game_rosters = pl.DataFrame()
    for tm in team_ids:
        team_roster_url = "{x}/{t}/roster".format(x=summary_url, t=tm)
        try:
            team_roster_resp = download(team_roster_url, **kwargs)
        except NoESPNDataError:
            # ESPN has no roster resource for this team in this game (a 404 —
            # common for older games and non-D1 opponents). Skip it so the other
            # team's roster is still recovered instead of failing the whole game.
            continue
        entries = team_roster_resp.json().get("entries", [])
        if not entries:
            continue
        team_roster = pl.from_pandas(pd.json_normalize(entries, sep="_"))
        team_roster.columns = [col.replace("$ref", "href") for col in team_roster.columns]
        team_roster.columns = [underscore(c) for c in team_roster.columns]
        team_roster = team_roster.with_columns(team_id=pl.lit(tm).cast(pl.Int32))
        game_rosters = pl.concat([game_rosters, team_roster], how="diagonal")
    if game_rosters.is_empty():
        # No team in this game exposes a roster resource — genuinely no data.
        raise NoESPNDataError(f"NoESPNDataError: No roster data found for any team at {summary_url}")
    game_rosters = game_rosters.drop([c for c in ["period", "for_player_id", "active"] if c in game_rosters.columns])
    game_rosters = game_rosters.with_columns(
        player_id=pl.col("player_id").cast(pl.Int64),
        team_id=pl.col("team_id").cast(pl.Int32),
    )
    return game_rosters


def helper_wbb_athlete_items(teams_rosters, **kwargs):
    athlete_hrefs = list(teams_rosters["athlete_href"])
    game_athletes = pl.DataFrame()
    pop_cols = [
        "links",
        "injuries",
        "teams",
        "team",
        "college",
        "proAthlete",
        "statistics",
        "notes",
        "eventLog",
        "$ref",
        "position",
    ]
    for athlete_href in athlete_hrefs:
        athlete_res = download(athlete_href, **kwargs)
        athlete_resp = athlete_res.json()
        for k in pop_cols:
            athlete_resp.pop(k, None)
        athlete = pl.from_pandas(pd.json_normalize(athlete_resp, sep="_"))
        athlete.columns = [col.replace("$ref", "href") for col in athlete.columns]
        athlete.columns = [underscore(c) for c in athlete.columns]

        game_athletes = pl.concat([game_athletes, athlete], how="diagonal")

    game_athletes = game_athletes.rename(
        {
            "id": "athlete_id",
            "uid": "athlete_uid",
            "guid": "athlete_guid",
            "type": "athlete_type",
            "display_name": "athlete_display_name",
        },
    )
    game_athletes = game_athletes.with_columns(athlete_id=pl.col("athlete_id").cast(pl.Int64))
    return game_athletes


# --- release producer (wehoop-wbb-data parity) --------------------------------


def _rel_chr(x: object) -> str | None:
    """R ``safe_chr``: NULL/empty -> NA; else first element as character."""
    if x is None:
        return None
    if isinstance(x, list):
        return str(x[0]) if x else None
    return str(x)


def _rel_int(x: object) -> int | None:
    """R ``safe_int``: NULL/empty -> NA; else first element as.integer."""
    if isinstance(x, list):
        x = x[0] if x else None
    if x is None:
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        try:
            return int(float(x))
        except (TypeError, ValueError):
            return None


def _rel_bool(x: object) -> bool | None:
    """R ``as.logical(x %||% NA)``."""
    if x is None:
        return None
    return bool(x)


_GAME_ROSTER_COLS = (
    "season",
    "game_id",
    "team_id",
    "team_slug",
    "team_abbreviation",
    "team_display_name",
    "home_away",
    "athlete_id",
    "athlete_uid",
    "athlete_guid",
    "athlete_display_name",
    "athlete_short_name",
    "athlete_first_name",
    "athlete_last_name",
    "athlete_jersey",
    "athlete_position",
    "athlete_headshot",
    "starter",
    "did_not_play",
    "active",
    "ejected",
    "reason",
)


def helper_wbb_game_rosters(payload: dict, *, season: int, game_id: int | str) -> pl.DataFrame:
    """Parse one game's rosters sidecar into the released game-rosters frame.

    Faithful polars port of the script-local ``parse_one_game`` /
    ``parse_one_athlete`` in ``wehoop-wbb-data/R/espn_wbb_08_game_rosters_creation.R``
    (no wehoop helper exists for this dataset). The stored sidecar is a
    summary-shaped payload, so the roster source falls through to
    ``boxscore.players`` and athletes to ``statistics[0].athletes``. The
    R-released parquet is the parity oracle (``game_id`` stays String; ids
    Int32).

    Args:
        payload: One game's ``wbb/game_rosters/json/{game_id}.json`` as a dict.
        season: Season year the game belongs to.
        game_id: The ESPN game id (kept as character, matching R).

    Returns:
        pl.DataFrame: One row per rostered athlete, deduped. Empty
        (zero-column) frame when no rosters are present.

    Example:
        Quick start::

            import json
            from sportsdataverse.wbb import helper_wbb_game_rosters
            payload = json.load(open("401804834.json", encoding="utf-8"))
            df = helper_wbb_game_rosters(payload, season=2026, game_id=401804834)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R data-repo producer this ports.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    rosters = payload.get("rosters") or payload.get("teams") or (payload.get("boxscore") or {}).get("players") or []
    if not rosters:
        return pl.DataFrame()
    rows: list[dict] = []
    for team_block in rosters:
        team = team_block.get("team") or {}
        athletes = team_block.get("roster") or team_block.get("athletes") or []
        if not athletes:
            stats = team_block.get("statistics")
            if isinstance(stats, list) and stats and isinstance(stats[0], dict):
                athletes = stats[0].get("athletes") or []
        if not athletes:
            continue
        for entry in athletes:
            ath = entry.get("athlete") or entry
            position = (
                (entry.get("position") or {}).get("abbreviation") if isinstance(entry.get("position"), dict) else None
            )
            if position is None:
                pos = ath.get("position") or {}
                position = pos.get("abbreviation") or pos.get("name") if isinstance(pos, dict) else None
            headshot = ath.get("headshot")
            if isinstance(headshot, dict):
                headshot = headshot.get("href")
            rows.append(
                {
                    "season": int(season),
                    "game_id": str(game_id),
                    "team_id": _rel_int(team.get("id") if team else team_block.get("id")),
                    "team_slug": _rel_chr((team or team_block).get("slug")),
                    "team_abbreviation": _rel_chr((team or team_block).get("abbreviation")),
                    "team_display_name": _rel_chr((team or team_block).get("displayName")),
                    "home_away": _rel_chr(team_block.get("homeAway")),
                    "athlete_id": _rel_int(ath.get("id")),
                    "athlete_uid": _rel_chr(ath.get("uid")),
                    "athlete_guid": _rel_chr(ath.get("guid")),
                    "athlete_display_name": _rel_chr(ath.get("displayName")),
                    "athlete_short_name": _rel_chr(ath.get("shortName")),
                    "athlete_first_name": _rel_chr(ath.get("firstName")),
                    "athlete_last_name": _rel_chr(ath.get("lastName")),
                    "athlete_jersey": _rel_chr(
                        entry.get("jersey") if entry.get("jersey") is not None else ath.get("jersey")
                    ),
                    "athlete_position": _rel_chr(position),
                    "athlete_headshot": _rel_chr(headshot),
                    "starter": _rel_bool(entry.get("starter") if "starter" in entry else ath.get("starter")),
                    "did_not_play": _rel_bool(
                        entry.get("didNotPlay") if "didNotPlay" in entry else ath.get("didNotPlay")
                    ),
                    "active": _rel_bool(entry.get("active") if "active" in entry else ath.get("active")),
                    "ejected": _rel_bool(entry.get("ejected") if "ejected" in entry else ath.get("ejected")),
                    "reason": _rel_chr(entry.get("reason") if entry.get("reason") is not None else ath.get("reason")),
                }
            )
    if not rows:
        return pl.DataFrame()
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in _GAME_ROSTER_COLS}, strict=False)
    int_cols = ("season", "team_id", "athlete_id")
    bool_cols = ("starter", "did_not_play", "active", "ejected")
    # Every remaining column is R character -- pin Utf8 so an all-null column
    # (e.g. home_away on summary-shaped sidecars) can't infer as Null dtype.
    str_cols = [c for c in _GAME_ROSTER_COLS if c not in int_cols and c not in bool_cols]
    df = df.with_columns(
        [pl.col(c).cast(pl.Int32, strict=False) for c in int_cols]
        + [pl.col(c).cast(pl.Boolean) for c in bool_cols]
        + [pl.col(c).cast(pl.Utf8) for c in str_cols]
    )
    # R: season-level distinct() -- per-game unique yields the identical set.
    return df.unique(maintain_order=True, keep="first")
