"""ESPN WBB season-level release producers -- polars ports of the script-local
parsers in ``wehoop-wbb-data/R/espn_wbb_0{4,5,6,7}_*_creation.R`` (no wehoop
helpers exist for these datasets; the creation scripts ARE the source).

Four datasets, all reading the raw repo's season-level JSON subtrees:

* ``helper_wbb_rosters`` -- one team's ``wbb/team_rosters/json/{season}/{team_id}.json``.
* ``helper_wbb_player_season_stats`` -- one athlete's
  ``wbb/player_season_stats/json/{season}/{athlete_id}.json`` (parallel-array
  categories; athlete identity joined from the team-rosters lookup because the
  ESPN player-stats payload carries no identity).
* ``helper_wbb_team_season_stats`` -- one team's
  ``wbb/team_stats/json/{season}/{team_id}.json`` (two payload shapes:
  list-of-dicts ``stats`` and legacy parallel arrays).
* ``helper_wbb_standings`` -- the single ``wbb/standings/json/{season}.json``
  (recursive group walk, long per-(team, stat) format).

The R-released parquets are the parity oracles; dtypes mirror them (Int32 ids,
Float64 ``value``, everything else String).
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.wbb.wbb_game_rosters import _rel_chr, _rel_int

__all__ = [
    "build_athlete_identity_lookup",
    "helper_wbb_player_season_stats",
    "helper_wbb_rosters",
    "helper_wbb_standings",
    "helper_wbb_team_season_stats",
]


def _coalesce_chr(*vals: Any) -> str | None:
    """R ``%|%``: treat None / NA / empty string as missing."""
    for v in vals:
        s = _rel_chr(v)
        if s is not None and s != "":
            return s
    return None


def _to_num(x: Any) -> float | None:
    """R ``as.numeric`` best-effort: "-"/"--"/garbage -> NA."""
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _frame(
    rows: list[dict[str, Any]], cols: tuple[str, ...], int32: tuple[str, ...], float64: tuple[str, ...] = ()
) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in cols}, strict=False)
    str_cols = [c for c in cols if c not in int32 and c not in float64]
    return df.with_columns(
        [pl.col(c).cast(pl.Int32, strict=False) for c in int32]
        + [pl.col(c).cast(pl.Float64, strict=False) for c in float64]
        + [pl.col(c).cast(pl.Utf8) for c in str_cols]
    )


# --- 04: rosters ---------------------------------------------------------------

_ROSTER_COLS: tuple[str, ...] = (
    "season",
    "team_id",
    "team_slug",
    "team_abbreviation",
    "team_display_name",
    "team_short_display_name",
    "team_color",
    "team_alternate_color",
    "team_logo",
    "athlete_id",
    "uid",
    "guid",
    "full_name",
    "display_name",
    "short_name",
    "first_name",
    "last_name",
    "jersey",
    "position_abbreviation",
    "position_name",
    "position_id",
    "height",
    "weight",
    "age",
    "date_of_birth",
    "birth_place_city",
    "birth_place_state",
    "birth_place_country",
    "experience_years",
    "experience_display_value",
    "headshot_href",
    "headshot_alt",
    "link_web",
    "status_id",
    "status_name",
    "status_type",
)


def _flatten_athletes(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """R ``flatten_athletes``: unwrap position-group buckets each with items[]."""
    athletes = raw.get("athletes") or raw.get("items") or []
    if not athletes:
        return []
    first = athletes[0]
    if isinstance(first, dict) and first.get("items") is not None:
        return [a for bucket in athletes for a in (bucket.get("items") or [])]
    return athletes


def helper_wbb_rosters(payload: dict, *, season: int, team_id: int | str) -> pl.DataFrame:
    """Parse one team's season roster JSON into the released rosters frame.

    Faithful polars port of ``parse_one_team`` in
    ``espn_wbb_04_rosters_creation.R``. ``athlete_id`` stays String (R keeps
    ``map_chr``); only ``season``/``team_id`` are Int32.

    Args:
        payload: One team's ``wbb/team_rosters/json/{season}/{team_id}.json``.
        season: Season year.
        team_id: The ESPN team id this roster file belongs to.

    Returns:
        pl.DataFrame: One row per rostered athlete; empty frame when none.

    Example:
        Quick start::

            import json
            from sportsdataverse.wbb import helper_wbb_rosters
            payload = json.load(open("52.json", encoding="utf-8"))
            df = helper_wbb_rosters(payload, season=2026, team_id=52)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R data-repo producer this ports.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    flat = _flatten_athletes(payload)
    if not flat:
        return pl.DataFrame()
    team = payload.get("team") or {}
    logos = team.get("logos") or []
    team_logo = team.get("logo") or ((logos[0] or {}).get("href") if logos else None)
    rows: list[dict[str, Any]] = []
    for a in flat:
        pos = a.get("position") or {}
        birth = a.get("birthPlace") or {}
        exp = a.get("experience") or {}
        head = a.get("headshot") or {}
        status = a.get("status") or {}
        links = a.get("links") or []
        rows.append(
            {
                "season": int(season),
                "team_id": _rel_int(team_id),
                "team_slug": _rel_chr(team.get("slug")),
                "team_abbreviation": _rel_chr(team.get("abbreviation")),
                "team_display_name": _rel_chr(team.get("displayName")),
                "team_short_display_name": _rel_chr(team.get("shortDisplayName")),
                "team_color": _rel_chr(team.get("color")),
                "team_alternate_color": _rel_chr(team.get("alternateColor")),
                "team_logo": _rel_chr(team_logo),
                "athlete_id": _rel_chr(a.get("id")),
                "uid": _rel_chr(a.get("uid")),
                "guid": _rel_chr(a.get("guid")),
                "full_name": _coalesce_chr(a.get("fullName"), a.get("displayName")),
                "display_name": _rel_chr(a.get("displayName")),
                "short_name": _rel_chr(a.get("shortName")),
                "first_name": _rel_chr(a.get("firstName")),
                "last_name": _rel_chr(a.get("lastName")),
                "jersey": _rel_chr(a.get("jersey")),
                "position_abbreviation": _rel_chr(pos.get("abbreviation")),
                "position_name": _rel_chr(pos.get("displayName")),
                "position_id": _rel_chr(pos.get("id")),
                "height": _coalesce_chr(a.get("displayHeight"), a.get("height")),
                "weight": _coalesce_chr(a.get("displayWeight"), a.get("weight")),
                "age": _rel_chr(a.get("age")),
                "date_of_birth": _rel_chr(a.get("dateOfBirth")),
                "birth_place_city": _rel_chr(birth.get("city")),
                "birth_place_state": _rel_chr(birth.get("state")),
                "birth_place_country": _rel_chr(birth.get("country")),
                "experience_years": _rel_chr(exp.get("years")),
                "experience_display_value": _rel_chr(exp.get("displayValue")),
                "headshot_href": _rel_chr(head.get("href") if isinstance(head, dict) else head),
                "headshot_alt": _rel_chr(head.get("alt") if isinstance(head, dict) else None),
                "link_web": _rel_chr((links[0] or {}).get("href") if links else None),
                "status_id": _rel_chr(status.get("id")),
                "status_name": _rel_chr(status.get("name")),
                "status_type": _rel_chr(status.get("type")),
            }
        )
    return _frame(rows, _ROSTER_COLS, int32=("season", "team_id"))


# --- 05: player season stats -----------------------------------------------------

_PLAYER_STATS_COLS: tuple[str, ...] = (
    "season",
    "athlete_id",
    "athlete_display_name",
    "athlete_first_name",
    "athlete_last_name",
    "athlete_position_abbreviation",
    "athlete_jersey",
    "team_id",
    "team_display_name",
    "category",
    "stat_label",
    "stat_name",
    "stat_display_name",
    "stat_description",
    "display_value",
    "value",
)


def build_athlete_identity_lookup(rosters: dict[int | str, dict]) -> dict[str, dict[str, Any]]:
    """R ``build_athlete_identity_lookup``: athlete_id -> identity from team rosters.

    Args:
        rosters: Mapping of team_id -> that team's raw roster payload
            (``wbb/team_rosters/json/{season}/{team_id}.json``). NOTE: R walks
            ``raw$athletes`` directly here (no position-bucket unwrap, unlike
            the rosters dataset itself).

    Returns:
        dict: athlete_id (str) -> identity fields for
        :func:`helper_wbb_player_season_stats`.
    """
    lookup: dict[str, dict[str, Any]] = {}
    for tid, roster in rosters.items():
        for ath in (roster or {}).get("athletes") or []:
            aid = _rel_int(ath.get("id"))
            if aid is None:
                continue
            pos = ath.get("position") or {}
            lookup[str(aid)] = {
                "display_name": _coalesce_chr(ath.get("displayName"), ath.get("fullName")),
                "first_name": _rel_chr(ath.get("firstName")),
                "last_name": _rel_chr(ath.get("lastName")),
                "position_abbreviation": _rel_chr(pos.get("abbreviation")),
                "jersey": _rel_chr(ath.get("jersey")),
                "team_id": _rel_int(tid),
                "team_display_name": None,
            }
    return lookup


def _pad(x: list[Any] | None, n: int) -> list[str | None]:
    vals = [_rel_chr(v) for v in (x or [])]
    if len(vals) >= n:
        return vals[:n]
    return vals + [None] * (n - len(vals))


def helper_wbb_player_season_stats(
    payload: dict,
    *,
    season: int,
    athlete_id: int | str,
    identity_lookup: dict[str, dict[str, Any]] | None = None,
) -> pl.DataFrame:
    """Parse one athlete's season-stats JSON into the released long frame.

    Faithful polars port of ``parse_one_athlete`` / ``parse_one_category`` /
    ``extract_athlete_meta`` in ``espn_wbb_05_player_season_stats_creation.R``:
    parallel-array categories pivot long, identity comes from the
    team-rosters lookup with the payload's own ``athlete``/``teams`` blocks as
    fallback.

    Args:
        payload: One athlete's ``wbb/player_season_stats/json/{season}/{athlete_id}.json``.
        season: Season year.
        athlete_id: The ESPN athlete id this stats file belongs to.
        identity_lookup: Output of :func:`build_athlete_identity_lookup`.

    Returns:
        pl.DataFrame: One row per (category, stat); empty frame when the
        payload has no categories.

    Example:
        Quick start::

            import json
            from sportsdataverse.wbb import helper_wbb_player_season_stats
            payload = json.load(open("5240973.json", encoding="utf-8"))
            df = helper_wbb_player_season_stats(payload, season=2026, athlete_id=5240973)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R data-repo producer this ports.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    categories = (
        payload.get("categories")
        or payload.get("statCategories")
        or (payload.get("splits") or {}).get("categories")
        or []
    )
    if not categories:
        return pl.DataFrame()

    ident = (identity_lookup or {}).get(str(_rel_int(athlete_id))) or {}
    # team_display_name lives in the payload's `teams` map keyed by slug.
    teams_map = payload.get("teams") or {}
    team_slug = None
    for cat in categories:
        for s in cat.get("statistics") or []:
            cand = _rel_chr(s.get("teamSlug"))
            if cand:
                team_slug = cand
                break
        if team_slug:
            break
    team_block = teams_map.get(team_slug) or {} if team_slug else {}
    athlete = payload.get("athlete") or {}
    meta = {
        "display_name": _coalesce_chr(ident.get("display_name"), athlete.get("displayName")),
        "first_name": _coalesce_chr(ident.get("first_name"), athlete.get("firstName")),
        "last_name": _coalesce_chr(ident.get("last_name"), athlete.get("lastName")),
        "position_abbreviation": _coalesce_chr(
            ident.get("position_abbreviation"),
            (athlete.get("position") or {}).get("abbreviation"),
        ),
        "jersey": _coalesce_chr(ident.get("jersey"), athlete.get("jersey")),
        "team_id": ident.get("team_id") if ident.get("team_id") is not None else _rel_int(team_block.get("id")),
        "team_display_name": _coalesce_chr(team_block.get("displayName"), ident.get("team_display_name")),
    }

    rows: list[dict[str, Any]] = []
    for cat in categories:
        totals = cat.get("totals") or cat.get("values") or []
        if not totals:
            continue
        vals = [_rel_chr(v) for v in totals]
        n = len(vals)
        cat_name = _coalesce_chr(cat.get("name"), cat.get("displayName"))
        labels = _pad(cat.get("labels"), n)
        names = _pad(cat.get("names"), n)
        display_names = _pad(cat.get("displayNames"), n)
        descriptions = _pad(cat.get("descriptions"), n)
        for i in range(n):
            rows.append(
                {
                    "season": int(season),
                    "athlete_id": _rel_int(athlete_id),
                    "athlete_display_name": meta["display_name"],
                    "athlete_first_name": meta["first_name"],
                    "athlete_last_name": meta["last_name"],
                    "athlete_position_abbreviation": meta["position_abbreviation"],
                    "athlete_jersey": meta["jersey"],
                    "team_id": meta["team_id"],
                    "team_display_name": meta["team_display_name"],
                    "category": cat_name,
                    "stat_label": labels[i],
                    "stat_name": names[i],
                    "stat_display_name": display_names[i],
                    "stat_description": descriptions[i],
                    "display_value": vals[i],
                    "value": _to_num(vals[i]),
                }
            )
    return _frame(
        rows,
        _PLAYER_STATS_COLS,
        int32=("season", "athlete_id", "team_id"),
        float64=("value",),
    )


# --- 06: team season stats -------------------------------------------------------

_TEAM_STATS_COLS: tuple[str, ...] = (
    "season",
    "team_id",
    "team_slug",
    "team_abbreviation",
    "team_display_name",
    "team_short_display_name",
    "team_color",
    "team_alternate_color",
    "team_logo",
    "category",
    "stat_label",
    "stat_name",
    "stat_display_name",
    "stat_description",
    "display_value",
    "value",
)


def helper_wbb_team_season_stats(payload: dict, *, season: int, team_id: int | str) -> pl.DataFrame:
    """Parse one team's season-stats JSON into the released long frame.

    Faithful polars port of ``parse_one_team`` / ``parse_one_category`` /
    ``extract_team_meta`` in ``espn_wbb_06_team_season_stats_creation.R``.
    Handles both payload shapes: Shape A (list-of-dicts ``stats``) and
    Shape B (legacy parallel arrays).

    Args:
        payload: One team's ``wbb/team_stats/json/{season}/{team_id}.json``.
        season: Season year.
        team_id: The ESPN team id this stats file belongs to.

    Returns:
        pl.DataFrame: One row per (category, stat); empty frame when the
        payload has no categories.

    Example:
        Quick start::

            import json
            from sportsdataverse.wbb import helper_wbb_team_season_stats
            payload = json.load(open("52.json", encoding="utf-8"))
            df = helper_wbb_team_season_stats(payload, season=2026, team_id=52)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R data-repo producer this ports.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    categories = (
        payload.get("categories")
        or payload.get("statCategories")
        or (payload.get("splits") or {}).get("categories")
        or ((payload.get("results") or {}).get("stats") or {}).get("categories")
        or []
    )
    if not categories:
        return pl.DataFrame()
    team = payload.get("team") or payload.get("requestedTeam") or {}
    logos = team.get("logos") or []
    meta = {
        "team_slug": _rel_chr(team.get("slug")),
        "team_abbreviation": _rel_chr(team.get("abbreviation")),
        "team_display_name": _rel_chr(team.get("displayName")),
        "team_short_display_name": _rel_chr(team.get("shortDisplayName")),
        "team_color": _rel_chr(team.get("color")),
        "team_alternate_color": _rel_chr(team.get("alternateColor")),
        "team_logo": _rel_chr(team.get("logo") or ((logos[0] or {}).get("href") if logos else None)),
    }

    def _base_row() -> dict[str, Any]:
        return {"season": int(season), "team_id": _rel_int(team_id), **meta}

    rows: list[dict[str, Any]] = []
    for cat in categories:
        cat_name = _coalesce_chr(cat.get("name"), cat.get("displayName"))
        stats_list = cat.get("stats")
        if isinstance(stats_list, list) and stats_list and isinstance(stats_list[0], dict):
            # Shape A: list-of-dicts.
            for s in stats_list:
                row = _base_row()
                row.update(
                    {
                        "category": cat_name,
                        "stat_label": _rel_chr(s.get("shortDisplayName")),
                        "stat_name": _rel_chr(s.get("name")),
                        "stat_display_name": _rel_chr(s.get("displayName")),
                        "stat_description": _rel_chr(s.get("description")),
                        "display_value": _rel_chr(s.get("displayValue")),
                        "value": _to_num(s.get("value")),
                    }
                )
                rows.append(row)
            continue
        # Shape B: parallel arrays.
        totals = cat.get("totals") or cat.get("values") or []
        if not totals:
            continue
        vals = [_rel_chr(v) for v in totals]
        n = len(vals)
        labels = _pad(cat.get("labels"), n)
        names = _pad(cat.get("names"), n)
        display_names = _pad(cat.get("displayNames"), n)
        descriptions = _pad(cat.get("descriptions"), n)
        for i in range(n):
            row = _base_row()
            row.update(
                {
                    "category": cat_name,
                    "stat_label": labels[i],
                    "stat_name": names[i],
                    "stat_display_name": display_names[i],
                    "stat_description": descriptions[i],
                    "display_value": vals[i],
                    "value": _to_num(vals[i]),
                }
            )
            rows.append(row)
    return _frame(rows, _TEAM_STATS_COLS, int32=("season", "team_id"), float64=("value",))


# --- 07: standings ---------------------------------------------------------------

_STANDINGS_COLS: tuple[str, ...] = (
    "season",
    "group_id",
    "group_name",
    "group_abbreviation",
    "group_short_name",
    "team_id",
    "team_uid",
    "team_slug",
    "team_location",
    "team_name",
    "team_abbreviation",
    "team_display_name",
    "team_short_display_name",
    "team_color",
    "team_alternate_color",
    "team_logo",
    "stat_name",
    "stat_display_name",
    "stat_short_display_name",
    "stat_description",
    "stat_abbreviation",
    "stat_type",
    "display_value",
    "value",
)


def helper_wbb_standings(payload: dict, *, season: int) -> pl.DataFrame:
    """Parse the season standings JSON into the released long frame.

    Faithful polars port of ``walk_groups`` / ``parse_one_entry`` in
    ``espn_wbb_07_standings_creation.R``: recursive descent through
    ``children``/``groups`` buckets, one row per (team, stat).

    Args:
        payload: The season's ``wbb/standings/json/{season}.json`` as a dict.
        season: Season year.

    Returns:
        pl.DataFrame: One row per (group, team, stat), deduped; empty frame
        when no entries parse.

    Example:
        Quick start::

            import json
            from sportsdataverse.wbb import helper_wbb_standings
            payload = json.load(open("2026.json", encoding="utf-8"))
            df = helper_wbb_standings(payload, season=2026)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R data-repo producer this ports.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    rows: list[dict[str, Any]] = []

    def _walk(node: dict[str, Any]) -> None:
        group_meta = {
            "group_id": _rel_chr(node.get("id")),
            "group_name": _coalesce_chr(node.get("name"), node.get("displayName")),
            "group_abbreviation": _rel_chr(node.get("abbreviation")),
            "group_short_name": _rel_chr(node.get("shortName")),
        }
        entries = (node.get("standings") or {}).get("entries") or node.get("entries") or []
        for entry in entries:
            team = entry.get("team") or {}
            stats = entry.get("stats") or []
            if not stats:
                continue
            logos = team.get("logos") or []
            team_logo = team.get("logo") or ((logos[0] or {}).get("href") if logos else None)
            for s in stats:
                rows.append(
                    {
                        "season": int(season),
                        **group_meta,
                        "team_id": _rel_int(team.get("id")),
                        "team_uid": _rel_chr(team.get("uid")),
                        "team_slug": _rel_chr(team.get("slug")),
                        "team_location": _rel_chr(team.get("location")),
                        "team_name": _rel_chr(team.get("name")),
                        "team_abbreviation": _rel_chr(team.get("abbreviation")),
                        "team_display_name": _rel_chr(team.get("displayName")),
                        "team_short_display_name": _rel_chr(team.get("shortDisplayName")),
                        "team_color": _rel_chr(team.get("color")),
                        "team_alternate_color": _rel_chr(team.get("alternateColor")),
                        "team_logo": _rel_chr(team_logo),
                        "stat_name": _rel_chr(s.get("name")),
                        "stat_display_name": _rel_chr(s.get("displayName")),
                        "stat_short_display_name": _rel_chr(s.get("shortDisplayName")),
                        "stat_description": _rel_chr(s.get("description")),
                        "stat_abbreviation": _rel_chr(s.get("abbreviation")),
                        "stat_type": _rel_chr(s.get("type")),
                        "display_value": _rel_chr(s.get("displayValue")),
                        "value": _to_num(_rel_chr(s.get("value"))),
                    }
                )
        for child in node.get("children") or node.get("groups") or []:
            _walk(child)

    _walk(payload)
    df = _frame(rows, _STANDINGS_COLS, int32=("season", "team_id"), float64=("value",))
    if df.is_empty():
        return df
    # R: season-level distinct().
    return df.unique(maintain_order=True, keep="first")
