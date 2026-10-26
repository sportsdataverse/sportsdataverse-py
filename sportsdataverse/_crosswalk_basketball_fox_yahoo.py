"""Fox game ids and Yahoo team / game ids for the basketball crosswalks.

Two optional legs on top of the ESPN-anchored crosswalks, both fail-soft: a
fetch that cannot be produced logs a warning and leaves its columns null, it
never fails the build.

* **Fox games.** One ``{sport}/league/scores-segment/{YYYYMMDD}`` call per ET
  date (``cbk`` / ``wcbk`` with ``groupId=2``, Fox's Division I slate). A Fox
  game lands on the schedule row on the same ET date whose Fox-known teams are
  exactly the Fox game's resolved teams, so a neutral-site home/away flip still
  matches and a game against a team Fox cannot resolve still matches through
  the other side. Fox team ids resolve one-to-one through the team crosswalk's
  ``fox_team_id`` and, failing that, the normalized team name (the same name
  join the team crosswalks use, aliases included).
* **Yahoo teams.** One ``leagueTeams`` call (college: Division I by division,
  which is not capped at 1000 rows the way the flat listing is), joined to ESPN
  on the normalized full name, then the short school name, then the nickname
  (only where the school names share a word or an acronym), each tier one-to-one
  among the rows still unmatched. Yahoo lists historical teams under
  today's franchise names, as ESPN does.
* **Yahoo game ids.** ``{lg}.g.{YYYYMMDD ET}{home Yahoo team id}`` with the home
  id zero-padded to 3 digits (WNBA) or 4 (NCAAB / NCAAW). Probed 2026-10-11
  against ``leagueGameIdsByDate``: WNBA 2025 311/312 games, MBB 2025-26
  5530 + 112 neutral-site flips, WBB 2025-26 5477 + 99 flips; the rest are games
  whose home team is not Division I. The computed id is checked against the
  real ids Yahoo lists over the schedule's dates (``season=`` is required for a
  past season), so a neutral-site game takes whichever side Yahoo calls home
  and a game Yahoo does not list stays null. When that list cannot be fetched
  the unverified home-side id is kept.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set

import polars as pl

from sportsdataverse._common_crosswalk_basketball import normalize_college_team, normalize_team, str_id, to_eastern
from sportsdataverse._temporal import as_date
from sportsdataverse._crosswalk_basketball_sources import CrosswalkSourceError, FetchTally, require_source

logger = logging.getLogger(__name__)

# Fox Bifrost sport slug per league; ``groupId=2`` is the "DIVISION I" entry of
# the cbk / wcbk scores ``groupList`` (probed 2026-10-11).
_FOX_SPORT = {"nba": "nba", "wnba": "wnba", "mbb": "cbk", "wbb": "wcbk"}
_FOX_D1_GROUP = 2
# Yahoo league slug and the zero-padded width of the home team id in a game id.
_YAHOO = {"wnba": ("wnba", 3), "mbb": ("ncaab", 4), "wbb": ("ncaaw", 4)}
# leagueGameIdsByDate (and the flat leagueTeams listing) stop at 1000 rows; a
# busy college week holds about 450 games.
_YAHOO_ROW_CAP = 1000
_YAHOO_CHUNK_DAYS = 7

FOX_GAMES_SCHEMA = {
    "fox_game_id": pl.Utf8,
    "game_date": pl.Date,
    "fox_home_team_id": pl.Utf8,
    "fox_away_team_id": pl.Utf8,
    "fox_home_team": pl.Utf8,
    "fox_away_team": pl.Utf8,
}
YAHOO_TEAMS_SCHEMA = {
    "yahoo_team_id": pl.Utf8,
    "yahoo_team_abbreviation": pl.Utf8,
    "yahoo_team_name": pl.Utf8,
    "yahoo_display_name": pl.Utf8,
    "yahoo_nickname": pl.Utf8,
}


def fox_scoreboard_games(league: str, dates: Sequence[date], **kwargs: Any) -> pl.DataFrame:
    """One Fox scores-segment call per ET date, unioned to one row per Fox game.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        dates: ET calendar dates to fetch.
        **kwargs: Forwarded to the Fox getter.

    Returns:
        ``pl.DataFrame`` with :data:`FOX_GAMES_SCHEMA`; ``game_date`` is the ET
        date of Fox's ``eventTime``.

    Raises:
        CrosswalkSourceError: Every date's fetch failed and none answered.
    """
    from sportsdataverse._fox_layout import fox_get, parse_segment_events

    sport = _FOX_SPORT[league]
    params = {"groupId": _FOX_D1_GROUP} if league in ("mbb", "wbb") else None
    tally = FetchTally(f"fox_{league}_scores_segment")
    rows: List[Dict[str, Any]] = []
    for day in dates:
        seg = day.strftime("%Y%m%d")

        def _day(s: str = seg) -> Dict[str, Any]:
            return fox_get(f"{sport}/league/scores-segment/{s}", params, **kwargs)

        rows += parse_segment_events(tally.fetch(f"segment_id={seg}", _day) or {}, seg)
    tally.finish()
    out = pl.DataFrame(
        [
            {
                "fox_game_id": None if r["game_id"] is None else str(r["game_id"]),
                "game_date": to_eastern(r["date"]),
                "fox_home_team_id": r["home_team_id"],
                "fox_away_team_id": r["away_team_id"],
                "fox_home_team": r["home_team"],
                "fox_away_team": r["away_team"],
            }
            for r in rows
        ],
        schema=FOX_GAMES_SCHEMA,
    )
    return out.filter(pl.col("fox_game_id").is_not_null()).unique("fox_game_id", keep="first", maintain_order=True)


def fox_team_map(
    team_xwalk: pl.DataFrame, fox_games: pl.DataFrame, alias: Optional[Mapping[str, str]] = None
) -> Dict[str, int]:
    """Fox team id -> ESPN team id, from the team crosswalk then the team name.

    Args:
        team_xwalk: A team crosswalk (``espn_team_id``, ``espn_display_name``
            and, when it has one, ``fox_team_id``).
        fox_games: Output of :func:`fox_scoreboard_games`.
        alias: Normalized Fox name -> normalized ESPN name bridge (the league's
            ``FOX_DISPLAY_ALIAS``).

    Returns:
        ``{fox_team_id: espn_team_id}``, one-to-one. A name two ESPN teams share
        maps nowhere, and neither does a name two Fox ids share or one that
        lands on an ESPN team the crosswalk already gave another Fox id.
    """
    alias = alias or {}
    out: Dict[str, int] = {}
    if "fox_team_id" in team_xwalk.columns:
        ids = team_xwalk.select(str_id(team_xwalk, "fox_team_id")).to_series().to_list()
        for fox_id, espn_id in zip(ids, team_xwalk["espn_team_id"].to_list()):
            if fox_id is not None and espn_id is not None:
                out.setdefault(fox_id, int(espn_id))
    by_name: Dict[str, Optional[int]] = {}
    for name, espn_id in zip(team_xwalk["espn_display_name"].to_list(), team_xwalk["espn_team_id"].to_list()):
        key = normalize_team(name)
        if key and espn_id is not None:
            by_name[key] = None if key in by_name else int(espn_id)
    claimed = set(out.values())
    named: Dict[str, int] = {}
    for side in ("home", "away"):
        for fox_id, name in fox_games.select(f"fox_{side}_team_id", f"fox_{side}_team").iter_rows():
            if fox_id is None or fox_id in out:
                continue
            key = normalize_team(name)
            espn_id = by_name.get(alias.get(key, key))
            if espn_id is not None and espn_id not in claimed:
                named[fox_id] = espn_id
    counts: Dict[int, int] = {}
    for espn_id in named.values():
        counts[espn_id] = counts.get(espn_id, 0) + 1
    out.update({fox_id: espn_id for fox_id, espn_id in named.items() if counts[espn_id] == 1})
    return out


def _one_to_one(sched_long: pl.DataFrame, fox_long: pl.DataFrame, need: pl.DataFrame, min_teams: int) -> pl.DataFrame:
    """``(_row, fox_game_id)`` pairs whose Fox-known teams are the same set, unique both ways.

    ``sched_long`` carries only the row teams Fox knows, and ``need`` both the
    Fox game's resolved-team count and the row's Fox-known count: a Fox game
    with one resolved team only takes a row whose other team Fox cannot place,
    never a row against another team Fox lists.
    """
    return (
        sched_long.join(fox_long, on=["game_date", "espn_team_id"])
        .group_by("_row", "fox_game_id")
        .agg(pl.len().alias("_n"))
        .join(need, on="fox_game_id")
        .join(sched_long.group_by("_row").agg(pl.len().alias("_known")), on="_row")
        .filter((pl.col("_n") == pl.col("_need")) & (pl.col("_n") == pl.col("_known")) & (pl.col("_n") >= min_teams))
        .filter((pl.len().over("_row") == 1) & (pl.len().over("fox_game_id") == 1))
        .select("_row", "fox_game_id")
    )


def attach_fox_games(schedule: pl.DataFrame, fox_games: pl.DataFrame, fox_to_espn: Mapping[str, int]) -> pl.DataFrame:
    """Fill ``fox_game_id`` (and ``fox_home/away_team_id`` where present) on a schedule crosswalk.

    A Fox game matches the one schedule row on its ET date whose teams Fox
    knows are exactly the Fox game's resolved teams (one side is enough when
    Fox can place neither the game's other team nor the row's), and that row
    matches no other Fox game. Rows and Fox games left over then match when
    Fox's date is the day after ESPN's and both Fox teams resolve: ESPN files a
    late West-coast or Hawaii tip at 23:59 ET, where Fox has 00:00 ET the next
    day. ``fox_home_team_id`` / ``fox_away_team_id`` are Fox's own home and
    away, which can be the reverse of ESPN's at a neutral site.

    Args:
        schedule: Schedule crosswalk with ``game_date``, ``home_espn_team_id``,
            ``away_espn_team_id`` and ``fox_game_id``.
        fox_games: Output of :func:`fox_scoreboard_games`.
        fox_to_espn: Output of :func:`fox_team_map`.

    Returns:
        ``schedule`` with the Fox columns filled, same rows and column order.
    """
    fox_cols = [c for c in ("fox_game_id", "fox_home_team_id", "fox_away_team_id") if c in schedule.columns]
    rows = schedule.with_row_index("_row")
    fox = fox_games.with_columns(
        pl.col(f"fox_{s}_team_id")
        .replace_strict(list(fox_to_espn), list(fox_to_espn.values()), default=None, return_dtype=pl.Int32)
        .alias(f"_{s}_espn")
        for s in ("home", "away")
    )
    # Both join keys are cast here, not asserted: a Datetime or String game_date or an
    # Int64 team id from a caller's frame must not change which rows match.
    known = pl.Series(sorted(set(fox_to_espn.values())), dtype=pl.Int32).implode()
    sched_long = (
        pl.concat(
            rows.select(
                "_row",
                as_date(pl.col("game_date")).alias("game_date"),
                pl.col(f"{s}_espn_team_id").cast(pl.Int32, strict=False).alias("espn_team_id"),
            )
            for s in ("home", "away")
        )
        .drop_nulls()
        .filter(pl.col("espn_team_id").is_in(known))
    )
    fox_long = pl.concat(
        fox.select(
            "fox_game_id", as_date(pl.col("game_date")).alias("game_date"), pl.col(f"_{s}_espn").alias("espn_team_id")
        )
        for s in ("home", "away")
    ).drop_nulls()
    need = fox_long.group_by("fox_game_id").agg(pl.len().alias("_need"))
    same_day = _one_to_one(sched_long, fox_long, need, 1)
    rest_rows = sched_long.join(same_day, on="_row", how="anti")
    rest_fox = fox_long.join(same_day, on="fox_game_id", how="anti")
    # Fox's date is the day after ESPN's for a tip ESPN files at 23:59 ET; never the reverse.
    shifted = rest_fox.with_columns(pl.col("game_date").dt.offset_by("-1d"))
    pairs = (
        pl.concat([same_day, _one_to_one(rest_rows, shifted, need, 2)])
        .join(fox.select("fox_game_id", "fox_home_team_id", "fox_away_team_id"), on="fox_game_id")
        .select("_row", *fox_cols)
    )
    return rows.drop(fox_cols).join(pairs, on="_row", how="left", maintain_order="left").select(schedule.columns)


def with_fox_games(
    league: str,
    schedule: pl.DataFrame,
    team_xwalk: pl.DataFrame,
    *,
    alias: Optional[Mapping[str, str]] = None,
    **kwargs: Any,
) -> pl.DataFrame:
    """Fetch Fox's slate for every schedule date and fill the Fox game columns, fail-soft.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        schedule: Schedule crosswalk (see :func:`attach_fox_games`).
        team_xwalk: The league's team crosswalk (see :func:`fox_team_map`).
        alias: The league's ``FOX_DISPLAY_ALIAS``.
        **kwargs: Forwarded to the Fox getter.

    Returns:
        ``schedule`` with the Fox columns filled; unchanged (and a warning
        logged) when the Fox leg fails anywhere: no date could be fetched, a
        payload would not parse, or the frames would not join.
    """
    try:
        dates = schedule.select(as_date(pl.col("game_date"))).to_series().drop_nulls().unique().sort().to_list()
        fox_games = fox_scoreboard_games(league, dates, **kwargs)
        return attach_fox_games(schedule, fox_games, fox_team_map(team_xwalk, fox_games, alias))
    except Exception as exc:  # the leg is optional: any failure leaves the Fox columns null
        logger.warning("%s: Fox games unavailable, fox_game_id left null: %s: %s", league, type(exc).__name__, exc)
        return schedule


def yahoo_teams(league: str, **kwargs: Any) -> pl.DataFrame:
    """Yahoo's team listing for a league (college: Division I only).

    Args:
        league: ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        **kwargs: Forwarded to the Yahoo getter.

    Returns:
        ``pl.DataFrame`` with :data:`YAHOO_TEAMS_SCHEMA`; ``yahoo_team_id`` is the
        dotted id (``"ncaab.t.376"``).

    Raises:
        CrosswalkSourceError: The fetch failed or listed no teams.
    """
    slug, _ = _YAHOO[league]

    def fetch() -> pl.DataFrame:
        from sportsdataverse.providers.yahoo.yahoo_shangrila import yahoo_league_teams

        extra = {"division_ids": "1", "get_teams_by_division": "true"} if league in ("mbb", "wbb") else {}
        raw = yahoo_league_teams(league=slug, return_parsed=False, **extra, **kwargs)
        teams: List[Dict[str, Any]] = []
        for lg in (raw.get("data") or {}).get("leagues") or []:
            teams += lg.get("teams") or []
            for div in lg.get("divisions") or []:
                teams += div.get("teams") or []
        return pl.DataFrame(
            [
                {
                    "yahoo_team_id": t.get("teamId"),
                    "yahoo_team_abbreviation": t.get("abbreviation"),
                    "yahoo_team_name": t.get("fullName"),
                    "yahoo_display_name": t.get("displayName"),
                    "yahoo_nickname": t.get("nickname"),
                }
                for t in teams
            ],
            schema=YAHOO_TEAMS_SCHEMA,
        ).unique("yahoo_team_id", keep="first", maintain_order=True)

    out = require_source(f"yahoo_league_teams(league={slug!r})", fetch)
    if out.height == 0 or out.height >= _YAHOO_ROW_CAP:
        raise CrosswalkSourceError(f"yahoo_league_teams(league={slug!r}) listed {out.height} teams")
    return out


# Words two different schools' names commonly share; they never vouch for a nickname match.
_GENERIC_WORDS = {
    "st", "tech", "college", "of", "at", "the", "and", "a", "m",
    "north", "south", "east", "west", "central", "northern", "southern", "eastern", "western",
}  # fmt: skip


def _same_school(espn_location: Optional[str], yahoo: Mapping[str, Any]) -> bool:
    """Whether ESPN's school name and Yahoo's (short name, full name minus nickname) agree.

    They agree on a shared non-generic word (one a prefix of the other, or the
    first four letters of one inside the other: ``"Massachusetts"`` /
    ``"UMass"``, ``"UAlbany"`` / ``"Albany"``), or when ESPN's initials open a
    Yahoo word (``"Long Island University"`` / ``"LIU"``).
    """
    espn = [w for w in normalize_college_team(espn_location).split() if w not in _GENERIC_WORDS]
    full = normalize_team(yahoo.get("yahoo_team_name"))
    nick = normalize_team(yahoo.get("yahoo_nickname"))
    if nick and full.endswith(" " + nick):
        full = full[: -len(nick) - 1]
    words = {
        w
        for name in (yahoo.get("yahoo_display_name"), full)
        for w in normalize_college_team(name).split()
        if w not in _GENERIC_WORDS
    }
    for a in espn:
        for b in words:
            if a == b or (
                min(len(a), len(b)) >= 3 and (a.startswith(b) or b.startswith(a) or a[:4] in b or b[:4] in a)
            ):
                return True
    initials = "".join(w[0] for w in normalize_team(espn_location).split())
    return len(initials) >= 2 and any(w.startswith(initials) for w in words)


def match_yahoo_teams(team_xwalk: pl.DataFrame, yahoo: pl.DataFrame) -> pl.DataFrame:
    """Join Yahoo teams to ESPN by name in tiers, one-to-one.

    Tiers, each among the rows both sides still have unmatched and only where
    the key is unique on both sides: normalized full name vs ESPN display name;
    Yahoo's short display name vs ESPN location (college-normalized, so
    ``"St."`` == ``"State"``); Yahoo nickname vs ESPN mascot, kept only when the
    school names also agree (:func:`_same_school`), since a nickname alone
    pairs any two "Hawks".

    Args:
        team_xwalk: Team crosswalk with ``espn_team_id``, ``espn_display_name``,
            ``espn_location``, ``espn_mascot``.
        yahoo: Output of :func:`yahoo_teams`.

    Returns:
        ``espn_team_id`` (``Int32``) plus the :data:`YAHOO_TEAMS_SCHEMA` columns
        and ``yahoo_match_method`` (``exact_name`` / ``location`` / ``nickname``),
        one row per matched ESPN team.
    """
    espn = list(
        zip(
            team_xwalk["espn_team_id"].to_list(),
            team_xwalk["espn_display_name"].to_list(),
            team_xwalk["espn_location"].to_list(),
            team_xwalk["espn_mascot"].to_list(),
        )
    )
    ys = yahoo.to_dicts()
    tiers = [
        ("exact_name", lambda e: normalize_team(e[1]), lambda y: normalize_team(y["yahoo_team_name"])),
        ("location", lambda e: normalize_college_team(e[2]), lambda y: normalize_college_team(y["yahoo_display_name"])),
        ("nickname", lambda e: normalize_team(e[3]), lambda y: normalize_team(y["yahoo_nickname"])),
    ]
    guard = {"nickname": lambda e, y: _same_school(e[2], y)}
    matched: Dict[int, Dict[str, Any]] = {}
    used: Set[str] = set()
    for method, espn_key, yahoo_key in tiers:
        rest_e = [e for e in espn if e[0] is not None and int(e[0]) not in matched]
        rest_y = [y for y in ys if y["yahoo_team_id"] not in used]
        e_keys = [espn_key(e) for e in rest_e]
        y_keys = [yahoo_key(y) for y in rest_y]
        y_by = {k: y for k, y in zip(y_keys, rest_y) if k and y_keys.count(k) == 1}
        ok = guard.get(method, lambda e, y: True)
        for e, k in zip(rest_e, e_keys):
            if k and e_keys.count(k) == 1 and k in y_by and ok(e, y_by[k]):
                matched[int(e[0])] = {**y_by[k], "yahoo_match_method": method}
                used.add(y_by[k]["yahoo_team_id"])
    schema = {"espn_team_id": pl.Int32, **YAHOO_TEAMS_SCHEMA, "yahoo_match_method": pl.Utf8}
    return pl.DataFrame([{"espn_team_id": k, **v} for k, v in matched.items()], schema=schema)


def with_yahoo_teams(league: str, team_xwalk: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
    """Fetch Yahoo's teams and fill the team crosswalk's ``yahoo_*`` columns, fail-soft.

    Yahoo teams no ESPN row took are logged by id and name.

    Args:
        league: ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        team_xwalk: The league's team crosswalk.
        **kwargs: Forwarded to the Yahoo getter.

    Returns:
        ``team_xwalk`` with ``yahoo_team_id`` (and ``yahoo_team_abbreviation`` /
        ``yahoo_team_name`` where the table has them) filled; unchanged, with a
        warning logged, when Yahoo could not be fetched.
    """
    try:
        yahoo = yahoo_teams(league, **kwargs)
        matched = match_yahoo_teams(team_xwalk, yahoo)
    except Exception as exc:  # the leg is optional: any failure leaves the Yahoo columns null
        logger.warning("%s: Yahoo teams unavailable, yahoo_team_* left null: %s: %s", league, type(exc).__name__, exc)
        return team_xwalk
    unmatched = yahoo.join(matched.select("yahoo_team_id"), on="yahoo_team_id", how="anti")
    if unmatched.height:
        logger.warning(
            "%s: %d Yahoo teams matched no ESPN team: %s",
            league,
            unmatched.height,
            ", ".join(f"{i} {n}" for i, n in unmatched.select("yahoo_team_id", "yahoo_team_name").iter_rows()),
        )
    cols = [c for c in ("yahoo_team_id", "yahoo_team_abbreviation", "yahoo_team_name") if c in team_xwalk.columns]
    key = team_xwalk.schema["espn_team_id"]
    return (
        team_xwalk.drop(cols)
        .join(
            matched.select(pl.col("espn_team_id").cast(key), *cols),
            on="espn_team_id",
            how="left",
            maintain_order="left",
        )
        .select(team_xwalk.columns)
    )


def yahoo_game_ids(league: str, season: int, dates: Sequence[date], **kwargs: Any) -> Set[str]:
    """Every game id Yahoo lists between the first and last of ``dates``.

    Args:
        league: ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        season: Season, ending year (WNBA: the calendar year).
        dates: The schedule's ET dates.
        **kwargs: Forwarded to the Yahoo getter.

    Returns:
        The set of dotted Yahoo game ids (``"ncaab.g.202603020376"``).

    Raises:
        CrosswalkSourceError: A week's fetch failed, or came back at the
            1000-row cap (a truncated list would null real games).
    """
    from sportsdataverse.providers.yahoo.yahoo_shangrila import yahoo_league_game_ids_by_date

    slug, _ = _YAHOO[league]
    # Yahoo keys a college season by its starting year; without season= a past season returns nothing.
    yahoo_season = season if league == "wnba" else season - 1
    out: Set[str] = set()
    if not dates:
        return out
    start, last = min(dates), max(dates)
    while start <= last:
        end = min(start + timedelta(days=_YAHOO_CHUNK_DAYS - 1), last)
        label = f"yahoo_league_game_ids_by_date(leagues={slug!r}, {start}..{end})"
        try:
            raw = yahoo_league_game_ids_by_date(
                leagues=slug,
                start_range=start.isoformat(),
                end_range=end.isoformat(),
                season=str(yahoo_season),
                return_parsed=False,
                **kwargs,
            )
            games = [g for lg in (raw.get("data") or {}).get("leagues") or [] for g in lg.get("games") or []]
            ids = {g["gameId"] for g in games if g.get("gameId")}
        except Exception as exc:
            raise CrosswalkSourceError(f"{label} failed: {type(exc).__name__}: {exc}") from exc
        if len(games) >= _YAHOO_ROW_CAP:
            raise CrosswalkSourceError(f"{label} hit the {_YAHOO_ROW_CAP}-row cap")
        out |= ids
        start = end + timedelta(days=1)
    return out


def assign_yahoo_game_ids(
    league: str, schedule: pl.DataFrame, team_xwalk: pl.DataFrame, known: Optional[Set[str]]
) -> pl.DataFrame:
    """Fill ``yahoo_game_id`` from the ET date and the Yahoo id of the home team.

    Args:
        league: ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        schedule: Schedule crosswalk (``game_date``, ``home_espn_team_id``,
            ``away_espn_team_id``, ``yahoo_game_id``).
        team_xwalk: Team crosswalk with ``yahoo_team_id`` filled.
        known: Yahoo's real ids for the dates (:func:`yahoo_game_ids`). With it,
            the home-side id is kept if Yahoo lists it, else the away-side id
            (a neutral-site flip), else null. ``None`` keeps the unverified
            home-side id.

    Returns:
        ``schedule`` with ``yahoo_game_id`` filled, same rows and column order.
    """
    slug, width = _YAHOO[league]
    pairs = team_xwalk.select(
        pl.col("espn_team_id").cast(pl.Int32), pl.col("yahoo_team_id").str.split(".").list.last().str.zfill(width)
    ).drop_nulls()
    old, new = pairs["espn_team_id"].to_list(), pairs["yahoo_team_id"].to_list()

    def candidate(side: str) -> pl.Expr:
        team = (
            pl.col(f"{side}_espn_team_id").cast(pl.Int32).replace_strict(old, new, default=None, return_dtype=pl.Utf8)
        )
        return pl.concat_str([pl.lit(f"{slug}.g."), as_date(pl.col("game_date")).dt.strftime("%Y%m%d"), team])

    home, away = candidate("home"), candidate("away")
    if known is None:
        game_id = home
    else:
        listed = pl.Series(sorted(known), dtype=pl.Utf8).implode()
        game_id = pl.when(home.is_in(listed)).then(home).when(away.is_in(listed)).then(away)
    return schedule.with_columns(game_id.cast(pl.Utf8).alias("yahoo_game_id"))


def with_yahoo_games(
    league: str, season: int, schedule: pl.DataFrame, team_xwalk: pl.DataFrame, **kwargs: Any
) -> pl.DataFrame:
    """Fill ``yahoo_game_id``, checked against Yahoo's listed ids when they can be fetched.

    Args:
        league: ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        season: Season, ending year.
        schedule: Schedule crosswalk.
        team_xwalk: Team crosswalk with ``yahoo_team_id`` filled (see
            :func:`with_yahoo_teams`).
        **kwargs: Forwarded to the Yahoo getter.

    Returns:
        ``schedule`` with ``yahoo_game_id`` filled. When the id list cannot be
        fetched a warning is logged and the unverified home-side ids are kept;
        any other failure logs a warning and returns ``schedule`` unchanged.
    """
    try:
        dates = schedule.select(as_date(pl.col("game_date"))).to_series().drop_nulls().unique().sort().to_list()
        known: Optional[Set[str]]
        try:
            known = yahoo_game_ids(league, season, dates, **kwargs)
        except CrosswalkSourceError as exc:
            logger.warning(
                "%s %s: Yahoo game ids unavailable, keeping unverified home-side ids: %s", league, season, exc
            )
            known = None
        return assign_yahoo_game_ids(league, schedule, team_xwalk, known)
    except Exception as exc:  # the leg is optional: any failure leaves yahoo_game_id as it was
        logger.warning(
            "%s %s: Yahoo game ids failed, yahoo_game_id left as is: %s: %s", league, season, type(exc).__name__, exc
        )
        return schedule
