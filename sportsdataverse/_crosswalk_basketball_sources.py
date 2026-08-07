"""Thin network adapters for the basketball crosswalks.

Every function here fetches one provider and projects it onto the small,
uniform mini-schema the pure assemblers in the ``*_crosswalk`` modules expect.
Keeping the adapters separate from the join logic is what lets the parity
tests exercise the assemblers completely offline.

The R sources call each provider through its own package's accessor
(``wehoop::espn_wbb_teams()`` etc.), whose column names differ from sdv-py's.
These adapters own that rename so the assemblers can stay a literal port.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence

import polars as pl

from sportsdataverse._common_crosswalk_basketball import str_id, to_eastern
from sportsdataverse.errors import SportsDataverseError

__all__ = [
    "CrosswalkSourceError",
    "espn_team_directory",
    "espn_scoreboard_games",
    "espn_rosters",
    "fox_rosters",
    "stats_rosters",
    "bart_super_sked",
    "stats_schedule_games",
]


class CrosswalkSourceError(SportsDataverseError):
    """Raised when a whole-source crosswalk fetch produced nothing usable.

    A crosswalk join against a source that returned nothing degrades every row
    to ``unmatched`` — a well-formed, entirely wrong output. These adapters
    therefore distinguish *provably empty* (the provider answered, and the
    answer had no games) from *unproduced* (the fetch failed, or the payload
    could not be rendered): the first returns a typed empty frame, the second
    raises this.

    Only whole-source fetches raise. The per-item loops (one scoreboard call
    per date, one roster call per team) keep tolerating an individual failure,
    matching the R producers.

    Example:
        Fail loudly instead of publishing an all-null crosswalk::

            from sportsdataverse._crosswalk_basketball_sources import (
                CrosswalkSourceError,
                stats_schedule_games,
            )

            try:
                games = stats_schedule_games("wnba", 2026)
            except CrosswalkSourceError as exc:
                print(f"refusing to build a crosswalk: {exc}")
    """


# The 55 positional fields of barttorvik's {year}_super_sked.json, per Torvik's
# documentation (https://adamcwisports.blogspot.com/p/data.html). Transcribed
# verbatim from hoopR::torvik_game_schedule / wehoop::bart_wbb_game_schedule.
SUPER_SKED_FIELDS = [
    "muid", "date", "conmatch", "matchup", "prediction", "ttq", "conf", "venue",
    "team1", "t1oe", "t1de", "t1py", "t1wp", "t1propt",
    "team2", "t2oe", "t2de", "t2py", "t2wp", "t2propt",
    "tpro", "t1qual", "t2qual", "gp", "result", "tempo", "possessions", "t1pts",
    "t2pts", "winner", "loser", "t1adjt", "t2adjt", "t1adjo", "t1adjd", "t2adjo",
    "t2adjd", "gamevalue", "mismatch", "blowout", "t1elite", "t2elite", "ord_date",
    "t1ppp", "t2ppp", "gameppp", "t1rk", "t2rk", "t1gs", "t2gs", "gamestats",
    "overtimes", "t1fun", "t2fun", "results",
]  # fmt: skip

_TORVIK_HOST = {
    "mbb": "https://barttorvik.com",
    "wbb": "https://barttorvik.com/ncaaw",
}


def _pick(df: pl.DataFrame, *candidates: str) -> pl.Expr:
    """First present column among ``candidates``, else a null ``Utf8`` literal."""
    for name in candidates:
        if name in df.columns:
            return pl.col(name)
    return pl.lit(None, dtype=pl.Utf8)


def _espn_accessors(league: str) -> Dict[str, Callable[..., Any]]:
    if league == "wbb":
        from sportsdataverse.wbb.wbb_espn_ext import espn_wbb_scoreboard
        from sportsdataverse.wbb.wbb_team_roster import espn_wbb_team_roster
        from sportsdataverse.wbb.wbb_teams import espn_wbb_teams

        return {"teams": espn_wbb_teams, "scoreboard": espn_wbb_scoreboard, "roster": espn_wbb_team_roster}
    if league == "mbb":
        from sportsdataverse.mbb.mbb_espn_ext import espn_mbb_scoreboard
        from sportsdataverse.mbb.mbb_teams import espn_mbb_teams

        from sportsdataverse.wbb.wbb_team_roster import _espn_basketball_team_roster  # noqa: F401

        def _roster(team_id: int, season: Optional[int] = None, **kw: Any) -> pl.DataFrame:
            from sportsdataverse.mbb.mbb_espn_ext import espn_mbb_team_roster

            return espn_mbb_team_roster(team_id=team_id, **kw)

        return {"teams": espn_mbb_teams, "scoreboard": espn_mbb_scoreboard, "roster": _roster}
    if league == "nba":
        from sportsdataverse.nba.nba_espn_ext import espn_nba_scoreboard
        from sportsdataverse.nba.nba_teams import espn_nba_teams

        def _roster(team_id: int, season: Optional[int] = None, **kw: Any) -> pl.DataFrame:
            from sportsdataverse.nba.nba_espn_ext import espn_nba_team_roster

            return espn_nba_team_roster(team_id=team_id, **kw)

        return {"teams": espn_nba_teams, "scoreboard": espn_nba_scoreboard, "roster": _roster}
    if league == "wnba":
        from sportsdataverse.wnba.wnba_espn_ext import espn_wnba_scoreboard
        from sportsdataverse.wnba.wnba_team_roster import espn_wnba_team_roster
        from sportsdataverse.wnba.wnba_teams import espn_wnba_teams

        return {"teams": espn_wnba_teams, "scoreboard": espn_wnba_scoreboard, "roster": espn_wnba_team_roster}
    raise ValueError(f"unknown league {league!r}")


def espn_team_directory(league: str, season: Optional[int] = None, **kwargs: Any) -> pl.DataFrame:
    """ESPN team directory projected onto the R accessor's column names.

    sdv-py's ``espn_{lg}_teams()`` returns ``team_*``-prefixed columns from
    ``pd.json_normalize``; the R crosswalks expect ``team_id`` /
    ``abbreviation`` / ``display_name`` / ``short_name`` / ``team`` /
    ``mascot``. This renames one to the other.

    Args:
        league: ``"mbb"``, ``"wbb"``, ``"nba"`` or ``"wnba"``.
        season: Unused by the ESPN teams endpoint; accepted for symmetry.
        **kwargs: Forwarded to the accessor.

    Returns:
        ``pl.DataFrame``, one row per team. ``conference_name`` is present only
        when the upstream frame carries it (sdv-py's does not).

    Example:
        Quick start::

            from sportsdataverse._crosswalk_basketball_sources import espn_team_directory
            print(espn_team_directory("wnba").columns)
    """
    raw = _espn_accessors(league)["teams"](**kwargs)
    if not isinstance(raw, pl.DataFrame):
        raw = pl.from_pandas(raw)
    out = raw.select(
        _pick(raw, "team_id", "id").alias("team_id"),
        _pick(raw, "team_abbreviation", "abbreviation").alias("abbreviation"),
        _pick(raw, "team_display_name", "display_name").alias("display_name"),
        _pick(raw, "team_short_display_name", "short_display_name", "short_name").alias("short_name"),
        _pick(raw, "team_location", "location").alias("team"),
        _pick(raw, "team_name", "mascot", "name").alias("mascot"),
    )
    for candidate in ("team_conference_name", "conference_name", "group_name"):
        if candidate in raw.columns:
            out = out.with_columns(raw[candidate].cast(pl.Utf8).alias("conference_name"))
            break
    return out


def espn_scoreboard_games(league: str, dates: Sequence[date], **kwargs: Any) -> pl.DataFrame:
    """One ESPN scoreboard call per ET date, unioned to a game-level frame.

    Args:
        league: ``"mbb"``, ``"wbb"``, ``"nba"`` or ``"wnba"``.
        dates: ET calendar dates to fetch.
        **kwargs: Forwarded to the scoreboard wrapper.

    Returns:
        ``pl.DataFrame`` with ``espn_game_id``, ``game_date`` (ET),
        ``home_espn_team_id``, ``away_espn_team_id``; a typed empty frame when
        every call fails.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse._crosswalk_basketball_sources import espn_scoreboard_games
            df = espn_scoreboard_games("wnba", [dt.date(2025, 6, 1)])
    """
    scoreboard = _espn_accessors(league)["scoreboard"]
    frames: List[pl.DataFrame] = []
    for day in dates:
        try:
            sb = scoreboard(dates=int(day.strftime("%Y%m%d")), return_parsed=True, **kwargs)
        except Exception:
            continue
        if sb is None or not isinstance(sb, pl.DataFrame) or sb.height == 0:
            continue
        stamps = sb.select(_pick(sb, "date", "game_date_time", "game_date")).to_series().to_list()
        frames.append(
            sb.select(
                str_id(sb, "game_id").alias("espn_game_id"),
                _pick(sb, "home_team_id", "home_id").cast(pl.Int32, strict=False).alias("home_espn_team_id"),
                _pick(sb, "away_team_id", "away_id").cast(pl.Int32, strict=False).alias("away_espn_team_id"),
            ).with_columns(pl.Series("game_date", [to_eastern(v) for v in stamps], dtype=pl.Date))
        )
    if not frames:
        return pl.DataFrame(
            schema={
                "espn_game_id": pl.Utf8,
                "home_espn_team_id": pl.Int32,
                "away_espn_team_id": pl.Int32,
                "game_date": pl.Date,
            }
        )
    return pl.concat(frames, how="diagonal_relaxed")


def espn_rosters(
    league: str,
    espn_team_id: Any,
    abbreviation: Optional[str],
    season: Optional[int] = None,
    **kwargs: Any,
) -> pl.DataFrame:
    """One ESPN team roster projected onto the assembler's ESPN mini-schema.

    Args:
        league: ``"mbb"``, ``"wbb"``, ``"nba"`` or ``"wnba"``.
        espn_team_id: ESPN team id.
        abbreviation: Team abbreviation stamped onto every row.
        season: Season year (recorded, not always sent upstream).
        **kwargs: Forwarded to the roster accessor.

    Returns:
        ``pl.DataFrame`` with ``espn_team_id``, ``team_abbreviation``,
        ``espn_athlete_id``, ``espn_full_name``, ``espn_jersey``,
        ``espn_position``, ``espn_birth_date``; empty on any fetch failure (a
        single team's roster is per-item tolerant, as in the R producers).

    Raises:
        CrosswalkSourceError: The roster had rows but no resolvable athlete id,
            which would silently break every join keyed on it.

    Example:
        Quick start::

            from sportsdataverse._crosswalk_basketball_sources import espn_rosters
            df = espn_rosters("wnba", 3, "DAL", 2026)
    """
    empty = pl.DataFrame(
        schema={
            "espn_team_id": pl.Int32,
            "team_abbreviation": pl.Utf8,
            "espn_athlete_id": pl.Utf8,
            "espn_full_name": pl.Utf8,
            "espn_jersey": pl.Utf8,
            "espn_position": pl.Utf8,
            "espn_birth_date": pl.Utf8,
        }
    )
    try:
        raw = _espn_accessors(league)["roster"](team_id=espn_team_id, season=season, **kwargs)
    except Exception:
        return empty
    if raw is None or not isinstance(raw, pl.DataFrame) or raw.height == 0:
        return empty
    # The wbb/wnba roster module names the athlete key `athlete_id`; the generic
    # ESPN wrappers behind mbb/nba name it `id`. Looking for only one of the two
    # yielded an all-null join key -- a full roster whose every row then failed to
    # rejoin its own match, which is why every NBA player row came back with a
    # null match_method instead of exact_name/unmatched.
    athlete_key = next((c for c in ("athlete_id", "id") if c in raw.columns), "athlete_id")
    out = raw.select(
        pl.lit(int(espn_team_id), dtype=pl.Int32).alias("espn_team_id"),
        pl.lit(abbreviation, dtype=pl.Utf8).alias("team_abbreviation"),
        str_id(raw, athlete_key).alias("espn_athlete_id"),
        _pick(raw, "full_name", "display_name").cast(pl.Utf8).alias("espn_full_name"),
        str_id(raw, "jersey").alias("espn_jersey"),
        _pick(raw, "position_abbreviation", "position_abbrev").cast(pl.Utf8).alias("espn_position"),
        _pick(raw, "birth_date", "date_of_birth").cast(pl.Utf8).alias("espn_birth_date"),
    )
    if out["espn_athlete_id"].null_count() == out.height:
        # A populated roster with no athlete id is a renamed upstream column, not
        # a roster of anonymous players. Fail here rather than downstream, where
        # it only shows up as a silently unjoinable crosswalk.
        raise CrosswalkSourceError(
            f"espn_{league}_team_roster(team_id={espn_team_id}) returned {out.height} rows "
            f"with no resolvable athlete id (columns: {sorted(raw.columns)[:15]}...)"
        )
    return out


def fox_rosters(league: str, espn_team_id: Any, fox_team_id: Optional[str], **kwargs: Any) -> pl.DataFrame:
    """One Fox team roster projected onto the assembler's Fox mini-schema.

    Args:
        league: ``"mbb"``, ``"wbb"``, ``"nba"`` or ``"wnba"``.
        espn_team_id: ESPN team id, stamped on as the match block.
        fox_team_id: Fox Bifrost team id; ``None`` returns an empty frame.
        **kwargs: Forwarded to the Fox roster wrapper.

    Returns:
        ``pl.DataFrame`` with ``espn_team_id``, ``fox_athlete_id``,
        ``fox_player``, ``fox_jersey``, ``fox_position_group``.

    Example:
        Quick start::

            from sportsdataverse._crosswalk_basketball_sources import fox_rosters
            df = fox_rosters("wbb", 41, "11")
    """
    empty = pl.DataFrame(
        schema={
            "espn_team_id": pl.Int32,
            "fox_athlete_id": pl.Utf8,
            "fox_player": pl.Utf8,
            "fox_jersey": pl.Utf8,
            "fox_position_group": pl.Utf8,
        }
    )
    if fox_team_id is None:
        return empty
    getters = {
        "wbb": "sportsdataverse.wbb.wbb_fox_ext:fox_wbb_team_roster",
        "mbb": "sportsdataverse.mbb.mbb_fox_ext:fox_mbb_team_roster",
        "nba": "sportsdataverse.nba.nba_fox_ext:fox_nba_team_roster",
        "wnba": "sportsdataverse.wnba.wnba_fox_ext:fox_wnba_team_roster",
    }
    module_path, func_name = getters[league].split(":")
    module = __import__(module_path, fromlist=[func_name])
    try:
        raw = getattr(module, func_name)(fox_team_id, **kwargs)
    except Exception:
        return empty
    if raw is None or not isinstance(raw, pl.DataFrame) or raw.height == 0:
        return empty
    return raw.select(
        pl.lit(int(espn_team_id), dtype=pl.Int32).alias("espn_team_id"),
        str_id(raw, "athlete_id").alias("fox_athlete_id"),
        _pick(raw, "player", "name").cast(pl.Utf8).alias("fox_player"),
        # parse_roster names jersey from the table header, which varies.
        str_id(raw, "jersey" if "jersey" in raw.columns else ("#" if "#" in raw.columns else "x")).alias("fox_jersey"),
        _pick(raw, "position_group").cast(pl.Utf8).alias("fox_position_group"),
    )


def stats_rosters(
    league: str, espn_team_id: Any, stats_team_id: Optional[str], season: str, **kwargs: Any
) -> pl.DataFrame:
    """One NBA/WNBA Stats API ``commonteamroster`` projected for the assembler.

    Args:
        league: ``"nba"`` or ``"wnba"``.
        espn_team_id: ESPN team id, stamped on as the match block.
        stats_team_id: Stats API team id; ``None`` returns an empty frame.
        season: Stats API season string (e.g. ``"2025-26"``).
        **kwargs: Forwarded to the wrapper.

    Returns:
        ``pl.DataFrame`` with ``espn_team_id`` and the ``{league}_player_*``
        columns the assembler consumes.

    Example:
        Quick start::

            from sportsdataverse._crosswalk_basketball_sources import stats_rosters
            df = stats_rosters("wnba", 5, "1611661319", "2025")
    """
    prefix = league
    empty = pl.DataFrame(
        schema={
            "espn_team_id": pl.Int32,
            f"{prefix}_player_id": pl.Utf8,
            f"{prefix}_player_name": pl.Utf8,
            f"{prefix}_jersey_num": pl.Utf8,
            f"{prefix}_position": pl.Utf8,
            f"{prefix}_birth_date": pl.Utf8,
        }
    )
    if stats_team_id is None:
        return empty
    if league == "nba":
        from sportsdataverse.nba.nba_stats import nba_stats_commonteamroster as fetch
    else:
        from sportsdataverse.wnba.wnba_stats import wnba_stats_commonteamroster as fetch
    try:
        raw = fetch(team_id=stats_team_id, season=season, **kwargs)
    except Exception:
        return empty
    if isinstance(raw, dict):
        raw = raw.get("CommonTeamRoster")
    if raw is None or not isinstance(raw, pl.DataFrame) or raw.height == 0:
        return empty
    return raw.select(
        pl.lit(int(espn_team_id), dtype=pl.Int32).alias("espn_team_id"),
        str_id(raw, "player_id").alias(f"{prefix}_player_id"),
        _pick(raw, "player", "player_name").cast(pl.Utf8).alias(f"{prefix}_player_name"),
        str_id(raw, "num").alias(f"{prefix}_jersey_num"),
        _pick(raw, "position").cast(pl.Utf8).alias(f"{prefix}_position"),
        _pick(raw, "birth_date").cast(pl.Utf8).alias(f"{prefix}_birth_date"),
    )


def parse_super_sked(payload: Any, year: int) -> pl.DataFrame:
    """Parse Torvik's positional ``{year}_super_sked.json`` into a tidy frame.

    The file is a JSON array of arrays: 55 positional fields per game, with no
    header. Nested sub-arrays are collapsed to ``";"``-joined strings, matching
    the R readers. ``date`` (``%m/%d/%y``) is parsed into ``game_date``.

    Args:
        payload: Raw JSON text (or an already-decoded list) from Torvik.
        year: Season year, stamped onto the ``year`` column.

    Returns:
        ``pl.DataFrame`` with :data:`SUPER_SKED_FIELDS` plus ``game_date`` and
        ``year``; zero rows on empty/malformed input.

    Example:
        Quick start::

            from sportsdataverse._crosswalk_basketball_sources import parse_super_sked
            df = parse_super_sked("[]", 2026)
            print(df.height)
    """
    schema = {name: pl.Utf8 for name in SUPER_SKED_FIELDS}
    schema["game_date"] = pl.Date
    schema["year"] = pl.Int32
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return pl.DataFrame(schema=schema)
    if not isinstance(payload, list) or not payload:
        return pl.DataFrame(schema=schema)

    def flat(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, list):
            return ";".join("" if v is None else str(v) for v in value) or None
        return str(value)

    rows: List[Dict[str, Optional[str]]] = []
    for game in payload:
        if not isinstance(game, list) or len(game) != len(SUPER_SKED_FIELDS):
            continue
        rows.append({name: flat(v) for name, v in zip(SUPER_SKED_FIELDS, game)})
    if not rows:
        return pl.DataFrame(schema=schema)

    def parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%m/%d/%y").date()
        except ValueError:
            return None

    df = pl.DataFrame(rows, schema={name: pl.Utf8 for name in SUPER_SKED_FIELDS})
    return df.with_columns(
        pl.Series("game_date", [parse_date(v) for v in df["date"].to_list()], dtype=pl.Date),
        pl.lit(year, dtype=pl.Int32).alias("year"),
    )


def bart_super_sked(league: str, year: int, **kwargs: Any) -> pl.DataFrame:
    """Fetch + parse Torvik's season "super schedule" for a league.

    Args:
        league: ``"mbb"`` (barttorvik.com) or ``"wbb"`` (``/ncaaw`` mirror).
        year: Season year.
        **kwargs: Forwarded to the Torvik HTTP getter.

    Returns:
        ``pl.DataFrame`` from :func:`parse_super_sked`, with unparseable dates
        dropped (as the R readers do). Empty only when Torvik served an empty
        schedule.

    Raises:
        CrosswalkSourceError: The fetch raised, or the payload could not be
            parsed into any game row. Same contract as
            :func:`stats_schedule_games`: an unavailable source must not pass
            for an empty one.

    Example:
        Quick start::

            from sportsdataverse._crosswalk_basketball_sources import bart_super_sked
            games = bart_super_sked("wbb", 2026)
            print(games.select("muid", "team1", "team2", "game_date").head())
    """
    from sportsdataverse.mbb.torvik_runtime import _get

    url = f"{_TORVIK_HOST[league]}/{year}_super_sked.json"
    try:
        payload = _get(url, **kwargs)
    except Exception as exc:
        raise CrosswalkSourceError(f"{url} failed: {type(exc).__name__}: {exc}") from exc
    parsed = parse_super_sked(payload, year)
    if parsed.height == 0 and payload not in (None, "", "[]", []):
        raise CrosswalkSourceError(
            f"{url} returned a payload that parsed to zero games ({type(payload).__name__}, {len(payload)} items/chars)"
        )
    return parsed.filter(pl.col("game_date").is_not_null())


def stats_schedule_games(league: str, season: int, *, teams: bool = False, **kwargs: Any) -> pl.DataFrame:
    """NBA/WNBA Stats league schedule, projected for the schedule assembler.

    Args:
        league: ``"nba"`` or ``"wnba"``.
        season: Season year (hoopR/wehoop convention).
        teams: When ``True`` return the distinct team directory derived from
            the schedule's home/away fields (the wehoop WNBA recipe) instead
            of the game rows.
        **kwargs: Forwarded to the wrapper.

    Returns:
        ``pl.DataFrame`` of games (or teams when ``teams=True``). A typed empty
        frame **only** when the endpoint answered with a valid but game-less
        schedule — an unavailable source raises instead of degrading every
        downstream join to ``unmatched``.

    Raises:
        CrosswalkSourceError: The fetch raised, or returned a payload the
            ``scheduleleaguev2`` parser could not render into a frame.

    Note:
        ``stats.{nba,wnba}.com`` hangs on datacenter IPs. Prefer passing a
        pre-fetched frame into the crosswalk builders when running in CI.

    Example:
        Quick start::

            from sportsdataverse._crosswalk_basketball_sources import stats_schedule_games
            games = stats_schedule_games("wnba", 2026)
    """
    p = league
    game_schema = {
        "game_date": pl.Date,
        "season_type": pl.Utf8,
        f"{p}_game_id": pl.Utf8,
        f"{p}_game_code": pl.Utf8,
        f"{p}_home_team_id": pl.Utf8,
        f"{p}_away_team_id": pl.Utf8,
    }
    team_schema = {
        f"{p}_team_id": pl.Utf8,
        f"{p}_team_tricode": pl.Utf8,
        f"{p}_team_name": pl.Utf8,
        f"{p}_team_city": pl.Utf8,
        f"{p}_team_slug": pl.Utf8,
    }
    from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

    if league == "nba":
        from sportsdataverse.nba.nba_stats import nba_stats_scheduleleaguev2 as fetch

        stats_season = f"{season - 1}-{str(season)[-2:]}"
    else:
        from sportsdataverse.wnba.wnba_stats import wnba_stats_scheduleleaguev2 as fetch

        stats_season = str(season)
    endpoint = f"{league}_stats_scheduleleaguev2(season={stats_season!r})"
    try:
        # return_parsed=False so the envelope itself is inspectable: only a
        # payload that really carries `leagueSchedule.gameDates` can be called
        # empty. Anything else is unproduced, and must not pass for empty.
        payload = fetch(season=stats_season, return_parsed=False, **kwargs)
    except Exception as exc:
        raise CrosswalkSourceError(f"{endpoint} failed: {type(exc).__name__}: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("leagueSchedule"), dict):
        raise CrosswalkSourceError(
            f"{endpoint} returned no leagueSchedule envelope "
            f"(got {type(payload).__name__} with keys {sorted(payload)[:10] if isinstance(payload, dict) else '-'})"
        )
    game_dates = payload["leagueSchedule"].get("gameDates")
    if not isinstance(game_dates, list):
        raise CrosswalkSourceError(
            f"{endpoint}: leagueSchedule.gameDates is {type(game_dates).__name__}, expected a list"
        )
    raw = parse_nba_stats_result_sets(payload)
    if not isinstance(raw, pl.DataFrame):  # pragma: no cover - single result set by construction
        raise CrosswalkSourceError(f"{endpoint}: parser returned {type(raw).__name__}, expected a DataFrame")
    if raw.height == 0:
        # gameDates was present and genuinely carried no games (e.g. a season
        # published before its schedule drops) — provably empty, not a failure.
        return pl.DataFrame(schema=team_schema if teams else game_schema)

    if teams:
        sides = []
        for side in ("home", "away"):
            sides.append(
                raw.select(
                    str_id(raw, f"{side}_team_id").alias(f"{p}_team_id"),
                    _pick(raw, f"{side}_team_tricode").cast(pl.Utf8).alias(f"{p}_team_tricode"),
                    _pick(raw, f"{side}_team_name").cast(pl.Utf8).alias(f"{p}_team_name"),
                    _pick(raw, f"{side}_team_city").cast(pl.Utf8).alias(f"{p}_team_city"),
                    _pick(raw, f"{side}_team_slug").cast(pl.Utf8).alias(f"{p}_team_slug"),
                )
            )
        return pl.concat(sides, how="diagonal_relaxed").unique(keep="first", maintain_order=True)

    stamps = raw.select(_pick(raw, "game_date_time_utc", "game_date_est", "game_date")).to_series().to_list()
    return raw.select(
        _pick(raw, "season_type_description", "week_name", "season_type").cast(pl.Utf8).alias("season_type"),
        str_id(raw, "game_id").alias(f"{p}_game_id"),
        str_id(raw, "game_code").alias(f"{p}_game_code"),
        str_id(raw, "home_team_id").alias(f"{p}_home_team_id"),
        str_id(raw, "away_team_id").alias(f"{p}_away_team_id"),
    ).with_columns(pl.Series("game_date", [to_eastern(v) for v in stamps], dtype=pl.Date))
