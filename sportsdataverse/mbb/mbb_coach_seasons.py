"""College coach seasons and careers (BB-D8): ``mbb.coach_seasons`` / ``wbb.coach_seasons``.

One row per head coach and team-season, with the W-L, points, ratings and four factors of the
team's games; careers sum them. The crediting rule is in
:mod:`sportsdataverse._common.coach_seasons`.

**Who coached** comes from stats.ncaa.org (not KenPom): each team-season's roster page carries a
"Coach" card naming the head coach, his NCAA person id, and his career seasons and record through
that season. :func:`parse_ncaa_team_coach` reads the card and
:func:`ncaa_team_coaches` reads every page of a season from a raw-repo tree
(``ncaa-mbb-hoops-raw`` / ``ncaa-wbb-hoops-raw``, ``{league}/rosters/html/{season}/{id}.html``,
mapped to ESPN team ids by ``{league}/teams/json/{season}.json``).

The card names one coach per team-season, so he is credited with every game of it: the source
does not say which games of a changed season were whose. The card's record is the check: it is
his career record, so now minus a season ago (:func:`coach_season_records`) is the season's.
On 2014-2026 it matches ESPN's final scores exactly for 4,104 of the 4,330 men's team-seasons it
can be checked on (3,992 of 4,356 women's), and is within one loss for 4,288 (4,262). The NCAA
record drops vacated wins, never losses (Arizona 2016-17 is 0-5 on the card, 32-5 on the floor),
counts forfeits, and some cards carry a season figure instead of a career one, so a coach's first
season after one of those goes unchecked. Of the other 42 men's (94 women's) team-seasons, more
than a loss apart, the 37 men's (29 women's) where the card's season is short of the team's are
mid-season changes whose card figures are the other coach's (Syracuse 2015-16: Jim Boeheim's card
reads 4-5, Mike Hopkins's games during Boeheim's suspension). Those stay uncredited
(:func:`ncaa_coach_stints`).

Every completed ESPN game counts (postseason included), as in BB-D22's college records.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from bs4 import BeautifulSoup

from sportsdataverse._common.coach_seasons import (
    STINTS_SCHEMA,
    TEAM_GAMES_SCHEMA,
    coach_careers,
    coach_of_record,
    coach_seasons,
)
from sportsdataverse.mbb.mbb_team_season_rates import (
    _check_league,
    _league_loaders,
    ncaa_possession_rate_rows,
    ncaa_team_box_count_rows,
)

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_coach_careers", "mbb_coach_seasons", "ncaa_coach_stints", "ncaa_team_coaches", "parse_ncaa_team_coach"]

logger = logging.getLogger(__name__)

_TREE = {"mens": "mbb", "womens": "wbb"}

#: One row per NCAA team-season page with a coach card.
NCAA_COACHES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "ncaa_team_id": pl.Int64(),
    "team_id": pl.Int64(),
    "team": pl.Utf8(),
    "coach_id": pl.Int64(),
    "coach": pl.Utf8(),
    "career_seasons": pl.Int64(),
    "career_wins": pl.Int64(),
    "career_losses": pl.Int64(),
}

_PERSON = re.compile(r"/people/(\d+)")
_RECORD = re.compile(r"(\d+)\s*-\s*(\d+)")


def parse_ncaa_team_coach(html: str) -> dict | None:
    """The head coach on a stats.ncaa.org team page's "Coach" card.

    Args:
        html: A ``teams/{id}`` or ``teams/{id}/roster`` page.

    Returns:
        ``coach_id`` (the NCAA person id), ``coach``, ``career_seasons`` (his seasons as a head
        coach through this one, every school and division) and ``career_wins`` /
        ``career_losses`` (his record through this season); ``None`` when the page has no coach
        card. A field the card lacks is None.
    """
    doc = BeautifulSoup(html, "html.parser")
    header = doc.select_one("div.card-header:-soup-contains(Coach)")
    body = header.find_next_sibling("div") if header is not None else None
    link = body.select_one("a[href*='/people/']") if body is not None else None
    if body is None or link is None:
        return None
    fields = {dt.get_text(strip=True).rstrip(":"): dt.find_next_sibling("dd") for dt in body.select("dt")}
    seasons = fields.get("Seasons")
    record = _RECORD.search(fields["Record"].get_text(" ", strip=True)) if fields.get("Record") is not None else None
    years = re.search(r"\d+", seasons.get_text(" ", strip=True)) if seasons is not None else None
    return {
        "coach_id": int(_PERSON.search(link["href"]).group(1)),  # type: ignore[union-attr]
        "coach": link.get_text(strip=True),
        "career_seasons": int(years.group(0)) if years else None,
        "career_wins": int(record.group(1)) if record else None,
        "career_losses": int(record.group(2)) if record else None,
    }


def ncaa_team_coaches(root: str | Path, seasons: int | list[int], *, league: str = "mens") -> pl.DataFrame:
    """Every team-season's head coach from a raw-repo tree of stats.ncaa.org roster pages.

    Args:
        root: The ``ncaa-mbb-hoops-raw`` / ``ncaa-wbb-hoops-raw`` checkout.
        seasons: A season or list of seasons, named by the year it ends.
        league: ``"mens"`` or ``"womens"``.

    Returns:
        One row per page with a coach card, in :data:`NCAA_COACHES_SCHEMA`; ``team_id`` is the
        ESPN team id from the season's ``teams/json`` crosswalk (null when it has none). A season
        with no crosswalk file is skipped with a warning.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.
    """
    _check_league(league)
    base = Path(root) / _TREE[league]
    rows = []
    for season in [seasons] if isinstance(seasons, int) else list(seasons):
        xw_path = base / "teams" / "json" / f"{season}.json"
        if not xw_path.exists():
            logger.warning("ncaa_team_coaches: no team crosswalk for %s (%s) -- skipped", season, xw_path)
            continue
        teams = {str(t["ncaa_team_id"]): t for t in json.loads(xw_path.read_text(encoding="utf-8"))}
        for page in sorted((base / "rosters" / "html" / str(season)).glob("*.html")):
            card = parse_ncaa_team_coach(page.read_text(encoding="utf-8", errors="replace"))
            if card is None:
                continue
            team = teams.get(page.stem) or {}
            espn = team.get("espn_team_id")
            rows.append(
                {
                    "season": season,
                    "ncaa_team_id": int(page.stem),
                    "team_id": int(espn) if espn else None,
                    "team": team.get("team"),
                    **card,
                }
            )
    return (
        pl.DataFrame(rows, schema=NCAA_COACHES_SCHEMA, orient="row")
        if rows
        else pl.DataFrame(schema=NCAA_COACHES_SCHEMA)
    )


def coach_season_records(coaches: pl.DataFrame) -> pl.DataFrame:
    """The card coach's own record for each team-season: his career record now minus a season ago.

    Args:
        coaches: :func:`ncaa_team_coaches` rows (the previous season too, for the difference).

    Returns:
        ``coaches`` plus ``season_wins`` / ``season_losses``: the career record itself in his first
        season as a head coach, the difference when the previous season's cards (any school) name
        him with one career season fewer, else null (a season off, or a page without a card).
    """
    rows = coaches.select(list(NCAA_COACHES_SCHEMA)).cast(NCAA_COACHES_SCHEMA)
    prev = (
        rows.select(
            (pl.col("season") + 1).alias("season"),
            "coach_id",
            pl.col("career_seasons").alias("_seasons"),
            pl.col("career_wins").alias("_wins"),
            pl.col("career_losses").alias("_losses"),
        )
        .sort("season", "coach_id", "_seasons")
        .unique(["season", "coach_id"], keep="last", maintain_order=True)
    )
    first = pl.col("career_seasons") == 1
    chained = pl.col("_seasons") == pl.col("career_seasons") - 1
    return (
        rows.join(prev, on=["season", "coach_id"], how="left", validate="m:1")
        .with_columns(
            pl.when(first)
            .then(pl.col("career_wins"))
            .when(chained)
            .then(pl.col("career_wins") - pl.col("_wins"))
            .alias("season_wins"),
            pl.when(first)
            .then(pl.col("career_losses"))
            .when(chained)
            .then(pl.col("career_losses") - pl.col("_losses"))
            .alias("season_losses"),
        )
        .drop("_seasons", "_wins", "_losses")
    )


def ncaa_coach_stints(coaches: pl.DataFrame, records: pl.DataFrame) -> pl.DataFrame:
    """The card coach of each team-season as a whole-season stint, unless the card says the season changed hands.

    A card whose season record (:func:`coach_season_records`) is short of the team's by more than
    a game and more than a loss marks a mid-season change, and its figures are not the card
    coach's own games: on Sports-Reference they are the other coach's (Ole Miss 2017-18's card
    names Andy Kennedy with 1-4, Tony Madlock's five games; Kennedy went 11-16). Such a team-season
    gets no stint, so it stays uncredited (and is counted in a log line). Otherwise -- the record
    matches, is a loss off, drops vacated wins (never losses), or cannot be computed -- the card
    coach has the whole season.

    Args:
        coaches: :func:`ncaa_team_coaches` rows, with the season before each wanted one for the
            record difference.
        records: The teams' season records: ``season``, ``team_id``, ``games``, ``losses`` (as
            :func:`sportsdataverse._common.team_luck.season_records` gives them).

    Returns:
        Stints in :data:`~sportsdataverse._common.coach_seasons.STINTS_SCHEMA` with no counts and
        ``position="any"``, one per team-season with a card, an ESPN team id (cards with none are
        counted in a log line) and no mid-season change.

    Raises:
        ValueError: If two cards name a coach for the same (season, team_id).
    """
    cards = coach_season_records(coaches)
    if cards["team_id"].null_count():
        logger.warning("ncaa_coach_stints: %d cards with no ESPN team id (dropped)", cards["team_id"].null_count())
        cards = cards.filter(pl.col("team_id").is_not_null())
    if cards.select(pl.struct("season", "team_id").is_duplicated().any()).item():
        raise ValueError("coaches: two cards for one (season, team_id)")
    team = records.select("season", "team_id", pl.col("games").alias("_games"), pl.col("losses").alias("_losses"))
    w, lost = pl.col("season_wins"), pl.col("season_losses")
    changed = (w >= 0) & (lost >= 0) & (w + lost < pl.col("_games") - 1) & (lost < pl.col("_losses") - 1)
    joined = cards.join(team, on=["season", "team_id"], how="left", validate="1:1")
    n_changed = joined.filter(changed).height
    if n_changed:
        logger.info("ncaa_coach_stints: %d team-seasons changed coach mid-season (left uncredited)", n_changed)
    return (
        joined.filter(~changed.fill_null(False))
        .select(
            "season",
            "team_id",
            "coach_id",
            "coach",
            pl.lit(None, dtype=pl.Int64).alias("games"),
            pl.lit(None, dtype=pl.Int64).alias("wins"),
            pl.lit("any").alias("position"),
        )
        .cast(STINTS_SCHEMA)
        .sort("season", "team_id")
    )


def espn_team_games(schedule: pl.DataFrame) -> pl.DataFrame:
    """Two rows per completed ESPN game (postseason included), with its date and school name.

    Args:
        schedule: ``load_mbb_schedule`` / ``load_wbb_schedule`` rows.

    Returns:
        Completed games in :data:`~sportsdataverse._common.coach_seasons.TEAM_GAMES_SCHEMA`.
    """
    if schedule.height == 0:
        return pl.DataFrame(schema=TEAM_GAMES_SCHEMA)
    done = schedule.filter(
        (pl.col("status_type_completed") == True)  # noqa: E712
        & pl.col("home_score").is_not_null()
        & pl.col("away_score").is_not_null()
    ).unique("game_id", keep="first", maintain_order=True)
    day = pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d")
    return pl.concat(
        [
            done.select(
                "season",
                pl.col("game_id").cast(pl.Utf8),
                day.alias("game_date"),
                pl.col(f"{us}_id").alias("team_id"),
                pl.col(f"{us}_location").alias("team"),
                pl.col(f"{them}_id").alias("opp_team_id"),
                pl.col(f"{us}_score").alias("pts"),
                pl.col(f"{them}_score").alias("opp_pts"),
            ).cast(TEAM_GAMES_SCHEMA)
            for us, them in (("home", "away"), ("away", "home"))
        ]
    )


def _espn_keyed(frame: pl.DataFrame, ids: pl.DataFrame) -> pl.DataFrame:
    """Possession / box rows re-keyed from the NCAA contest id to ESPN's game id."""
    if frame.schema["game_id"] != ids.schema["game_id"]:
        raise TypeError(f"contest id dtype differs: {frame.schema['game_id']} vs {ids.schema['game_id']}")
    return (
        frame.join(ids, on="game_id", how="inner", validate="m:1").drop("game_id").rename({"espn_game_id": "game_id"})
    )


def mbb_coach_seasons(
    seasons: int | list[int],
    coaches: pl.DataFrame,
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """College head-coach seasons: W-L, ratings and four factors of the team-seasons each coached.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
            NCAA possessions start in 2010; earlier seasons get null ratings.
        coaches: :func:`ncaa_team_coaches` rows for those seasons and the season before each
            (the card record check, :func:`ncaa_coach_stints`).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_coach_seasons.wbb_coach_seasons`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, coach_id)``: ``coach_id`` is the NCAA person id,
        ``team_id`` the ESPN team id, ``team`` the school. The columns are those of
        :func:`sportsdataverse.nba.nba_coach_seasons.nba_coach_seasons`; ``pace`` is per 40
        minutes. Ranks are within the season among coaches with half the most games (which
        leaves out the non-D1 opponents, who have no coach). Empty input returns that schema
        with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start (with a raw-repo checkout)::

            from sportsdataverse.mbb.mbb_coach_seasons import mbb_coach_seasons, ncaa_team_coaches
            coaches = ncaa_team_coaches("ncaa-mbb-hoops-raw", 2025)
            mbb_coach_seasons(2025, coaches).sort("net_rtg_rank").select("coach", "team", "wins", "net_rtg").head()
    """
    _check_league(league)
    wanted = [seasons] if isinstance(seasons, int) else list(seasons)
    load_possessions, load_team_box, load_schedule = _league_loaders(league)
    schedule = load_schedule(wanted)
    games = espn_team_games(schedule)
    won = pl.col("pts") > pl.col("opp_pts")
    records = games.group_by("season", "team_id").agg(pl.len().alias("games"), (~won).sum().alias("losses"))
    stints = ncaa_coach_stints(coaches.filter(pl.col("season").is_in([*wanted, *(x - 1 for x in wanted)])), records)
    stints = stints.filter(pl.col("season").is_in(wanted))
    credit = coach_of_record(games, stints)
    possessions = load_possessions(wanted)
    rows = ncaa_possession_rate_rows(possessions, schedule, league=league)
    ids = (
        possessions.filter(pl.col("espn_game_id").is_not_null())
        .select(
            pl.col("contest_id").cast(pl.Utf8).alias("game_id"), pl.col("espn_game_id").cast(pl.Int64).cast(pl.Utf8)
        )
        .unique("game_id")
        if possessions.height
        else pl.DataFrame(schema={"game_id": pl.Utf8, "espn_game_id": pl.Utf8})
    )
    counts = ncaa_team_box_count_rows(load_team_box(wanted)) if rows.height else None
    out = coach_seasons(
        games,
        credit,
        _espn_keyed(rows, ids) if rows.height else None,
        minutes_per_game=40,
        counts=_espn_keyed(counts, ids) if counts is not None and counts.height else None,
    )
    return out.to_pandas() if return_as_pandas else out


def mbb_coach_careers(
    seasons: int | list[int],
    coaches: pl.DataFrame,
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """College head-coach careers over ``seasons``: :func:`mbb_coach_seasons` summed per coach.

    Args:
        seasons: The seasons the careers cover (a career counts only these).
        coaches: :func:`ncaa_team_coaches` rows for those seasons.
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``coach_id`` (NCAA person id), most games first, as
        :func:`sportsdataverse.nba.nba_coach_seasons.nba_coach_careers`; ``teams`` lists the
        schools.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_coach_seasons import mbb_coach_careers, ncaa_team_coaches
            seasons = list(range(2014, 2027))
            mbb_coach_careers(seasons, ncaa_team_coaches("ncaa-mbb-hoops-raw", seasons)).head()
    """
    out = coach_careers(mbb_coach_seasons(seasons, coaches, league=league), minutes_per_game=40)
    return out.to_pandas() if return_as_pandas else out
