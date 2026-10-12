# sportsdataverse/nba/nba_game_page.py
"""Game-page derivations for one NBA game (shared by the GOP processor and the sdv-db stage).

Every function takes the hoopR-schema frames (``load_nba_pbp`` / ``load_nba_team_boxscore`` /
``load_nba_player_boxscore`` column names) so one implementation serves both callers: the Game on
Paper Flask processor builds those frames live from ESPN through
:func:`~sportsdataverse.nba.nba_pbp.espn_nba_pbp` + the ``helper_nba_*`` converters (see
:func:`game_page_frames`), and the sdv-db nightly stage reads the same shapes from the warehouse.
"""

from __future__ import annotations

import math
from typing import Optional

import polars as pl

from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob, win_prob_from_margin
from sportsdataverse.nba.nba_prediction_constants import get_constants

__all__ = [
    "FT_WEIGHT",
    "REGULATION_PERIODS",
    "COORD_SENTINEL",
    "GAME_PAGE_LEAGUES",
    "game_regulation_periods",
    "game_minutes",
    "team_game_rates",
    "player_game_rates",
    "home_prior",
    "ot_adjusted_seconds",
    "score_win_prob",
    "shot_coordinates",
    "game_page_frames",
]

#: Oliver's free-throw possession weight, used by every rate below.
FT_WEIGHT: float = 0.44
REGULATION_PERIODS: int = 4
#: ESPN marks a missing play coordinate with ``-214748340``; anything below this is "no location".
COORD_SENTINEL: float = -1000.0

#: The leagues the game page scores. ``league_id`` ``"00"`` / ``"10"`` remain aliases of ``"nba"`` / ``"wnba"``.
GAME_PAGE_LEAGUES: tuple[str, ...] = ("nba", "wnba", "mbb", "wbb")
_LEAGUE_IDS: dict[str, str] = {"00": "nba", "10": "wnba"}
_NBA_FAMILY_IDS: dict[str, str] = {"nba": "00", "wnba": "10"}
#: ``mbb_prediction_constants`` keys of the college leagues.
_NCAA: dict[str, str] = {"mbb": "mens", "wbb": "womens"}
_REGULATION_MINUTES: dict[str, int] = {"nba": 48, "wnba": 40, "mbb": 40, "wbb": 40}
#: First season played in quarters; earlier seasons were two 20-minute halves (MBB always plays halves).
_QUARTERS_FROM: dict[str, int] = {"wnba": 2006, "wbb": 2016}


def _resolve_league(league: Optional[str], league_id: Optional[str]) -> str:
    """The game-page league from ``league`` and/or the legacy ``league_id``; both absent means NBA."""
    if league_id is not None and league_id not in _LEAGUE_IDS:
        raise ValueError(
            f"league_id {league_id!r} is not supported by the game page; use one of {tuple(_LEAGUE_IDS)}"
            f" or league= one of {GAME_PAGE_LEAGUES}"
        )
    if league is not None and league not in GAME_PAGE_LEAGUES:
        raise ValueError(f"league {league!r} is not supported by the game page; use one of {GAME_PAGE_LEAGUES}")
    if league is not None and league_id is not None and _LEAGUE_IDS[league_id] != league:
        raise ValueError(f"league={league!r} contradicts league_id={league_id!r}")
    return league or _LEAGUE_IDS[league_id or "00"]


def game_regulation_periods(
    league: str,
    *,
    season: Optional[int] = None,
    format_regulation_periods: Optional[float] = None,
) -> int:
    """Regulation periods of one game: 2 (halves) or 4 (quarters).

    The game's own value wins: ESPN's summary ``format.regulation.periods`` on the live path, the
    schedule's ``format_regulation_periods`` in the warehouse. Without it (null, NaN, or anything
    other than 2 or 4) the season decides: the NBA always plays quarters, men's college basketball
    always halves, the WNBA halves before 2006 and women's college basketball halves before the
    2015-16 season (``season`` 2016, ESPN's end year). With neither, WNBA and WBB default to quarters.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        season: ESPN ``season.year`` of the game (the end year for college).
        format_regulation_periods: The game's own regulation period count, if known.

    Returns:
        ``2`` or ``4``.

    Raises:
        ValueError: If ``league`` is not one of the four.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_regulation_periods
            game_regulation_periods("wbb", season=2015)  # 2: halves before 2015-16
            game_regulation_periods("wnba", season=2024, format_regulation_periods=4.0)  # 4

        A warehouse schedule row::

            game_regulation_periods("mbb", season=2024, format_regulation_periods=2.0)  # 2

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, None)
    try:
        own = int(format_regulation_periods) if format_regulation_periods is not None else None
    except (TypeError, ValueError):  # NaN from a null schedule cell
        own = None
    if own in (2, 4):
        return int(own)
    if lg == "mbb":
        return 2
    if lg == "nba" or season is None:
        return 4
    return 2 if int(season) < _QUARTERS_FROM[lg] else 4


#: League ids the game page can score (the in-game win-probability artifacts ship for these two).
_SUPPORTED_LEAGUES = ("00", "10")

_TEAM_RATES_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32,
    "team_id": pl.Int32,
    "efg_pct": pl.Float64,
    "tov_pct": pl.Float64,
    "orb_pct": pl.Float64,
    "ft_rate": pl.Float64,
    "possessions": pl.Float64,
    "off_rtg": pl.Float64,
    "def_rtg": pl.Float64,
    "pace": pl.Float64,
    "total_turnovers": pl.Int32,
    "game_minutes": pl.Int64,
}
_PLAYER_RATES_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32,
    "athlete_id": pl.Int32,
    "team_id": pl.Int32,
    "ts_pct": pl.Float64,
    "usg_pct": pl.Float64,
}

_SCORE_COLS = (
    "home_team_spread",
    "game_spread_available",
    "period_number",
    "start_game_seconds_remaining",
    "home_score",
    "away_score",
    "team_id",
    "home_team_id",
)

#: The period clock: ``start_quarter_seconds_remaining`` (NBA / WNBA / WBB frames, pre-2023 MBB releases) or
#: ``start_period_seconds_remaining`` (``helper_mbb_play_by_play`` and 2023+ MBB releases).
_CLOCK_COLUMNS = ("start_quarter_seconds_remaining", "start_period_seconds_remaining")


def _period_clock(pbp: pl.DataFrame) -> pl.Expr:
    """Seconds left in the current period, from whichever clock column(s) the frame carries."""
    present = [c for c in _CLOCK_COLUMNS if c in pbp.columns]
    if not present:
        raise pl.exceptions.ColumnNotFoundError(
            "needs the period clock: start_quarter_seconds_remaining or start_period_seconds_remaining"
        )
    return pl.coalesce([pl.col(c).cast(pl.Float64) for c in present])


def _in_game_win_prob(lg: str, tagged: pl.DataFrame, prior: float) -> pl.Series:
    """The league's in-game model: NBA / WNBA booster through nba_in_game_win_prob, college through mbb_in_game_win_prob."""
    if lg in _NCAA:
        from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob

        scored = mbb_in_game_win_prob(tagged, prior, league=_NCAA[lg])
    else:
        scored = nba_in_game_win_prob(tagged, prior, league_id=_NBA_FAMILY_IDS[lg])
    return scored["home_win_prob"].cast(pl.Float64)


def _check_league(league_id: str) -> None:
    if league_id not in _SUPPORTED_LEAGUES:
        raise ValueError(f"league_id {league_id!r} is not supported by the game page; use one of {_SUPPORTED_LEAGUES}")


def _margin_to_prob(lg: str, margin: float) -> float:
    """The league's pregame model: home win probability from an expected home margin."""
    if lg in _NCAA:
        from sportsdataverse.mbb.mbb_game_predict import win_prob_from_margin as ncaa_win_prob_from_margin

        return ncaa_win_prob_from_margin(margin, league=_NCAA[lg])
    return win_prob_from_margin(margin, league_id=_NBA_FAMILY_IDS[lg])


def _hfa(lg: str) -> float:
    """The league's fitted home-court advantage in points."""
    if lg in _NCAA:
        from sportsdataverse.mbb.mbb_prediction_constants import get_constants as ncaa_constants

        return ncaa_constants(_NCAA[lg]).hfa
    return get_constants(_NBA_FAMILY_IDS[lg]).hfa


def home_prior(
    home_team_spread: Optional[float],
    game_spread_available: Optional[bool],
    *,
    league_id: Optional[str] = None,
    league: Optional[str] = None,
    neutral_site: Optional[bool] = False,
) -> tuple[float, str]:
    """Pregame home win probability for the in-game model, and where it came from.

    ``home_team_spread`` is the signed expected home margin the basketball pbp frames carry
    (positive when the home side is favoured). Every league's pickcenter helper substitutes ``+2.5``
    when no line exists, so the value is only trusted when ``game_spread_available`` is true. A line
    already prices the venue. Without one the prior is home-court advantage alone, and at a neutral
    site (common in college tournaments) there is none: the prior is 0.5. NBA and WNBA use
    :func:`~sportsdataverse.nba.nba_game_predict.win_prob_from_margin` and their fitted ``hfa``;
    MBB and WBB use :func:`~sportsdataverse.mbb.mbb_game_predict.win_prob_from_margin` with
    ``league="mens"`` / ``"womens"``.

    Args:
        home_team_spread: Signed expected home margin, or ``None``.
        game_spread_available: Whether a real line backed that spread.
        league_id: Legacy alias: ``"00"`` NBA / ``"10"`` WNBA.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        neutral_site: ESPN ``neutralSite`` / the schedule's ``neutral_site``; ``None`` or NaN counts as false.

    Returns:
        ``(probability, source)`` with ``source`` ``"spread"``, ``"neutral"`` or ``"hfa"``.

    Raises:
        ValueError: If the league is unknown or ``league`` contradicts ``league_id``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import home_prior
            home_prior(3.0, True)      # (0.58, 'spread')
            home_prior(2.5, False)     # (0.56, 'hfa')  the default line is ignored

        A lineless college tournament game::

            home_prior(None, None, league="mbb", neutral_site=True)  # (0.5, 'neutral')

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, league_id)
    if game_spread_available and home_team_spread is not None and not math.isnan(float(home_team_spread)):
        return _margin_to_prob(lg, float(home_team_spread)), "spread"
    if neutral_site and neutral_site == neutral_site:  # NaN (a null pandas cell) is not a neutral site
        return _margin_to_prob(lg, 0.0), "neutral"
    return _margin_to_prob(lg, _hfa(lg)), "hfa"


def ot_adjusted_seconds(pbp: pl.DataFrame, *, regulation_periods: int = REGULATION_PERIODS) -> pl.DataFrame:
    """Tag overtime rows and give them the overtime period clock as their seconds remaining.

    The in-game models clip ``start_game_seconds_remaining`` at zero, so every overtime play
    (negative in the NBA frame) would score as "game over at the current margin". Each overtime is
    scored as its own five-minute endgame instead: on rows with ``period_number > regulation_periods``
    ``start_game_seconds_remaining`` becomes the period clock, ``start_quarter_seconds_remaining`` or
    (men's college frames) ``start_period_seconds_remaining``, whichever is present and not null.
    In a halves game (``regulation_periods=2``) period 3 is the first overtime.

    Args:
        pbp: One game in the basketball pbp schema.
        regulation_periods: The game's regulation periods (4 quarters, 2 halves); see
            :func:`game_regulation_periods`.

    Returns:
        The frame with ``wp_regime`` (``"reg"`` / ``"ot"``) and the adjusted seconds column.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``period_number`` or ``start_game_seconds_remaining``
            is missing, or neither period clock column is present.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_page import ot_adjusted_seconds
            from sportsdataverse.nba.nba_loaders import load_nba_pbp
            pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
            tagged = ot_adjusted_seconds(pbp)
            tagged.filter(pl.col("wp_regime") == "ot").select("start_game_seconds_remaining").head()

        A men's college game (two halves)::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
            one = load_mbb_pbp([2026]).filter(pl.col("game_id") == 401830342)
            ot_adjusted_seconds(one, regulation_periods=2)

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    is_ot = pl.col("period_number") > regulation_periods
    clock = _period_clock(pbp)
    return pbp.with_columns(
        pl.when(is_ot).then(pl.lit("ot")).otherwise(pl.lit("reg")).alias("wp_regime"),
        pl.when(is_ot)
        .then(clock)
        .otherwise(pl.col("start_game_seconds_remaining").cast(pl.Float64))
        .alias("start_game_seconds_remaining"),
    )


def score_win_prob(
    pbp: pl.DataFrame,
    *,
    league_id: Optional[str] = None,
    league: Optional[str] = None,
    regulation_periods: Optional[int] = None,
    neutral_site: Optional[bool] = False,
) -> pl.DataFrame:
    """Per-play home win probability, its play-to-play delta, and the prior, for one game.

    NBA and WNBA games score with the NBA / WNBA in-game booster
    (:func:`~sportsdataverse.nba.nba_game_predict.nba_in_game_win_prob`), men's and women's college
    games with the men's / women's college booster
    (:func:`~sportsdataverse.mbb.mbb_game_predict.mbb_in_game_win_prob`).

    Args:
        pbp: One game in the basketball pbp schema (needs ``home_team_spread``,
            ``game_spread_available``, ``period_number``, ``start_game_seconds_remaining``,
            the period clock (``start_quarter_seconds_remaining`` or ``start_period_seconds_remaining``),
            ``home_score``, ``away_score``, ``team_id``, ``home_team_id``), in play order.
        league_id: Legacy alias: ``"00"`` NBA / ``"10"`` WNBA.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods (2 halves, 4 quarters); default from the
            frame's ``season`` via :func:`game_regulation_periods`.
        neutral_site: Whether the game is at a neutral site: no home-court advantage in a lineless
            prior; see :func:`home_prior`.

    Returns:
        The input plus ``pregame_home_prob``, ``prior_source`` (``"spread"``, ``"neutral"`` or
        ``"hfa"``), ``wp_regime``, ``home_win_prob`` and ``wp_delta`` (the first play's delta is
        measured from the prior). ``home_win_prob`` is settled to 1.0 / 0.0 on "End Game" /
        "End Period" rows (``type_text``) from the last regulation period on (period 2 in a halves
        game, 4 in a quarters game) where the score is not tied, before ``wp_delta`` is taken; tied
        rows go to overtime and keep the model value. A frame without ``type_text`` is not settled.
        An empty input returns an empty frame with those columns. A null ``home_score`` /
        ``away_score`` means "no change" and is carried forward within the game for the model (a
        null before the first score is 0); the returned ``home_score`` / ``away_score`` are left as given.

    Raises:
        polars.exceptions.ColumnNotFoundError: If a required column is missing (``home_team_spread``,
            ``game_spread_available``, ``period_number``, ``start_game_seconds_remaining``,
            ``home_score``, ``away_score``, ``team_id``, ``home_team_id``, or neither period clock column).
        ValueError: If the league is unknown, ``league`` contradicts ``league_id``, or ``pbp`` holds
            more than one ``game_id``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_page import score_win_prob
            from sportsdataverse.nba.nba_loaders import load_nba_pbp
            one = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
            wp = score_win_prob(one)

        Biggest swings::

            wp.sort(pl.col("wp_delta").abs(), descending=True).select("text", "wp_delta").head(10)

        A men's college tournament game::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
            one = load_mbb_pbp([2026]).filter(pl.col("game_id") == 401830342)
            score_win_prob(one, league="mbb", neutral_site=True)

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, league_id)
    for c in _SCORE_COLS:
        if c not in pbp.columns:
            raise pl.exceptions.ColumnNotFoundError(f"score_win_prob needs column {c!r}")
    _period_clock(pbp)  # raises before the empty-frame return, like the column loop
    if "game_id" in pbp.columns and pbp.height and pbp["game_id"].n_unique() > 1:
        raise ValueError(f"score_win_prob scores one game; got {pbp['game_id'].n_unique()} game_id values")
    if pbp.height == 0:
        return pbp.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("pregame_home_prob"),
            pl.lit(None, dtype=pl.String).alias("prior_source"),
            pl.lit(None, dtype=pl.String).alias("wp_regime"),
            pl.lit(None, dtype=pl.Float64).alias("home_win_prob"),
            pl.lit(None, dtype=pl.Float64).alias("wp_delta"),
        )
    season = pbp["season"][0] if "season" in pbp.columns else None
    reg = regulation_periods if regulation_periods is not None else game_regulation_periods(lg, season=season)
    # a null score on a play means the score did not change; a null before the first score is 0-0.
    # Only the model input is filled: the returned score columns stay as given.
    filled = pbp.with_columns(pl.col("home_score", "away_score").forward_fill().fill_null(0))
    first = filled.row(0, named=True)
    prior, source = home_prior(
        first["home_team_spread"], first["game_spread_available"], league=lg, neutral_site=neutral_site
    )
    tagged = ot_adjusted_seconds(filled, regulation_periods=reg)
    wp = _in_game_win_prob(lg, tagged, prior)
    if "type_text" in pbp.columns:
        # a period from the end of regulation on that ends with a lead ends the game; a tie goes to overtime
        ends = (
            filled["type_text"].is_in(["End Game", "End Period"])
            & (filled["period_number"] >= reg)
            & (filled["home_score"] != filled["away_score"])
        ).fill_null(False)
        settled = (filled["home_score"] > filled["away_score"]).cast(pl.Float64)
        wp = pl.select(pl.when(ends).then(settled).otherwise(wp).alias("w")).to_series()
    return pbp.with_columns(
        pl.lit(prior, dtype=pl.Float64).alias("pregame_home_prob"),
        pl.lit(source, dtype=pl.String).alias("prior_source"),
        tagged["wp_regime"],
        wp.alias("home_win_prob"),
        (wp - wp.shift(n=1, fill_value=prior)).alias("wp_delta"),
    )


def shot_coordinates(pbp: pl.DataFrame) -> pl.DataFrame:
    """Shot locations in the court frame the Game on Paper hex chart uses.

    ESPN's play coordinates are feet across the court with the hoop at ``(25, 0)`` and ``y``
    growing toward half court (measured on 401766458: dunks at ``y`` about 2, a 22-foot corner
    three at ``(3, 0)``). The chart wants tenths of a foot with the hoop at the origin, so
    ``x_legacy = (x - 25) * 10`` and ``y_legacy = y * 10``. Free throws carry no real location
    (``load_nba_pbp`` pins them to ``(25, 13.75)``) and ESPN's missing-coordinate sentinel is a
    large negative number; both become ``null``, as does every non-shooting play.

    Args:
        pbp: Plays in the ``load_nba_pbp`` schema (``shooting_play``, ``type_text``,
            ``coordinate_x_raw``, ``coordinate_y_raw``).

    Returns:
        The input plus ``x_legacy`` and ``y_legacy`` (Float64).

    Raises:
        polars.exceptions.ColumnNotFoundError: If a required column is missing (``shooting_play``,
            ``type_text``, ``coordinate_x_raw``, ``coordinate_y_raw``).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_page import shot_coordinates
            from sportsdataverse.nba.nba_loaders import load_nba_pbp
            pbp = load_nba_pbp([2025]).filter(pl.col("game_id") == 401766458)
            shots = shot_coordinates(pbp).filter(pl.col("x_legacy").is_not_null())
            shots.select("text", "x_legacy", "y_legacy").head()

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    located = (
        (pl.col("shooting_play") == True)
        & (pl.col("type_text").fill_null("").str.contains("(?i)free throw") == False)
        & (pl.col("coordinate_x_raw").cast(pl.Float64) > COORD_SENTINEL)
        & (pl.col("coordinate_y_raw").cast(pl.Float64) > COORD_SENTINEL)
    )
    return pbp.with_columns(
        pl.when(located).then((pl.col("coordinate_x_raw").cast(pl.Float64) - 25.0) * 10.0).alias("x_legacy"),
        pl.when(located).then(pl.col("coordinate_y_raw").cast(pl.Float64) * 10.0).alias("y_legacy"),
    )


def game_minutes(
    periods: int,
    *,
    league_id: Optional[str] = None,
    league: Optional[str] = None,
    regulation_periods: Optional[int] = None,
) -> int:
    """Minutes played in a game of ``periods`` periods: the league's regulation length plus five per overtime.

    Regulation is 48 minutes in the NBA and 40 in the WNBA and both college leagues (two 20-minute
    halves or four 10-minute quarters); every overtime is 5 minutes. A period past the game's
    ``regulation_periods`` is an overtime, so period 3 of a halves game is the first overtime.

    Args:
        periods: Highest period number played.
        league_id: Legacy alias: ``"00"`` NBA / ``"10"`` WNBA.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods (2 or 4); default
            :func:`game_regulation_periods` for the league without a season (MBB 2, otherwise 4).

    Returns:
        Game minutes.

    Raises:
        TypeError: If ``periods`` is ``None`` or another non-numeric type.
        ValueError: If ``periods`` is a string that is not an integer, the league is unknown, or
            ``league`` contradicts ``league_id``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_minutes
            game_minutes(5)  # 53

        A double-overtime men's college game (two halves + two OTs)::

            game_minutes(4, league="mbb")  # 50

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, league_id)
    reg = regulation_periods if regulation_periods is not None else game_regulation_periods(lg)
    return _REGULATION_MINUTES[lg] + 5 * max(int(periods) - reg, 0)


def _ratio(num: pl.Expr, den: pl.Expr) -> pl.Expr:
    """``num / den`` as Float64, ``null`` when the denominator is zero or null."""
    return (
        pl.when(den > 0)
        .then(num.cast(pl.Float64) / den.cast(pl.Float64))
        .otherwise(pl.lit(None, dtype=pl.Float64))
        .cast(pl.Float64)
    )


def _team_tov(cols: list[str]) -> pl.Expr:
    """The team turnover count used for possessions, TOV% and usage.

    ESPN doubles the team turnover in 2002-2012 (``team_turnovers == turnovers`` and
    ``total_turnovers == 2 * turnovers``, e.g. 17/17/34) and leaves ``total_turnovers`` null in 2018.
    """
    total = pl.col("total_turnovers")
    if "turnovers" not in cols or "team_turnovers" not in cols:
        return total
    own, team = pl.col("turnovers"), pl.col("team_turnovers")
    doubled = (team == own) & (total == 2 * own) & (own > 0)
    return pl.when(total.is_null()).then(own + team.fill_null(0)).when(doubled).then(own).otherwise(total)


def _check_two_teams(team_box: pl.DataFrame, who: str) -> None:
    if team_box.height != 2:
        raise ValueError(f"{who} needs the two team rows of one game, got {team_box.height} team rows")
    if team_box["game_id"].n_unique() != 1 or team_box["team_id"].n_unique() != 2:
        raise ValueError(f"{who} needs two different teams of the same game_id")


def team_game_rates(team_box: pl.DataFrame, *, periods: int = 4, league_id: str = "00") -> pl.DataFrame:
    """Four factors, possessions, ratings and pace for both teams of one game.

    Dean Oliver's estimates on the ``load_nba_team_boxscore`` columns: possessions =
    FGA - ORB + TOV + 0.44 FTA, averaged over the two teams; eFG% = (FGM + 0.5 3PM) / FGA;
    TOV% = TOV / (FGA + 0.44 FTA + TOV); ORB% = ORB / (ORB + opponent DRB); FT rate = FTA / FGA;
    ORtg / DRtg per 100 possessions; pace per regulation length (48 minutes NBA, 40 WNBA). ``TOV`` is the team's player plus team turnovers:
    ``total_turnovers``, except that when it is null it is ``turnovers + team_turnovers``, and when ESPN
    has duplicated the team turnover (``team_turnovers == turnovers`` and ``total_turnovers == 2 *
    turnovers``, measured on every 2002-2012 game, e.g. 17/17/34) it is ``turnovers``. Without the
    ``turnovers`` / ``team_turnovers`` columns it is ``total_turnovers`` as given. A zero denominator
    gives ``null``.

    Args:
        team_box: Exactly two rows (one per team) of one game in the ``load_nba_team_boxscore``
            schema.
        periods: Highest period played, for the pace denominator.
        league_id: ``"00"`` NBA (48-minute regulation) / ``"10"`` WNBA (40); overtime is 5 minutes in both.

    Returns:
        One row per ``(game_id, team_id)`` with ``efg_pct, tov_pct, orb_pct, ft_rate,
        possessions, off_rtg, def_rtg, pace, total_turnovers, game_minutes``.

    Raises:
        ValueError: If ``team_box`` does not hold exactly two rows, or the rows are not two different
            ``team_id`` values of one ``game_id``, or ``league_id`` is unknown.
        polars.exceptions.ColumnNotFoundError: If a required box-score column is missing.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_page import team_game_rates
            from sportsdataverse.nba.nba_loaders import load_nba_team_boxscore
            box = load_nba_team_boxscore([2025]).filter(pl.col("game_id") == 401766458)
            team_game_rates(box).select("team_id", "efg_pct", "off_rtg", "pace")

        WNBA box (40-minute regulation)::

            team_game_rates(box, league_id="10")

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    _check_two_teams(team_box, "team_game_rates")
    tov = _team_tov(team_box.columns)
    fga, fta = pl.col("field_goals_attempted"), pl.col("free_throws_attempted")
    own = team_box.with_columns(
        (fga - pl.col("offensive_rebounds") + tov + FT_WEIGHT * fta).cast(pl.Float64).alias("_own_poss")
    )
    opp = own.select(
        pl.col("game_id"),
        pl.col("team_id").alias("_opp_id"),
        pl.col("team_score").alias("_opp_score"),
        pl.col("defensive_rebounds").alias("_opp_drb"),
        pl.col("_own_poss").alias("_opp_poss"),
    )
    joined = own.join(opp, on="game_id", how="inner").filter(pl.col("team_id") != pl.col("_opp_id"))
    minutes = game_minutes(periods, league_id=league_id)
    regulation = get_constants(league_id).game_minutes
    poss = (pl.col("_own_poss") + pl.col("_opp_poss")) / 2.0
    return joined.select(
        "game_id",
        "team_id",
        _ratio(pl.col("field_goals_made") + 0.5 * pl.col("three_point_field_goals_made"), fga).alias("efg_pct"),
        _ratio(tov, fga + FT_WEIGHT * fta + tov).alias("tov_pct"),
        _ratio(pl.col("offensive_rebounds"), pl.col("offensive_rebounds") + pl.col("_opp_drb")).alias("orb_pct"),
        _ratio(fta, fga).alias("ft_rate"),
        poss.alias("possessions"),
        _ratio(100.0 * pl.col("team_score"), poss).alias("off_rtg"),
        _ratio(100.0 * pl.col("_opp_score"), poss).alias("def_rtg"),
        (float(regulation) * poss / float(minutes)).alias("pace"),
        tov.alias("total_turnovers"),
        pl.lit(minutes, dtype=pl.Int64).alias("game_minutes"),
    )


def player_game_rates(
    player_box: pl.DataFrame, team_box: pl.DataFrame, *, periods: int = 4, league_id: str = "00"
) -> pl.DataFrame:
    """True-shooting and usage for every boxed player of one game.

    TS% = PTS / (2 (FGA + 0.44 FTA)); USG% = 100 ((FGA + 0.44 FTA + TOV) (team minutes / 5)) /
    (MIN (team FGA + 0.44 team FTA + team TOV)) with team TOV as in :func:`team_game_rates`
    (``total_turnovers``, repaired where ESPN doubled or omitted it) and team
    minutes = 5 x game minutes (regulation 48 NBA / 40 WNBA, 5 per overtime). A player with no shots, no minutes, or ``did_not_play`` gets
    ``null``.

    Args:
        player_box: One game in the ``load_nba_player_boxscore`` schema.
        team_box: The game's two ``load_nba_team_boxscore`` rows.
        periods: Highest period played.
        league_id: ``"00"`` NBA / ``"10"`` WNBA.

    Returns:
        One row per ``(game_id, athlete_id)`` with ``team_id, ts_pct, usg_pct``.

    Raises:
        ValueError: If ``team_box`` does not hold exactly two different teams of one ``game_id`` (a
            duplicated team row would double the player rows), or ``league_id`` is unknown.
        polars.exceptions.ColumnNotFoundError: If a required box-score column is missing.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_page import player_game_rates
            from sportsdataverse.nba.nba_loaders import load_nba_player_boxscore, load_nba_team_boxscore
            team_box = load_nba_team_boxscore([2025]).filter(pl.col("game_id") == 401766458)
            player_box = load_nba_player_boxscore([2025]).filter(pl.col("game_id") == 401766458)
            player_game_rates(player_box, team_box).sort("usg_pct", descending=True).head()

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    _check_two_teams(team_box, "player_game_rates")
    team_minutes = 5.0 * game_minutes(periods, league_id=league_id)
    team = team_box.select(
        "game_id",
        "team_id",
        (pl.col("field_goals_attempted") + FT_WEIGHT * pl.col("free_throws_attempted") + _team_tov(team_box.columns))
        .cast(pl.Float64)
        .alias("_team_tsa"),
    )
    joined = player_box.join(team, on=["game_id", "team_id"], how="left")
    fga, fta = pl.col("field_goals_attempted"), pl.col("free_throws_attempted")
    played = (pl.col("did_not_play").fill_null(False) == False) & (pl.col("minutes").fill_null(0.0) > 0)
    tsa = fga + FT_WEIGHT * fta
    usage_num = 100.0 * (tsa + pl.col("turnovers")) * (team_minutes / 5.0)
    usage_den = pl.col("minutes").cast(pl.Float64) * pl.col("_team_tsa")
    null_f = pl.lit(None, dtype=pl.Float64)
    return joined.select(
        "game_id",
        "athlete_id",
        "team_id",
        pl.when(played).then(_ratio(pl.col("points"), 2.0 * tsa)).otherwise(null_f).cast(pl.Float64).alias("ts_pct"),
        pl.when(played).then(_ratio(usage_num, usage_den)).otherwise(null_f).cast(pl.Float64).alias("usg_pct"),
    )


def game_page_frames(final: dict, *, league_id: str = "00") -> dict[str, object]:
    """Everything the Game on Paper NBA game page needs, from one ``espn_nba_pbp`` payload.

    Runs the hoopR converters (``helper_nba_play_by_play`` / ``helper_nba_team_box`` /
    ``helper_nba_player_box``) and then :func:`score_win_prob`, :func:`shot_coordinates`,
    :func:`team_game_rates` and :func:`player_game_rates`. The sdv-db nightly stage calls those four
    directly on the warehouse's copies of the same frames, so a game renders identically from
    either source.

    Args:
        final: The dict :func:`~sportsdataverse.nba.nba_pbp.espn_nba_pbp` returns.
        league_id: ``"00"`` NBA / ``"10"`` WNBA (48 / 40-minute regulation for the rates).

    Returns:
        ``{"pbp", "team_box", "team_game_rates", "player_box", "player_game_rates", "periods", "prior"}``
        where ``prior`` is ``{"home_win_prob", "source", "home_team_spread",
        "game_spread_available"}``. Before both team box rows exist, ``team_game_rates`` and
        ``player_game_rates`` are empty frames with their documented columns.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.
        KeyError: If ``final`` lacks the ``header`` or ``plays`` keys the converters read.
        polars.exceptions.ColumnNotFoundError: If a converter frame lacks a column the
            derivations need.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_page_frames
            from sportsdataverse.nba.nba_pbp import espn_nba_pbp
            frames = game_page_frames(espn_nba_pbp(401766458))
            frames["pbp"].select("text", "home_win_prob", "wp_delta").head()

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    _check_league(league_id)
    from sportsdataverse.nba.nba_play_by_play import helper_nba_play_by_play
    from sportsdataverse.nba.nba_player_box import helper_nba_player_box
    from sportsdataverse.nba.nba_team_box import helper_nba_team_box

    pbp = helper_nba_play_by_play(final)
    periods = int(pbp["period_number"].max() or REGULATION_PERIODS) if pbp.height else REGULATION_PERIODS
    if pbp.height:  # a game with 10 or fewer plays parses to a zero-column frame; nothing to score yet
        pbp = shot_coordinates(score_win_prob(pbp, league_id=league_id))
    team_box = helper_nba_team_box(final)
    player_box = helper_nba_player_box(final)
    if pbp.height:
        prior_prob, prior_source = pbp["pregame_home_prob"][0], pbp["prior_source"][0]
        spread, available = pbp["home_team_spread"][0], bool(pbp["game_spread_available"][0] or False)
    else:
        (prior_prob, prior_source), spread, available = home_prior(None, None, league_id=league_id), None, False
    two_teams = team_box.height == 2
    return {
        "pbp": pbp,
        "team_box": team_box,
        "team_game_rates": team_game_rates(team_box, periods=periods, league_id=league_id)
        if two_teams
        else pl.DataFrame(schema=_TEAM_RATES_SCHEMA),
        "player_box": player_box,
        "player_game_rates": player_game_rates(player_box, team_box, periods=periods, league_id=league_id)
        if two_teams
        else pl.DataFrame(schema=_PLAYER_RATES_SCHEMA),
        "periods": periods,
        "prior": {
            "home_win_prob": prior_prob,
            "source": prior_source,
            "home_team_spread": spread,
            "game_spread_available": available,
        },
    }
