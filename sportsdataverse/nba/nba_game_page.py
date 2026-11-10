# sportsdataverse/nba/nba_game_page.py
"""Game-page derivations for one NBA, WNBA, men's or women's college basketball game (shared by the GOP processor and the sdv-db stage).

Every function takes the hoopR / wehoop basketball frames (``load_nba_pbp`` / ``load_nba_team_boxscore`` /
``load_nba_player_boxscore`` column names) so one implementation serves both callers: the Game on
Paper Flask processor builds those frames live from ESPN through ``espn_<league>_pbp`` + the
``helper_<league>_*`` converters (see :func:`game_page_frames` and :func:`league_pbp_functions`), and the
sdv-db nightly stage reads the same shapes from the warehouse (see :func:`game_page_tables`).
"""

from __future__ import annotations

import importlib
import math
from typing import Any, Callable, Optional

import polars as pl

from sportsdataverse._common.game_flow import _runs, _states
from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob, win_prob_from_margin
from sportsdataverse.nba.nba_play_context_constants import CORNER_THREE_MAX_Y, RIM_DISTANCE_FT
from sportsdataverse.nba.nba_possession_flags import TRANSITION_SECONDS
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
    "TRANSITION_WINDOW_SECONDS",
    "team_game_splits",
    "player_game_splits",
    "home_prior",
    "ot_adjusted_seconds",
    "score_win_prob",
    "GEI_SCALE",
    "game_excitement",
    "season_game_excitement",
    "shot_coordinates",
    "game_page_tables",
    "game_page_frames",
    "league_pbp_functions",
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


def _resolve_league(league: str | None, league_id: str | None) -> str:
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
    season: int | None = None,
    format_regulation_periods: float | None = None,
) -> int:
    """Regulation periods of one game: 2 (halves) or 4 (quarters).

    The game's own value wins: ESPN's summary ``format.regulation.periods`` on the live path, the
    schedule's ``format_regulation_periods`` in the warehouse. Without it (null, NaN, or anything
    other than 2 or 4) the season decides: the NBA always plays quarters, men's college basketball
    always halves, the WNBA halves before 2006 and women's college basketball halves before the
    2015-16 season (``season`` 2016, ESPN's end year). With neither, WNBA and WBB default to quarters.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``; ``None`` means NBA.
        season: ESPN ``season.year`` of the game (the end year for college); null or NaN counts as unknown.
        format_regulation_periods: The game's own regulation period count, if known.

    Returns:
        ``2`` or ``4``.

    Raises:
        ValueError: If ``league`` is a string other than the four.

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
    except (TypeError, ValueError, OverflowError):  # NaN / inf from a null or broken schedule cell
        own = None
    if own in (2, 4):
        return int(own)
    if lg == "mbb":
        return 2
    if lg == "nba" or season is None or season != season:  # NaN != NaN
        return 4
    return 2 if int(season) < _QUARTERS_FROM[lg] else 4


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


def _game_end(reg: int) -> pl.Expr:
    """The rows that end a game: "End Game" / "End Period" from the last regulation period on, score not tied.

    A tied period end goes to overtime. Needs forward-filled ``home_score`` / ``away_score``.
    """
    return (
        pl.col("type_text").is_in(["End Game", "End Period"])
        & (pl.col("period_number") >= reg)
        & (pl.col("home_score") != pl.col("away_score"))
    ).fill_null(False)


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


def _is_neutral(neutral_site: object) -> bool:
    """A neutral-site flag; ``None``, NaN and pandas' ``pd.NA`` (a null schedule cell) are not neutral."""
    try:
        return bool(bool(neutral_site) and neutral_site == neutral_site)  # NaN != NaN; numpy gives np.bool_
    except TypeError:  # bool(pd.NA) raises
        return False


def home_prior(
    home_team_spread: Optional[float],
    game_spread_available: Optional[bool],
    *,
    league_id: Optional[str] = None,
    league: Optional[str] = None,
    neutral_site: Optional[bool] = False,
    rated_home_prob: Optional[float] = None,
) -> tuple[float, str]:
    """Pregame home win probability for the in-game model, and where it came from.

    ``home_team_spread`` is the signed expected home margin the basketball pbp frames carry
    (positive when the home side is favoured). With no line, every basketball pickcenter helper leaves
    it ``None`` (older releases carry an invented ``+2.5``), so the value is only trusted when
    ``game_spread_available`` is true. A line
    already prices the venue. Without one the prior is ``rated_home_prob`` when given (the NBA / WNBA
    as-of team-ratings prior, :func:`~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`, which prices
    the venue itself), else home-court advantage alone, and at a neutral site (common in college
    tournaments) there is none: the prior is 0.5. NBA and WNBA use
    :func:`~sportsdataverse.nba.nba_game_predict.win_prob_from_margin` and their fitted ``hfa``;
    MBB and WBB use :func:`~sportsdataverse.mbb.mbb_game_predict.win_prob_from_margin` with
    ``league="mens"`` / ``"womens"``.

    Args:
        home_team_spread: Signed expected home margin, or ``None``.
        game_spread_available: Whether a real line backed that spread.
        league_id: Legacy alias: ``"00"`` NBA / ``"10"`` WNBA.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        neutral_site: ESPN ``neutralSite`` / the schedule's ``neutral_site``; ``None``, NaN or ``pd.NA``
            counts as false.
        rated_home_prob: The game's rated pregame home win probability, used when there is no line;
            ``None``, NaN or a value outside [0, 1] falls through to home-court advantage.

    Returns:
        ``(probability, source)`` with ``source`` ``"spread"``, ``"rated"``, ``"neutral"`` or ``"hfa"``.

    Raises:
        ValueError: If the league is unknown or ``league`` contradicts ``league_id``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import home_prior
            home_prior(3.0, True)      # (0.58, 'spread')
            home_prior(2.5, False)     # (0.56, 'hfa')  the default line is ignored
            home_prior(None, False, rated_home_prob=0.71)  # (0.71, 'rated')

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
    if rated_home_prob is not None and 0.0 <= float(rated_home_prob) <= 1.0:  # NaN and inf fall through
        return float(rated_home_prob), "rated"
    if _is_neutral(neutral_site):
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
    rated_home_prob: Optional[float] = None,
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
        rated_home_prob: The game's rated pregame home win probability (NBA / WNBA:
            :func:`~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`), the prior when there is
            no line; see :func:`home_prior`.

    Returns:
        The input plus ``pregame_home_prob``, ``prior_source`` (``"spread"``, ``"rated"``, ``"neutral"`` or
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
    season = _box_season(pbp)  # the first non-null, non-NaN season, or None
    reg = regulation_periods if regulation_periods is not None else game_regulation_periods(lg, season=season)
    # a null score on a play means the score did not change; a null before the first score is 0-0.
    # Only the model input is filled: the returned score columns stay as given.
    filled = pbp.with_columns(pl.col("home_score", "away_score").forward_fill().fill_null(0))
    first = filled.row(0, named=True)
    prior, source = home_prior(
        first["home_team_spread"],
        first["game_spread_available"],
        league=lg,
        neutral_site=neutral_site,
        rated_home_prob=rated_home_prob,
    )
    tagged = ot_adjusted_seconds(filled, regulation_periods=reg)
    wp = _in_game_win_prob(lg, tagged, prior)
    if "type_text" in pbp.columns:
        ends = filled.select(_game_end(reg)).to_series()
        settled = (filled["home_score"] > filled["away_score"]).cast(pl.Float64)
        wp = pl.select(pl.when(ends).then(settled).otherwise(wp).alias("w")).to_series()
    return pbp.with_columns(
        pl.lit(prior, dtype=pl.Float64).alias("pregame_home_prob"),
        pl.lit(source, dtype=pl.String).alias("prior_source"),
        tagged["wp_regime"],
        wp.alias("home_win_prob"),
        (wp - wp.shift(n=1, fill_value=prior)).alias("wp_delta"),
    )


#: The game excitement index scale per league: the mean number of WP points (pbp rows) in a
#: completed game, fitted by ``tools/fit_basketball_gei_scale.py`` on the sportsdataverse-data
#: ``espn_<league>_pbp`` releases, seasons 2024-2026, regular season and postseason. Football's
#: GEI uses 179.01777401608126 (game-on-paper ``calculateGEI``); a basketball game has several
#: hundred WP points, so that constant does not carry over.
GEI_SCALE: dict[str, float] = {
    "nba": 474.76350328117115,  # 3,962 games
    "wnba": 394.89462365591396,  # 930 games
    "mbb": 384.2563216499369,  # 18,231 games
    "wbb": 390.0788600545088,  # 16,878 games
}


def _gei_steps(wp: pl.Series, prior: float, final: float | None) -> pl.Series:
    """Each play's absolute home WP change, the first from the prior; the last play goes to ``final`` when given."""
    if final is not None and wp.len():
        wp = wp.clone().scatter(wp.len() - 1, final)
    return (wp - wp.shift(n=1, fill_value=prior)).abs()


def game_excitement(
    scored: pl.DataFrame,
    *,
    league_id: str | None = None,
    league: str | None = None,
    regulation_periods: int | None = None,
    completed: bool | None = None,
) -> dict[str, Any]:
    """How exciting one game was, with no direction: the game excitement index, lead changes and the biggest swing.

    Football's GEI (Game on Paper's ``calculateGEI`` / ``excitement.py``) on the basketball WP
    series: ``GEI_SCALE[league] / n * sum(|home WP change|)`` over the game's ``n`` plays. Each
    play's change is measured from the previous play's home WP (the pregame prior for the first
    play). The last play of a completed game swings to the result (1 if home won, 0 if it lost, 0.5
    for a tie); while the game is live the swings so far are summed. The scale is the league's mean
    plays per game (:data:`GEI_SCALE`), so the index reads as the total WP movement of a game of
    average length.

    Args:
        scored: One game from :func:`score_win_prob`, in play order (needs ``home_win_prob``,
            ``pregame_home_prob``, ``home_score``, ``away_score``; ``type_text`` and
            ``period_number`` to tell a completed game).
        league_id: Legacy alias: ``"00"`` NBA / ``"10"`` WNBA.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods; default from the frame's ``season`` via
            :func:`game_regulation_periods`.
        completed: Whether the game is final. ``None`` reads it from the plays: final when a row
            ends the game ("End Game" / "End Period" from the last regulation period on, score not
            tied), the rule :func:`score_win_prob` settles WP on. Without ``type_text`` it is live.

    Returns:
        ``{"gei", "lead_changes", "max_swing_pts", "completed"}``: the index (``None`` for no
        plays), how often the leader flipped (a tie is not a lead, so A ahead, tied, then B ahead
        counts once), the largest of the changes the index sums in percentage points (the last
        play's swing to the result included; ``None`` for no plays), and whether the last play was
        taken to the result.

    Raises:
        polars.exceptions.ColumnNotFoundError: If a required column is missing.
        ValueError: If the league is unknown, ``league`` contradicts ``league_id``, or ``scored``
            holds more than one ``game_id``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_page import game_excitement, score_win_prob
            from sportsdataverse.nba.nba_loaders import load_nba_pbp
            one = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
            game_excitement(score_win_prob(one))

        A men's college game::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
            one = load_mbb_pbp([2026]).filter(pl.col("game_id") == 401830342)
            game_excitement(score_win_prob(one, league="mbb"), league="mbb")

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, league_id)
    for c in ("home_win_prob", "pregame_home_prob", "home_score", "away_score"):
        if c not in scored.columns:
            raise pl.exceptions.ColumnNotFoundError(f"game_excitement needs column {c!r}")
    if "game_id" in scored.columns and scored.height and scored["game_id"].n_unique() > 1:
        raise ValueError(f"game_excitement scores one game; got {scored['game_id'].n_unique()} game_id values")
    filled = scored.with_columns(pl.col("home_score", "away_score").forward_fill().fill_null(0))
    if completed is None:
        completed = False
        if {"type_text", "period_number"} <= set(scored.columns) and scored.height:
            reg = regulation_periods or game_regulation_periods(lg, season=_box_season(scored))
            completed = bool(filled.select(_game_end(reg).any()).item())
    if scored.height == 0:
        return {"gei": None, "lead_changes": 0, "max_swing_pts": None, "completed": completed}
    margin = (filled["home_score"] - filled["away_score"]).sign()
    final = None
    if completed:
        m = margin[-1]
        final = 1.0 if m > 0 else 0.0 if m < 0 else 0.5
    steps = _gei_steps(scored["home_win_prob"].cast(pl.Float64), float(scored["pregame_home_prob"][0]), final)
    total, biggest = steps.sum(), steps.max()
    leads = margin.filter(margin != 0)
    return {
        "gei": GEI_SCALE[lg] / scored.height * float(total),
        "lead_changes": int((leads != leads.shift(1)).sum()),
        "max_swing_pts": None if biggest is None else 100.0 * float(biggest),
        "completed": completed,
    }


_EXCITEMENT_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int64,
    "season": pl.Int64,
    "gei": pl.Float64,
    "lead_changes": pl.Int64,
    "max_swing_pts": pl.Float64,
    "completed": pl.Boolean,
    "gei_rank": pl.Int64,
    "gei_pct": pl.Float64,
    "gei_n": pl.Int64,
}


def season_game_excitement(
    pbp: pl.DataFrame,
    *,
    league_id: str | None = None,
    league: str | None = None,
    schedule: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Every game's excitement index, lead changes and biggest swing, ranked within its season.

    Scores each game with :func:`score_win_prob` and :func:`game_excitement`, then ranks the
    completed games of each season by GEI. A game whose pbp stops before the end (a truncated
    release) keeps its GEI so far but is not ranked.

    Args:
        pbp: Plays of one league in the basketball pbp schema (a season's ``load_<league>_pbp``
            release, or any set of games), the columns :func:`score_win_prob` needs plus
            ``game_id`` and ``season``.
        league_id: Legacy alias: ``"00"`` NBA / ``"10"`` WNBA.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        schedule: Optional ``load_<league>_schedule`` frame; its ``game_id`` / ``neutral_site``
            give each game's neutral site for the prior (games it lacks are not neutral).

    Returns:
        One row per game, sorted by ``season`` then ``game_id``: ``game_id``, ``season``, ``gei``,
        ``lead_changes``, ``max_swing_pts``, ``completed``, ``gei_rank`` (1 = the season's most
        exciting completed game, ties share the best rank), ``gei_pct`` (the share of the season's
        completed games with a GEI at or below this one, so the top game is 1.0) and ``gei_n`` (the
        season's completed games). The three rank columns are null for a game that is not
        completed. An empty input returns an empty frame with these columns.

    Raises:
        polars.exceptions.ColumnNotFoundError: If a required column is missing.
        ValueError: If the league is unknown or ``league`` contradicts ``league_id``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import season_game_excitement
            from sportsdataverse.wnba.wnba_loaders import load_wnba_pbp, load_wnba_schedule
            gei = season_game_excitement(load_wnba_pbp([2025]), league="wnba", schedule=load_wnba_schedule([2025]))

        The season's best games::

            gei.sort("gei_rank", nulls_last=True).head(10)

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, league_id)
    for c in ("game_id", "season"):
        if c not in pbp.columns:
            raise pl.exceptions.ColumnNotFoundError(f"season_game_excitement needs column {c!r}")
    neutral: dict[int, bool] = {}
    if schedule is not None and schedule.height:
        sched = schedule.select(pl.col("game_id").cast(pl.Int64), pl.col("neutral_site").cast(pl.Boolean))
        neutral = dict(zip(sched["game_id"].to_list(), sched["neutral_site"].to_list(), strict=True))
    if "game_play_number" in pbp.columns:  # score_win_prob reads each game in play order
        pbp = pbp.sort("game_id", "game_play_number", maintain_order=True)
    rows = []
    for (gid,), game in pbp.group_by("game_id", maintain_order=True):
        game_id = int(gid)
        scored = score_win_prob(game, league=lg, neutral_site=neutral.get(game_id))
        rows.append({"game_id": game_id, "season": _box_season(game), **game_excitement(scored, league=lg)})
    out = pl.DataFrame(rows, schema={k: _EXCITEMENT_SCHEMA[k] for k in list(_EXCITEMENT_SCHEMA)[:6]})
    q = pl.col("completed") & pl.col("gei").is_not_null()
    gei = pl.when(q).then(pl.col("gei"))
    return out.with_columns(
        gei.rank(method="min", descending=True).over("season").cast(pl.Int64).alias("gei_rank"),
        (gei.rank(method="max") / q.sum()).over("season").alias("gei_pct"),
        pl.when(q).then(q.sum().over("season")).cast(pl.Int64).alias("gei_n"),
    ).sort("season", "game_id")


def shot_coordinates(pbp: pl.DataFrame) -> pl.DataFrame:
    """Shot locations in the court frame the Game on Paper hex chart uses.

    ESPN's play coordinates are feet across the court with the hoop at ``(25, 0)`` and ``y``
    growing toward half court (measured on 401766458: dunks at ``y`` about 2, a 22-foot corner
    three at ``(3, 0)``). The chart wants tenths of a foot with the hoop at the origin, so
    ``x_legacy = (x - 25) * 10`` and ``y_legacy = y * 10``. Free throws carry no real location
    (the NBA frame pins them to ``(25, 13.75)``, the college frames to the rim, ``(25, 0)``;
    ``(?i)free ?throw`` matches ``Free Throw - 1 of 2`` and college ``MadeFreeThrow``) and ESPN's
    missing-coordinate sentinel is a large negative number; both become ``null``, as does every
    non-shooting play.

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
            * `sdvplot`_ -- ``court_coords()`` converts ``x_legacy`` / ``y_legacy`` to the court
              ``surface("nba")`` draws

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sdvplot: https://sdvplot.sportsdataverse.org/
    """
    located = (
        (pl.col("shooting_play") == True)
        & (pl.col("type_text").fill_null("").str.contains("(?i)free ?throw") == False)
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


#: First WBB season whose box records turnovers (2009-2012 under ``team_turnovers``); before it every key is 0.
_WBB_FIRST_TURNOVER_SEASON = 2009
#: A WBB game whose known turnover total (both teams) is below this did not record turnovers. The lowest
#: recorded total anywhere in WBB 2009-2026 is 7; totals of 1-4 occur only in two 2009 games whose plays log
#: 12/16 and 21/16 turnovers against a box of 0/1, and one broken 2018 box (0 FGA). Below 5 means unrecorded.
_WBB_MIN_RECORDED_GAME_TURNOVERS = 5


def _box_season(team_box: pl.DataFrame) -> Optional[int]:
    """The game's season from a frame's ``season`` column (every converter and sdv-db's team box carry it)."""
    if "season" not in team_box.columns:
        return None
    seasons = team_box["season"].drop_nulls()
    if seasons.dtype.is_float():  # a warehouse float column can carry NaN
        seasons = seasons.drop_nans()
    return int(seasons[0]) if seasons.len() else None


def _tov(team_box: pl.DataFrame, lg: str, season: Optional[int]) -> pl.Expr:
    """Team turnovers for the rates: the NBA repair for NBA / WNBA, the college rule for MBB / WBB.

    College boxes count every team turnover under ``turnovers`` (the play-by-play turnover count
    matches it), except WBB 2009-2012, which files it under ``team_turnovers`` with ``turnovers``
    and ``total_turnovers`` both 0; :func:`sportsdataverse.mbb.mbb_team_ratings._turnovers` reads
    both. A college 0 is a real 0 (TOV% 0.0), except in WBB where ESPN's box did not record
    turnovers: there a 0 means "not recorded" and becomes null, so every rate built on it is null
    rather than a number built on zero turnovers. That is every WBB 0 before 2009 (the box carries
    no turnovers at all; a box with no known season counts as before 2009), and, in any WBB
    season, a game whose two teams together show fewer than 5 turnovers (a game-level rule over
    the two rows, ``_WBB_MIN_RECORDED_GAME_TURNOVERS``: 99 such 2008-09 games). A team's 0 in a
    game whose total is 5 or more stays 0.
    """
    if lg not in _NCAA:
        return _team_tov(team_box.columns)
    from sportsdataverse.mbb.mbb_team_ratings import _turnovers

    tov = _turnovers(team_box)
    if lg == "wbb" and (season is None or season < _WBB_FIRST_TURNOVER_SEASON):
        return pl.when(tov > 0).then(tov).otherwise(pl.lit(None, dtype=pl.Float64)).cast(pl.Int32)
    if lg == "wbb" and team_box.select(tov.fill_null(0.0).sum()).item() < _WBB_MIN_RECORDED_GAME_TURNOVERS:
        return pl.lit(None, dtype=pl.Int32)  # the game's box did not record its turnovers
    return tov.cast(pl.Int32)


def _check_two_teams(team_box: pl.DataFrame, who: str) -> None:
    if team_box.height != 2:
        raise ValueError(f"{who} needs the two team rows of one game, got {team_box.height} team rows")
    if team_box["game_id"].n_unique() != 1 or team_box["team_id"].n_unique() != 2:
        raise ValueError(f"{who} needs two different teams of the same game_id")


def team_game_rates(
    team_box: pl.DataFrame,
    *,
    periods: Optional[int] = None,
    league_id: Optional[str] = None,
    league: Optional[str] = None,
    regulation_periods: Optional[int] = None,
) -> pl.DataFrame:
    """Four factors, possessions, ratings and pace for both teams of one game.

    Dean Oliver's estimates on the ``load_nba_team_boxscore`` columns: possessions =
    FGA - ORB + TOV + 0.44 FTA, averaged over the two teams; eFG% = (FGM + 0.5 3PM) / FGA;
    TOV% = TOV / (FGA + 0.44 FTA + TOV); ORB% = ORB / (ORB + opponent DRB); FT rate = FTA / FGA;
    ORtg / DRtg per 100 possessions; pace per regulation length (48 minutes NBA; 40 WNBA, MBB and WBB). ``TOV`` is the team's player plus team turnovers:
    ``total_turnovers``, except that when it is null it is ``turnovers + team_turnovers``, and when ESPN
    has duplicated the team turnover (``team_turnovers == turnovers`` and ``total_turnovers == 2 *
    turnovers``, measured on every 2002-2012 game, e.g. 17/17/34) it is ``turnovers``. Without the
    ``turnovers`` / ``team_turnovers`` columns it is ``total_turnovers`` as given. For MBB and WBB
    ``TOV`` is ``turnovers`` (``team_turnovers`` in WBB 2009-2012, where the other two are 0), and a 0
    is a real 0, except where the WBB box recorded none: in WBB before 2009 (and for a WBB box with
    no ``season``) and in a WBB game of any season whose two teams together show fewer than 5,
    TOV%, possessions, ratings and pace are null, never computed from unrecorded turnovers. A
    team's 0 in a game whose total is 5 or more stays 0. A zero denominator gives ``null``.
    For the NBA, ``orb_pct`` is null when either team's ``offensive_rebounds + defensive_rebounds``
    differs from its ``total_rebounds`` (ESPN's 2002-2012 boxes); a box without ``total_rebounds``
    and the other leagues are not checked.

    Args:
        team_box: Exactly two rows (one per team) of one game in the ``load_nba_team_boxscore``
            schema.
        periods: Highest period played (default: the regulation periods), for the pace denominator.
        league_id: Legacy alias: ``"00"`` NBA (48-minute regulation) / ``"10"`` WNBA (40).
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA); overtime is 5 minutes in all four.
        regulation_periods: The game's regulation periods (2 or 4); default
            :func:`game_regulation_periods` for the league and the box's ``season``.

    Returns:
        One row per ``(game_id, team_id)`` with ``efg_pct, tov_pct, orb_pct, ft_rate,
        possessions, off_rtg, def_rtg, pace, total_turnovers, game_minutes``.

    Raises:
        ValueError: If ``team_box`` does not hold exactly two rows, or the rows are not two different
            ``team_id`` values of one ``game_id``, or the league is unknown, or ``league``
            contradicts ``league_id``.
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

        A women's college game from the halves era::

            from sportsdataverse.wbb.wbb_loaders import load_wbb_team_boxscore
            box = load_wbb_team_boxscore([2010]).filter(pl.col("game_id") == 293170052)
            team_game_rates(box, league="wbb", regulation_periods=2)

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, league_id)
    _check_two_teams(team_box, "team_game_rates")
    season = _box_season(team_box)
    reg = regulation_periods if regulation_periods is not None else game_regulation_periods(lg, season=season)
    tov = _tov(team_box, lg, season)
    fga, fta = pl.col("field_goals_attempted"), pl.col("free_throws_attempted")
    # ESPN's NBA 2002-2012 boxes split rebounds that don't add up to total_rebounds (#757). NBA only:
    # college totals can include team rebounds, which the box doesn't carry separately.
    # A null total is no total, like a box without the column: it keeps the rate rather than nulling it.
    split_ok = (
        ((pl.col("offensive_rebounds") + pl.col("defensive_rebounds")) == pl.col("total_rebounds")).fill_null(True)
        if lg == "nba" and "total_rebounds" in team_box.columns
        else pl.lit(True)
    )
    own = team_box.with_columns(
        (fga - pl.col("offensive_rebounds") + tov + FT_WEIGHT * fta).cast(pl.Float64).alias("_own_poss"),
        split_ok.alias("_reb_split_ok"),
    )
    opp = own.select(
        pl.col("game_id"),
        pl.col("team_id").alias("_opp_id"),
        pl.col("team_score").alias("_opp_score"),
        pl.col("defensive_rebounds").alias("_opp_drb"),
        pl.col("_own_poss").alias("_opp_poss"),
        pl.col("_reb_split_ok").alias("_opp_reb_split_ok"),
    )
    joined = own.join(opp, on="game_id", how="inner").filter(pl.col("team_id") != pl.col("_opp_id"))
    minutes = game_minutes(periods if periods is not None else reg, league=lg, regulation_periods=reg)
    regulation = _REGULATION_MINUTES[lg]
    poss = (pl.col("_own_poss") + pl.col("_opp_poss")) / 2.0
    return joined.select(
        "game_id",
        "team_id",
        _ratio(pl.col("field_goals_made") + 0.5 * pl.col("three_point_field_goals_made"), fga).alias("efg_pct"),
        _ratio(tov, fga + FT_WEIGHT * fta + tov).alias("tov_pct"),
        pl.when((pl.col("_reb_split_ok") == True) & (pl.col("_opp_reb_split_ok") == True))
        .then(_ratio(pl.col("offensive_rebounds"), pl.col("offensive_rebounds") + pl.col("_opp_drb")))
        .alias("orb_pct"),
        _ratio(fta, fga).alias("ft_rate"),
        poss.alias("possessions"),
        _ratio(100.0 * pl.col("team_score"), poss).alias("off_rtg"),
        _ratio(100.0 * pl.col("_opp_score"), poss).alias("def_rtg"),
        (float(regulation) * poss / float(minutes)).alias("pace"),
        tov.alias("total_turnovers"),
        pl.lit(minutes, dtype=pl.Int64).alias("game_minutes"),
    )


def player_game_rates(
    player_box: pl.DataFrame,
    team_box: pl.DataFrame,
    *,
    periods: Optional[int] = None,
    league_id: Optional[str] = None,
    league: Optional[str] = None,
    regulation_periods: Optional[int] = None,
) -> pl.DataFrame:
    """True-shooting and usage for every boxed player of one game.

    TS% = PTS / (2 (FGA + 0.44 FTA)); USG% = 100 ((FGA + 0.44 FTA + TOV) (team minutes / 5)) /
    (MIN (team FGA + 0.44 team FTA + team TOV)) with team TOV as in :func:`team_game_rates`
    (``total_turnovers``, repaired where ESPN doubled or omitted it) and team
    minutes = 5 x game minutes (regulation 48 NBA / 40 WNBA, MBB and WBB, 5 per overtime). For MBB and
    WBB team TOV follows the college rule of :func:`team_game_rates` (null in WBB before 2009 and in a
    WBB game whose two teams together show fewer than 5, so usage is null there). A player with no shots, no minutes,
    or ``did_not_play`` gets ``null``.

    Args:
        player_box: One game in the ``load_nba_player_boxscore`` schema.
        team_box: The game's two ``load_nba_team_boxscore`` rows.
        periods: Highest period played (default: the regulation periods).
        league_id: Legacy alias: ``"00"`` NBA / ``"10"`` WNBA.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods (2 or 4); default
            :func:`game_regulation_periods` for the league and the team box's ``season``.

    Returns:
        One row per ``(game_id, athlete_id)`` with ``team_id, ts_pct, usg_pct``.

    Raises:
        ValueError: If ``team_box`` does not hold exactly two different teams of one ``game_id`` (a
            duplicated team row would double the player rows), or the league is unknown, or
            ``league`` contradicts ``league_id``.
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
    lg = _resolve_league(league, league_id)
    _check_two_teams(team_box, "player_game_rates")
    season = _box_season(team_box)
    reg = regulation_periods if regulation_periods is not None else game_regulation_periods(lg, season=season)
    tov = _tov(team_box, lg, season)
    team_minutes = 5.0 * game_minutes(periods if periods is not None else reg, league=lg, regulation_periods=reg)
    team = team_box.select(
        "game_id",
        "team_id",
        (pl.col("field_goals_attempted") + FT_WEIGHT * pl.col("free_throws_attempted") + tov)
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


#: The first-shot window of a transition possession: BB-D7's 8 seconds for the pros, the college
#: possession release's (bigballR's) 10 seconds for men's and women's college basketball.
TRANSITION_WINDOW_SECONDS: dict[str, float] = {
    "nba": TRANSITION_SECONDS,
    "wnba": TRANSITION_SECONDS,
    "mbb": 10.0,
    "wbb": 10.0,
}
#: How a possession started (:func:`team_game_splits`): the ball changed hands live after a
#: ``made_shot``, a ``def_rebound`` or a ``live_turnover`` (a steal), dead after a ``dead_turnover``,
#: at a ``period_start``, or ``other`` (a jump ball, a foul, an unlogged change). The first three can be transition.
_LIVE_STARTS: tuple[str, ...] = ("made_shot", "def_rebound", "live_turnover")

_TEAM_SPLITS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32,
    "team_id": pl.Int32,
    "points": pl.Int64,
    "possessions": pl.Int64,
    "ppp": pl.Float64,
    "non_possession_points": pl.Int64,
    "avg_possession_seconds": pl.Float64,
    **{
        f"{family}_{m}": dt
        for family in ("transition", "half_court", "second_chance", "first_chance")
        for m, dt in (("poss", pl.Int64), ("pts", pl.Int64), ("ppp", pl.Float64))
    },
    "tov": pl.Int64,
    "live_tov": pl.Int64,
    "dead_tov": pl.Int64,
    "tov_pct": pl.Float64,
    "pts_off_live_tov": pl.Int64,
    "pts_off_dead_tov": pl.Int64,
    "fga": pl.Int64,
    "fgm": pl.Int64,
    **{
        f"{zone}_{m}": dt
        for zone in ("rim", "mid", "fg2_unlocated", "fg3", "corner3", "above_break3", "fg3_unlocated", "layup_dunk")
        for m, dt in (("fga", pl.Int64), ("fgm", pl.Int64), ("fg_pct", pl.Float64))
    },
    "rim_rate": pl.Float64,
    "fg3a_rate": pl.Float64,
    "avg_fg2_distance": pl.Float64,
    "assisted_fgm": pl.Int64,
    "assisted_share": pl.Float64,
    "fta": pl.Int64,
    "ftm": pl.Int64,
    "ft_rate": pl.Float64,
    "bench_points": pl.Int64,
    "largest_lead": pl.Int64,
    "max_run": pl.Int64,
}
_PLAYER_SPLITS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int32,
    "athlete_id": pl.Int32,
    "team_id": pl.Int32,
    "minutes": pl.Float64,
    "est_possessions": pl.Float64,
    "points": pl.Int64,
    "fga": pl.Int64,
    "fgm": pl.Int64,
    "assisted_fgm": pl.Int64,
    "assisted_share": pl.Float64,
    "rim_fga": pl.Int64,
    "rim_fgm": pl.Int64,
    "rim_assisted_fgm": pl.Int64,
    "rim_assisted_share": pl.Float64,
    "fg3a": pl.Int64,
    "fg3m": pl.Int64,
    "fg3_assisted_fgm": pl.Int64,
    "fg3_assisted_share": pl.Float64,
    "assists": pl.Int64,
    "rebounds": pl.Int64,
    "tov": pl.Int64,
    "live_tov": pl.Int64,
    **{f"{m}_per100": pl.Float64 for m in ("pts", "ast", "reb", "tov")},
}


#: A college steal logged up to this many seconds from its turnover still marks the turnover live.
_STEAL_GAP_SECONDS = 3.0


def _shot_kind() -> pl.Expr:
    """``"fga"``, ``"ft"``, ``"tech"`` (a technical free throw) or null for a play, from the ``_split_events`` flags."""
    return (
        pl.when(pl.col("_fga"))
        .then(pl.lit("fga"))
        .when(pl.col("_tech"))
        .then(pl.lit("tech"))
        .when(pl.col("_ft"))
        .then(pl.lit("ft"))
    )


def _split_events(pbp: pl.DataFrame) -> pl.DataFrame:
    """Every play with its event flags, its score change per side, and the possession it belongs to.

    A possession is a run of the same offense: a field-goal attempt, a free throw (not a technical),
    a turnover or a rebound names the offense (a defensive rebound is the new offense's first
    event), and a possession ends when the next such event is the other team's or the period ends.
    Every other play (fouls, substitutions, timeouts, steals) belongs to the possession in progress.
    """
    home, away = int(pbp["home_team_id"][0]), int(pbp["away_team_id"][0])
    tt, txt = pl.col("type_text").fill_null(""), pl.col("text").fill_null("")
    shoot = pl.col("shooting_play").fill_null(False)
    is_ft = shoot & tt.str.contains("(?i)free ?throw")
    clk, per = pl.col("_clk"), pl.col("period_number")
    # college logs the steal as its own play next to the turnover, by the other team, at the same
    # clock (2013-14 WBB: one to two seconds later)
    steal, team = tt == "Steal", pl.col("team_id")

    def beside(n: int) -> pl.Expr:
        near = (clk.shift(n) - clk).abs() <= _STEAL_GAP_SECONDS
        return steal.shift(n) & near & (per.shift(n) == per) & (team.shift(n) != team)

    hs, as_ = (pl.col(c).forward_fill().fill_null(0).cast(pl.Int64) for c in ("home_score", "away_score"))
    rows = pbp.with_row_index("_i").with_columns(
        _period_clock(pbp).alias("_clk"),
        pl.col("team_id").cast(pl.Int32).alias("_team"),
        hs.alias("_hs"),
        as_.alias("_as"),
    )
    # a three: points_attempted 3 where the frame has it; otherwise the text says "three point", or
    # (older NBA misses read "misses 25-foot", or nothing) the text gives 24 feet or more or the
    # play sits 22 feet or more to the side or 23.75 feet or more out, past every league's arc
    x, y = pl.col("x_legacy").cast(pl.Float64), pl.col("y_legacy").cast(pl.Float64)
    beyond = ((x.abs() >= 220.0) | ((x**2 + y**2).sqrt() >= 237.5)).fill_null(False)
    three = txt.str.contains("(?i)three point") | (
        (txt.str.contains(r"\b(2[4-9]|[3-9]\d)-foot") | beyond) & ~txt.str.contains("(?i)two point")
    )
    if "points_attempted" in pbp.columns:
        three = (pl.col("points_attempted") == 3).fill_null(False) | txt.str.contains("(?i)three point")
    rows = rows.with_columns(
        (shoot & ~is_ft).alias("_fga"),
        (shoot & ~is_ft & three).alias("_three"),
        is_ft.alias("_ft"),
        (is_ft & (tt.str.contains("(?i)technical") | txt.str.contains("(?i)technical"))).alias("_tech"),
        pl.col("scoring_play").fill_null(False).alias("_made"),
        (tt.str.contains("(?i)turnover") | (tt == "Traveling")).alias("_tov"),
        tt.str.contains("(?i)rebound").alias("_reb"),
        (txt.str.contains("(?i)steal") | (beside(1) | beside(-1)).fill_null(False)).alias("_stolen"),
        tt.str.contains("(?i)time ?out").alias("_timeout"),
        (pl.col("_hs") - pl.col("_hs").shift(1, fill_value=0)).alias("_dh"),
        (pl.col("_as") - pl.col("_as").shift(1, fill_value=0)).alias("_da"),
    ).with_columns(
        (
            (pl.col("_fga") | (pl.col("_ft") & ~pl.col("_tech")) | pl.col("_tov") | pl.col("_reb"))
            & pl.col("_team").is_in([home, away])
        ).alias("_off"),
        pl.col("_timeout").cast(pl.Int64).cum_sum().alias("_timeouts"),
        # the most recent shot before the play, and the next shot after it, with its clock
        _shot_kind().forward_fill().shift(1).alias("_prev_shot"),
        _shot_kind().backward_fill().shift(-1).alias("_next_shot"),
        pl.when(_shot_kind().is_not_null()).then(clk).backward_fill().shift(-1).alias("_next_shot_clk"),
    )
    ev = rows.filter(pl.col("_off")).with_columns(
        ((pl.col("_team") != pl.col("_team").shift(1)) | (per != per.shift(1))).fill_null(True).alias("_new")
    )
    ev = ev.with_columns(
        pl.col("_new").cast(pl.Int64).cum_sum().alias("_poss"),
        # not a chance: the dead-ball rebound between two free throws of a trip, the one after a
        # technical free throw, and one with no time left in the period
        (
            pl.col("_reb")
            & ~pl.col("_new")
            & (clk > 0)
            & (pl.col("_prev_shot") != "tech").fill_null(True)
            & ~(
                (pl.col("_prev_shot") == "ft")
                & pl.col("_next_shot").is_in(["ft", "tech"])
                & (pl.col("_next_shot_clk") == clk)
            ).fill_null(False)
        ).alias("_oreb"),
    )
    tagged = rows.join_asof(ev.select("_i", "_poss"), on="_i", strategy="backward")
    return tagged.join(ev.select("_i", "_new", "_oreb"), on="_i", how="left").with_columns(
        pl.col("_new", "_oreb").fill_null(False), pl.lit(home).alias("_home"), pl.lit(away).alias("_away")
    )


def _possessions(rows: pl.DataFrame, lg: str, reg: int) -> pl.DataFrame:
    """One row per possession: offense, start type, transition, second chance, points, length."""
    ev = rows.filter(pl.col("_off"))
    poss = (
        ev.group_by("_poss", maintain_order=True)
        .agg(
            pl.col("period_number").first().alias("period"),
            pl.col("_team").first().alias("offense"),
            pl.col("_reb").first().alias("_starts_on_reb"),
            pl.col("_i").last().alias("_last_i"),
            pl.col("_clk").last().alias("_end_clk"),
            (pl.col("_made") & (pl.col("_fga") | pl.col("_ft"))).last().alias("_ends_made"),
            pl.col("_tov").last().alias("_ends_tov"),
            (pl.col("_tov") & pl.col("_stolen")).last().alias("_ends_live_tov"),
            pl.col("_i").filter(pl.col("_fga")).first().alias("_shot_i"),
            pl.col("_clk").filter(pl.col("_fga")).first().alias("_shot_clk"),
            pl.col("_oreb").any().alias("second_chance"),
        )
        .sort("_poss")
    )
    period_secs = 60.0 * _REGULATION_MINUTES[lg] / reg
    same = pl.col("period") == pl.col("period").shift(1)
    prev_kind = (
        pl.when(~same.fill_null(False))
        .then(pl.lit("period_start"))
        .when(pl.col("_starts_on_reb"))
        .then(pl.lit("def_rebound"))
        .when(pl.col("_ends_made").shift(1))
        .then(pl.lit("made_shot"))
        .when(pl.col("_ends_live_tov").shift(1))
        .then(pl.lit("live_turnover"))
        .when(pl.col("_ends_tov").shift(1))
        .then(pl.lit("dead_turnover"))
        .otherwise(pl.lit("other"))
    )
    start_clk = (
        pl.when(same.fill_null(False))
        .then(pl.col("_end_clk").shift(1))
        .otherwise(pl.when(pl.col("period") <= reg).then(pl.lit(period_secs)).otherwise(pl.lit(300.0)))
    )
    timeouts = rows["_timeouts"]
    poss = poss.with_columns(
        prev_kind.alias("start_type"), start_clk.alias("_start_clk"), pl.col("_last_i").shift(1).alias("_prev_i")
    )
    # a timeout between the change of possession and the first shot is a set play, not transition
    stopped = [
        s is not None and p is not None and timeouts[s] > timeouts[p]
        for s, p in zip(poss["_shot_i"].to_list(), poss["_prev_i"].to_list())
    ]
    pts = (
        rows.filter(pl.col("_poss").is_not_null())
        .join(poss.select("_poss", "offense"), on="_poss")
        .group_by("_poss")
        .agg(
            pl.when(pl.col("offense") == pl.col("_home"))
            .then(pl.col("_dh"))
            .otherwise(pl.col("_da"))
            .sum()
            .alias("pts"),
            pl.when(pl.col("offense") == pl.col("_home"))
            .then(pl.col("_da"))
            .otherwise(pl.col("_dh"))
            .sum()
            .alias("_def_pts"),
        )
    )
    window = TRANSITION_WINDOW_SECONDS[lg]
    return (
        poss.with_columns(pl.Series("_stopped", stopped, dtype=pl.Boolean))
        .join(pts, on="_poss", how="left")
        .with_columns(
            (
                pl.col("start_type").is_in(_LIVE_STARTS)
                & ((pl.col("_start_clk") - pl.col("_shot_clk")) <= window).fill_null(False)
                & ~pl.col("_stopped")
            ).alias("transition"),
            (pl.col("_start_clk") - pl.col("_end_clk")).clip(lower_bound=0.0).alias("seconds"),
            pl.col("pts", "_def_pts").fill_null(0),
        )
    )


def _zone_counts(shots: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """FGA / FGM by zone (``rim`` under :data:`RIM_DISTANCE_FT`, ``mid``, threes split corner / above the break)."""
    dist = (pl.col("x_legacy") ** 2 + pl.col("y_legacy") ** 2).sqrt() / 10.0
    three = pl.col("_three")
    located = pl.col("x_legacy").is_not_null() & pl.col("y_legacy").is_not_null()
    zones = {
        "rim": ~three & located & (dist < RIM_DISTANCE_FT),
        "mid": ~three & located & (dist >= RIM_DISTANCE_FT),
        "fg2_unlocated": ~three & ~located,
        "fg3": three,
        "corner3": three & located & (pl.col("y_legacy") <= CORNER_THREE_MAX_Y),
        "above_break3": three & located & (pl.col("y_legacy") > CORNER_THREE_MAX_Y),
        "fg3_unlocated": three & ~located,
        "layup_dunk": pl.col("type_text").fill_null("").str.contains("(?i)lay ?up|dunk"),
    }
    made = pl.col("_made")
    return shots.group_by(by).agg(
        pl.len().cast(pl.Int64).alias("fga"),
        made.sum().cast(pl.Int64).alias("fgm"),
        *[
            x
            for z, m in zones.items()
            for x in (m.sum().cast(pl.Int64).alias(f"{z}_fga"), (m & made).sum().cast(pl.Int64).alias(f"{z}_fgm"))
        ],
        (made & pl.col("text").fill_null("").str.contains("(?i)assist")).sum().cast(pl.Int64).alias("assisted_fgm"),
        dist.filter(~three & located).mean().alias("avg_fg2_distance"),
        *[
            (made & m & pl.col("text").fill_null("").str.contains("(?i)assist"))
            .sum()
            .cast(pl.Int64)
            .alias(f"{z}_assisted_fgm")
            for z, m in (("rim", zones["rim"]), ("fg3", three))
        ],
    )


def team_game_splits(
    pbp: pl.DataFrame,
    player_box: Optional[pl.DataFrame] = None,
    *,
    league: Optional[str] = None,
    regulation_periods: Optional[int] = None,
) -> pl.DataFrame:
    """Situational splits for both teams of one game, counted from its play-by-play (works mid-game).

    Possessions are counted, not estimated: a field-goal attempt, a free throw, a turnover or a
    rebound names the offense (a defensive rebound is the new offense's first event), and a
    possession runs until the next such play is the other team's or the period ends. On the
    fixtures the count runs 0.95-1.01 of the box estimate (FGA - ORB + TOV + 0.44 FTA) in the pros
    and 0.99-1.05 in college. A technical free throw neither starts nor ends a possession, and its
    points land in ``non_possession_points`` when the shooting team was on defense. ``points`` is every point the
    team scored (the play-by-play's running score), ``ppp`` is ``points / possessions``, and each
    split pair partitions the possessions and their points: ``transition`` + ``half_court`` and
    ``second_chance`` + ``first_chance`` both add up to ``possessions`` and to ``points -
    non_possession_points``. A **transition** possession started live (after a made shot, a defensive
    rebound or a steal; not a period's first) and took its first field-goal attempt within
    :data:`TRANSITION_WINDOW_SECONDS` of the change of possession, with no timeout in between. A
    **second-chance** possession had an offensive rebound, team rebounds included (not the dead-ball
    one between two free throws of a trip, one after a technical free throw, or one with no time
    left). A turnover is **live** when a steal is credited on it, otherwise **dead**;
    ``pts_off_live_tov`` / ``pts_off_dead_tov`` are the points the team scored on the possessions
    that started from the opponent's live / dead turnovers. Shot zones read the play coordinates of
    :func:`shot_coordinates`: ``rim`` is a two under 4 feet, ``mid`` any other located two,
    ``corner3`` a three at or below 8.75 feet above the rim's line (``y_legacy`` 87.5), ``above_break3``
    any other located three; plays without a location count in ``fg2_unlocated`` / ``fg3_unlocated``,
    so ``rim + mid + fg2_unlocated + fg3 = fga`` and ``corner3 + above_break3 + fg3_unlocated = fg3``.
    A three is ``points_attempted`` 3; a frame without that column (older captures) reads the
    text ("three point", or 24 feet or more) and the location (22 feet or more to the side, or
    23.75 out). ``layup_dunk`` is any shot whose play type names a layup or a dunk. ``assisted_fgm`` is the makes
    whose text credits an assist. ``largest_lead`` is the team's biggest lead and ``max_run`` its
    biggest unanswered run (:mod:`sportsdataverse._common.game_flow`'s rule). ``bench_points`` needs
    the player box's ``starter`` flag (null without it). ESPN's college feed has no technical-foul
    marker on its free throws, so a college technical is counted as a possession of the shooting team.

    Args:
        pbp: One game's plays in the basketball pbp schema, after :func:`shot_coordinates` (needs
            ``type_text``, ``text``, ``shooting_play``, ``scoring_play``, ``points_attempted`` if present,
            ``team_id``, ``home_team_id``, ``away_team_id``, ``home_score``, ``away_score``,
            ``period_number``, a period clock and ``x_legacy`` / ``y_legacy``), in play order.
        player_box: The game's player box, for ``bench_points`` (``team_id``, ``points``, ``starter``).
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods (2 or 4), for the first possession's clock;
            default :func:`game_regulation_periods` from the frame's ``season``.

    Returns:
        One row per team (``game_id``, ``team_id``, the totals, the four split families as
        ``<family>_poss`` / ``_pts`` / ``_ppp``, the turnover, zone, assist and free-throw columns,
        ``bench_points``, ``largest_lead``, ``max_run``); an empty frame with those columns before
        tip-off.

    Raises:
        ValueError: If the league is unknown or ``pbp`` holds more than one game.
        polars.exceptions.ColumnNotFoundError: If a required column is missing.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_page_frames
            from sportsdataverse.nba.nba_pbp import espn_nba_pbp
            frames = game_page_frames(espn_nba_pbp(401766458, return_parsed=False))
            frames["team_game_splits"].select("team_id", "possessions", "ppp", "transition_ppp")

        From warehouse frames::

            import polars as pl
            from sportsdataverse.nba.nba_game_page import shot_coordinates, team_game_splits
            from sportsdataverse.wbb.wbb_loaders import load_wbb_pbp, load_wbb_player_boxscore
            pbp = load_wbb_pbp([2024]).filter(pl.col("game_id") == 401638645)
            box = load_wbb_player_boxscore([2024]).filter(pl.col("game_id") == 401638645)
            team_game_splits(shot_coordinates(pbp), box, league="wbb")

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, None)
    if pbp.height == 0:
        return pl.DataFrame(schema=_TEAM_SPLITS_SCHEMA)
    if pbp["game_id"].n_unique() > 1:
        raise ValueError(f"team_game_splits reads one game; got {pbp['game_id'].n_unique()} game_id values")
    reg = regulation_periods if regulation_periods is not None else game_regulation_periods(lg, season=_box_season(pbp))
    rows = _split_events(pbp)
    poss = _possessions(rows, lg, reg)
    home, away = int(rows["_home"][0]), int(rows["_away"][0])
    teams = pl.DataFrame(
        {"team_id": [home, away], "_opp": [away, home]}, schema={"team_id": pl.Int32, "_opp": pl.Int32}
    )
    total = rows.select(pl.col("_dh").sum().alias("h"), pl.col("_da").sum().alias("a")).row(0)
    margin = rows.select((pl.col("_hs") - pl.col("_as")).alias("m"))["m"]
    lead = {home: max(int(margin.max() or 0), 0), away: max(int(-(margin.min() or 0)), 0)}

    def fam(name: str, mask: pl.Expr) -> list[pl.Expr]:
        return [
            mask.sum().cast(pl.Int64).alias(f"{name}_poss"),
            pl.col("pts").filter(mask).sum().cast(pl.Int64).alias(f"{name}_pts"),
        ]

    off = poss.group_by(pl.col("offense").alias("team_id")).agg(
        pl.len().cast(pl.Int64).alias("possessions"),
        pl.col("pts").sum().cast(pl.Int64).alias("_poss_pts"),
        pl.col("seconds").mean().alias("avg_possession_seconds"),
        *fam("transition", pl.col("transition")),
        *fam("half_court", ~pl.col("transition")),
        *fam("second_chance", pl.col("second_chance")),
        *fam("first_chance", ~pl.col("second_chance")),
        pl.col("pts").filter(pl.col("start_type") == "live_turnover").sum().cast(pl.Int64).alias("pts_off_live_tov"),
        pl.col("pts").filter(pl.col("start_type") == "dead_turnover").sum().cast(pl.Int64).alias("pts_off_dead_tov"),
    )
    ev = rows.filter(pl.col("_team").is_in([home, away]))
    tov = (
        ev.filter(pl.col("_tov"))
        .group_by(pl.col("_team").alias("team_id"))
        .agg(pl.len().cast(pl.Int64).alias("tov"), pl.col("_stolen").sum().cast(pl.Int64).alias("live_tov"))
    )
    fts = (
        ev.filter(pl.col("_ft"))
        .group_by(pl.col("_team").alias("team_id"))
        .agg(pl.len().cast(pl.Int64).alias("fta"), pl.col("_made").sum().cast(pl.Int64).alias("ftm"))
    )
    shots = _zone_counts(ev.filter(pl.col("_fga")).with_columns(pl.col("_team").alias("team_id")), ["team_id"])
    bench = None
    if player_box is not None and {"starter", "points", "team_id"} <= set(player_box.columns) and player_box.height:
        bench = (
            player_box.filter(pl.col("starter").fill_null(False) == False)
            .group_by(pl.col("team_id").cast(pl.Int32))
            .agg(pl.col("points").sum().cast(pl.Int64).alias("bench_points"))
        )
    out = teams
    for part in (off, tov, fts, shots) + ((bench,) if bench is not None else ()):
        out = out.join(part.with_columns(pl.col("team_id").cast(pl.Int32)), on="team_id", how="left")
    counts = [c for c, dt in _TEAM_SPLITS_SCHEMA.items() if dt == pl.Int64 and c in out.columns and c != "bench_points"]
    out = out.with_columns(pl.col(counts).fill_null(0))
    runs = _runs(
        _states(
            rows.select(
                pl.lit("g").alias("game_id"),
                pl.lit(0.0).alias("elapsed"),
                pl.lit(0.0).alias("game_seconds"),
                pl.col("_hs").alias("home_score"),
                pl.col("_as").alias("away_score"),
            )
        )
    )
    run_of = {(home if side == "home" else away): int(n) for side, n in zip(runs["side"], runs["max_run"])}
    pct = {
        f"{z}_fg_pct": _ratio(pl.col(f"{z}_fgm"), pl.col(f"{z}_fga"))
        for z in ("rim", "mid", "fg2_unlocated", "fg3", "corner3", "above_break3", "fg3_unlocated", "layup_dunk")
    }
    out = out.with_columns(
        pl.lit(int(rows["game_id"][0]), dtype=pl.Int32).alias("game_id"),
        pl.col("team_id").replace_strict({home: total[0], away: total[1]}, return_dtype=pl.Int64).alias("points"),
        pl.col("team_id").replace_strict(lead, return_dtype=pl.Int64).alias("largest_lead"),
        pl.col("team_id").replace_strict(run_of, default=0, return_dtype=pl.Int64).alias("max_run"),
        (pl.col("tov") - pl.col("live_tov")).alias("dead_tov"),
        **pct,
    ).with_columns(
        _ratio(pl.col("points"), pl.col("possessions")).alias("ppp"),
        (pl.col("points") - pl.col("_poss_pts")).alias("non_possession_points"),
        *[
            _ratio(pl.col(f"{f}_pts"), pl.col(f"{f}_poss")).alias(f"{f}_ppp")
            for f in ("transition", "half_court", "second_chance", "first_chance")
        ],
        _ratio(pl.col("tov"), pl.col("possessions")).alias("tov_pct"),
        _ratio(pl.col("rim_fga"), pl.col("fga")).alias("rim_rate"),
        _ratio(pl.col("fg3_fga"), pl.col("fga")).alias("fg3a_rate"),
        _ratio(pl.col("assisted_fgm"), pl.col("fgm")).alias("assisted_share"),
        _ratio(pl.col("fta"), pl.col("fga")).alias("ft_rate"),
        *([] if bench is not None else [pl.lit(None, dtype=pl.Int64).alias("bench_points")]),
    )
    return out.select([pl.col(c).cast(dt) for c, dt in _TEAM_SPLITS_SCHEMA.items()])


def player_game_splits(
    pbp: pl.DataFrame,
    player_box: Optional[pl.DataFrame] = None,
    *,
    league: Optional[str] = None,
    regulation_periods: Optional[int] = None,
) -> pl.DataFrame:
    """Per-player shot, assist and turnover splits of one game, counted from its play-by-play (works mid-game).

    Counted from the plays the player is the first athlete on (``athlete_id_1``): field goals (rim,
    three and their assisted makes, as in :func:`team_game_splits`), turnovers (``live_tov`` with a
    steal credited), rebounds and points (made shots and free throws); ``assists`` are the made
    shots crediting the player as the passer (``athlete_id_2`` on a make whose text names an assist).
    ``est_possessions`` is an estimate, not an on-court count (that needs the lineup on each play):
    the team's counted possessions times the player's share of the team's minutes (the box's
    ``minutes`` over the team's minutes / 5), so a mid-game box gives the share so far. The per-100
    rates (``pts``, ``ast``, ``reb``, ``tov``) divide by it; they are null without minutes.

    Args:
        pbp: One game's plays, as :func:`team_game_splits` takes them (plus ``athlete_id_1`` /
            ``athlete_id_2``).
        player_box: The game's player box (``athlete_id``, ``team_id``, ``minutes``); every boxed
            player gets one row (a play crediting a player to the other team is left out); without a
            box, every player on the plays gets one, with null minutes.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods; see :func:`team_game_splits`.

    Returns:
        One row per player (``game_id``, ``athlete_id``, ``team_id``, ``minutes``,
        ``est_possessions``, the shot, assist, rebound and turnover counts and shares, and the four
        ``<m>_per100`` rates); an empty frame with those columns when there are no plays and no box.

    Raises:
        ValueError: If the league is unknown or ``pbp`` holds more than one game.
        polars.exceptions.ColumnNotFoundError: If a required column is missing.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_page_frames
            from sportsdataverse.wnba.wnba_pbp import espn_wnba_pbp
            frames = game_page_frames(espn_wnba_pbp(401726992, return_parsed=False), league="wnba")
            frames["player_game_splits"].sort("est_possessions", descending=True).head()

        Who scored at the rim off passes::

            frames["player_game_splits"].select("athlete_id", "rim_fgm", "rim_assisted_share")

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, None)
    box = player_box if player_box is not None and player_box.height and "athlete_id" in player_box.columns else None
    if pbp.height == 0 and box is None:
        return pl.DataFrame(schema=_PLAYER_SPLITS_SCHEMA)
    if pbp.height and pbp["game_id"].n_unique() > 1:
        raise ValueError(f"player_game_splits reads one game; got {pbp['game_id'].n_unique()} game_id values")
    key = ["athlete_id", "team_id"]
    if pbp.height:
        team = team_game_splits(pbp, league=lg, regulation_periods=regulation_periods).select("team_id", "possessions")
        rows = _split_events(pbp).filter(pl.col("_team").is_in(team["team_id"].to_list()))
        game_id = int(rows["game_id"][0]) if rows.height else int(pbp["game_id"][0])
        # only the counted plays: a foul's team_id can be the other side's (WNBA 2017). ESPN's older
        # plays can also name the stealer first on a turnover ("A bad pass (B steals)", athlete_id_1
        # B): with a box, a turnover whose first athlete plays for the other team goes to the second.
        a1, a2 = pl.col("athlete_id_1").cast(pl.Int32), pl.col("athlete_id_2").cast(pl.Int32)
        actor = a1
        if box is not None:
            roster = dict(box.select(pl.col("athlete_id").cast(pl.Int32), pl.col("team_id").cast(pl.Int32)).iter_rows())
            other_side = (a1.replace_strict(roster, default=None) != pl.col("_team")).fill_null(False)
            actor = pl.when(pl.col("_tov") & other_side & a2.is_not_null()).then(a2).otherwise(a1)
        mine = rows.filter(pl.col("athlete_id_1").is_not_null() & (pl.col("_off") | pl.col("_ft"))).with_columns(
            actor.alias("athlete_id"), pl.col("_team").alias("team_id")
        )
        shots = _zone_counts(mine.filter(pl.col("_fga")), key).rename({"fg3_fga": "fg3a", "fg3_fgm": "fg3m"})
        other = mine.group_by(key).agg(
            # not score_value: 2015-16 NBA free throws all ship scoreValue 0
            pl.when(pl.col("_made") & pl.col("_ft"))
            .then(1)
            .when(pl.col("_made") & pl.col("_fga"))
            .then(2 + pl.col("_three").cast(pl.Int64))
            .otherwise(0)
            .sum()
            .cast(pl.Int64)
            .alias("points"),
            pl.col("_reb").sum().cast(pl.Int64).alias("rebounds"),
            pl.col("_tov").sum().cast(pl.Int64).alias("tov"),
            (pl.col("_tov") & pl.col("_stolen")).sum().cast(pl.Int64).alias("live_tov"),
        )
        ast = (
            rows.filter(
                pl.col("_fga")
                & pl.col("_made")
                & pl.col("text").fill_null("").str.contains("(?i)assist")
                & pl.col("athlete_id_2").is_not_null()
            )
            .group_by(pl.col("athlete_id_2").cast(pl.Int32).alias("athlete_id"), pl.col("_team").alias("team_id"))
            .agg(pl.len().cast(pl.Int64).alias("assists"))
        )
        counted = other.join(shots, on=key, how="full", coalesce=True).join(ast, on=key, how="full", coalesce=True)
    else:
        assert box is not None  # the no-plays, no-box case returned above
        team = pl.DataFrame(schema={"team_id": pl.Int32, "possessions": pl.Int64})
        game_id = int(box["game_id"][0])
        counted = pl.DataFrame(schema={"athlete_id": pl.Int32, "team_id": pl.Int32})
    if box is not None:
        minutes = box.select(
            pl.col("athlete_id").cast(pl.Int32),
            pl.col("team_id").cast(pl.Int32),
            (pl.col("minutes").cast(pl.Float64) if "minutes" in box.columns else pl.lit(None, dtype=pl.Float64)).alias(
                "minutes"
            ),
        )
        # the box's players only: ESPN's older plays can name the stealer first on a turnover (WNBA 2017)
        counted = minutes.join(counted, on=key, how="left")
    else:
        counted = counted.with_columns(pl.lit(None, dtype=pl.Float64).alias("minutes"))
    out = counted.join(team.with_columns(pl.col("team_id").cast(pl.Int32)), on="team_id", how="left")
    counts = [c for c, dt in _PLAYER_SPLITS_SCHEMA.items() if dt == pl.Int64]
    out = out.with_columns(
        [(pl.col(c) if c in out.columns else pl.lit(None, dtype=pl.Int64)).fill_null(0).alias(c) for c in counts]
    )
    share = pl.col("minutes") / (pl.col("minutes").sum().over("team_id") / 5.0)
    est = pl.col("possessions").cast(pl.Float64) * share
    return (
        out.with_columns(
            pl.lit(game_id, dtype=pl.Int32).alias("game_id"),
            pl.when(pl.col("minutes") > 0).then(est).alias("est_possessions"),
        )
        .with_columns(
            _ratio(pl.col("assisted_fgm"), pl.col("fgm")).alias("assisted_share"),
            _ratio(pl.col("rim_assisted_fgm"), pl.col("rim_fgm")).alias("rim_assisted_share"),
            _ratio(pl.col("fg3_assisted_fgm"), pl.col("fg3m")).alias("fg3_assisted_share"),
            *[
                _ratio(100.0 * pl.col(src), pl.col("est_possessions")).alias(f"{m}_per100")
                for m, src in (("pts", "points"), ("ast", "assists"), ("reb", "rebounds"), ("tov", "tov"))
            ],
        )
        .select([pl.col(c).cast(dt) for c, dt in _PLAYER_SPLITS_SCHEMA.items()])
    )


def game_page_tables(
    pbp: pl.DataFrame,
    team_box: pl.DataFrame,
    player_box: pl.DataFrame,
    *,
    league_id: Optional[str] = None,
    league: Optional[str] = None,
    regulation_periods: Optional[int] = None,
    neutral_site: Optional[bool] = False,
    rated_home_prob: Optional[float] = None,
) -> dict[str, object]:
    """Every game-page table for one game, from its basketball pbp / team box / player box frames.

    The warehouse path: the sdv-db nightly stage reads one game's rows from ``<league>.pbp`` /
    ``team_box`` / ``player_box`` and its schedule row (``season``, ``format_regulation_periods``,
    ``neutral_site``) and calls this. :func:`game_page_frames` builds the same frames live from ESPN
    and calls this too, so a game renders identically from either source. Runs
    :func:`score_win_prob`, :func:`shot_coordinates`, :func:`team_game_rates`,
    :func:`player_game_rates`, :func:`team_game_splits` and :func:`player_game_splits` on the
    game's own regulation.

    Args:
        pbp: One game's plays in the basketball pbp schema, in play order (empty before tip-off).
        team_box: The game's team box rows (rates need exactly two).
        player_box: The game's player box rows.
        league_id: Legacy alias: ``"00"`` NBA / ``"10"`` WNBA.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        regulation_periods: The game's regulation periods; default :func:`game_regulation_periods`
            from the frame's ``season``. Pass the schedule's ``format_regulation_periods`` through
            :func:`game_regulation_periods` when you have it.
        neutral_site: Whether the game was at a neutral site (no home-court advantage in a lineless
            prior); ``None``, NaN or ``pd.NA`` counts as false.
        rated_home_prob: The game's rated pregame home win probability, the prior when there is no
            line (NBA / WNBA: :func:`~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`).

    Returns:
        ``{"pbp", "team_game_rates", "player_game_rates", "team_game_splits", "player_game_splits",
        "periods", "regulation_periods", "period_kind", "prior"}``: the scored plays with shot
        coordinates, the two rate frames (empty with their documented columns without two team
        rows), the two split frames (counted from the plays so far; the team frame is empty before
        tip-off, the player frame then holds the boxed players with zero counts), the highest period played
        (the regulation count before tip-off), the regulation periods, ``"half"`` or ``"quarter"``,
        and ``{"home_win_prob", "source", "home_team_spread", "game_spread_available"}``.

    Raises:
        ValueError: If the league is unknown, ``league`` contradicts ``league_id``, or ``pbp`` holds
            more than one game.
        polars.exceptions.ColumnNotFoundError: If a frame lacks a column the derivations need.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_game_page import game_page_tables, game_regulation_periods
            from sportsdataverse.wbb.wbb_loaders import load_wbb_pbp, load_wbb_player_boxscore, load_wbb_team_boxscore
            gid = 293170052
            pbp = load_wbb_pbp([2010]).filter(pl.col("game_id") == gid)
            tb = load_wbb_team_boxscore([2010]).filter(pl.col("game_id") == gid)
            pb = load_wbb_player_boxscore([2010]).filter(pl.col("game_id") == gid)
            tables = game_page_tables(pbp, tb, pb, league="wbb", regulation_periods=game_regulation_periods("wbb", season=2010))

        The win-probability chart::

            tables["pbp"].select("game_play_number", "home_win_prob", "wp_delta", "wp_regime")

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, league_id)
    season = _box_season(pbp) or _box_season(team_box)  # before tip-off the pbp is empty
    reg = regulation_periods if regulation_periods is not None else game_regulation_periods(lg, season=season)
    periods = int(pbp["period_number"].max() or reg) if pbp.height else reg
    if pbp.height:  # a game with 10 or fewer plays parses to a zero-column frame; nothing to score yet
        pbp = shot_coordinates(
            score_win_prob(
                pbp, league=lg, regulation_periods=reg, neutral_site=neutral_site, rated_home_prob=rated_home_prob
            )
        )
        prior_prob, prior_source = pbp["pregame_home_prob"][0], pbp["prior_source"][0]
        spread, available = pbp["home_team_spread"][0], bool(pbp["game_spread_available"][0] or False)
    else:
        (prior_prob, prior_source), spread, available = (
            home_prior(None, None, league=lg, neutral_site=neutral_site, rated_home_prob=rated_home_prob),
            None,
            False,
        )
    two_teams = team_box.height == 2
    if season is not None and "season" not in team_box.columns:  # D12: _tov reads the season from the box
        team_box = team_box.with_columns(pl.lit(season).alias("season"))
    return {
        "pbp": pbp,
        "team_game_rates": team_game_rates(team_box, periods=periods, league=lg, regulation_periods=reg)
        if two_teams
        else pl.DataFrame(schema=_TEAM_RATES_SCHEMA),
        "player_game_rates": player_game_rates(player_box, team_box, periods=periods, league=lg, regulation_periods=reg)
        if two_teams and not player_box.is_empty()
        else pl.DataFrame(schema=_PLAYER_RATES_SCHEMA),
        "team_game_splits": team_game_splits(pbp, player_box, league=lg, regulation_periods=reg),
        "player_game_splits": player_game_splits(pbp, player_box, league=lg, regulation_periods=reg),
        "periods": periods,
        "regulation_periods": reg,
        "period_kind": "half" if reg == 2 else "quarter",
        "prior": {
            "home_win_prob": prior_prob,
            "source": prior_source,
            "home_team_spread": spread,
            "game_spread_available": available,
        },
    }


def _converter(lg: str, kind: str) -> Callable[..., pl.DataFrame]:
    """``helper_<lg>_<kind>`` from ``sportsdataverse.<lg>.<lg>_<kind>`` (play_by_play / team_box / player_box)."""
    return getattr(importlib.import_module(f"sportsdataverse.{lg}.{lg}_{kind}"), f"helper_{lg}_{kind}")


def _payload_regulation(final: dict, lg: str) -> int:
    """The game's regulation periods: the payload's ``format.regulation.periods``, else the season era."""
    header = final["header"]
    # ESPN's `format` is normally {"regulation": {"periods": n}}, but it is not guaranteed to be an
    # object; anything else falls back to the league's season era
    fmt = final.get("format")
    fmt_reg = fmt.get("regulation") if isinstance(fmt, dict) else None
    return game_regulation_periods(
        lg,
        season=(header.get("season") or {}).get("year"),
        format_regulation_periods=fmt_reg.get("periods") if isinstance(fmt_reg, dict) else None,
    )


def _game_boxes(final: dict, lg: str, comp: dict) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """``(team_box, player_box, rate_team_box, rate_player_box)``; a live game's partial boxes get null rate inputs."""
    live = ((comp.get("status") or {}).get("type") or {}).get("state") == "in"
    if live:
        from sportsdataverse.wbb.wbb_team_box import helper_wbb_team_box

        team_box = helper_wbb_team_box(final, allow_partial=True)
        player_box = _converter(lg, "player_box")(final, allow_partial=True)
    else:
        team_box = _converter(lg, "team_box")(final)
        player_box = _converter(lg, "player_box")(final)
    rate_team_box, rate_player_box = team_box, player_box
    if live:
        # Missing rate inputs propagate as nulls. Keep the original box columns
        # intact and let the normal two-team requirement suppress one-sided rates.
        team_inputs = (
            "field_goals_made",
            "field_goals_attempted",
            "three_point_field_goals_made",
            "free_throws_attempted",
            "offensive_rebounds",
            "defensive_rebounds",
            "total_turnovers" if lg in {"nba", "wnba"} else "turnovers",
        )
        player_inputs = ("field_goals_attempted", "free_throws_attempted", "turnovers", "points")
        if team_box.height == 2:
            rate_team_box = team_box.with_columns(
                pl.lit(None, dtype=pl.Int32).alias(col) for col in team_inputs if col not in team_box.columns
            )
        if player_box.height:
            rate_player_box = player_box.with_columns(
                [pl.lit(None, dtype=pl.Int32).alias(col) for col in player_inputs if col not in player_box.columns]
                + ([pl.lit(None, dtype=pl.Float64).alias("minutes")] if "minutes" not in player_box.columns else [])
            )
    return team_box, player_box, rate_team_box, rate_player_box


def game_page_frames(
    final: dict,
    *,
    league_id: Optional[str] = None,
    league: Optional[str] = None,
    rated_home_prob: Optional[float] = None,
) -> dict[str, object]:
    """Everything the Game on Paper basketball game page needs, from one ``espn_<league>_pbp`` payload.

    Reads the game's regulation (``format.regulation.periods``, else the season cutoff of
    :func:`game_regulation_periods`) and neutral site (``header.competitions[0].neutralSite``),
    runs the league's hoopR / wehoop converters (``helper_<league>_play_by_play`` /
    ``helper_<league>_team_box`` / ``helper_<league>_player_box``) and then
    :func:`game_page_tables`. The sdv-db nightly stage calls :func:`game_page_tables` directly on
    the warehouse's copies of the same frames.

    Args:
        final: The dict ``espn_<league>_pbp(..., return_parsed=False)`` returns (for example
            :func:`~sportsdataverse.nba.nba_pbp.espn_nba_pbp`).
        league_id: Legacy alias: ``"00"`` NBA / ``"10"`` WNBA.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"`` (default NBA).
        rated_home_prob: The game's rated pregame home win probability, the prior when there is no
            line (NBA / WNBA: :func:`~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`).

    Returns:
        ``{"pbp", "team_box", "team_game_rates", "player_box", "player_game_rates",
        "team_game_splits", "player_game_splits", "periods", "prior", "league", "regulation_periods", "period_kind", "neutral_site"}``; see
        :func:`game_page_tables` for the shared keys. Missing boxes retain empty identifier columns.
        Team rates need both team rows, and player rates also need player rows. Unavailable rates
        are empty frames with their documented columns.

    Raises:
        ValueError: If the league is unknown or ``league`` contradicts ``league_id``.
        KeyError: If ``final`` lacks the ``header`` or ``plays`` keys the converters read.
        polars.exceptions.ColumnNotFoundError: If a converter frame lacks a column the derivations need.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_page_frames
            from sportsdataverse.nba.nba_pbp import espn_nba_pbp
            frames = game_page_frames(espn_nba_pbp(401766458, return_parsed=False))
            frames["pbp"].select("text", "home_win_prob", "wp_delta").head()

        A men's college game::

            from sportsdataverse.mbb.mbb_pbp import espn_mbb_pbp
            frames = game_page_frames(espn_mbb_pbp(401830342, return_parsed=False), league="mbb")
            frames["regulation_periods"], frames["period_kind"], frames["periods"]  # (2, 'half', 4)

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, league_id)
    header = final["header"]
    comp = (header.get("competitions") or [{}])[0]
    reg = _payload_regulation(final, lg)
    neutral = _is_neutral(comp.get("neutralSite"))
    pbp = _converter(lg, "play_by_play")(final)  # same converter order as the shipped NBA function
    team_box, player_box, rate_team_box, rate_player_box = _game_boxes(final, lg, comp)
    tables = game_page_tables(
        pbp,
        rate_team_box,
        rate_player_box,
        league=lg,
        regulation_periods=reg,
        neutral_site=neutral,
        rated_home_prob=rated_home_prob,
    )
    return {
        "pbp": tables["pbp"],
        # The release converters return zero columns for unavailable boxes. Keep the
        # lookup keys selectable so live consumers can still serve play-by-play.
        "team_box": team_box if team_box.width else pl.DataFrame(schema={"team_id": pl.Int32}),
        "team_game_rates": tables["team_game_rates"],
        "player_box": player_box
        if player_box.width
        else pl.DataFrame(schema={"team_id": pl.Int32, "athlete_id": pl.Int32}),
        "player_game_rates": tables["player_game_rates"],
        "team_game_splits": tables["team_game_splits"],
        "player_game_splits": tables["player_game_splits"],
        "periods": tables["periods"],
        "prior": tables["prior"],
        "league": lg,
        "regulation_periods": reg,
        "period_kind": tables["period_kind"],
        "neutral_site": neutral,
    }


def league_pbp_functions(league: str) -> tuple[Callable[..., dict], Callable[[Any, dict], dict]]:
    """The ESPN fetch and parse pair for one league: ``(espn_<league>_pbp, helper_<league>_pbp)``.

    A live processor fetches once (``fetch(game_id, raw=True)``, the summary with its keys
    defaulted and nothing parsed), checks the payload has plays, then parses (``parse(game_id,
    summary)``) and hands the result to :func:`game_page_frames`. Splitting the two keeps an
    upstream failure (fetch) apart from a parsing bug (parse).

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.

    Returns:
        ``(fetch, parse)``.

    Raises:
        ValueError: If ``league`` is not one of the four.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_page import game_page_frames, league_pbp_functions
            fetch, parse = league_pbp_functions("wnba")
            summary = fetch(401726992, raw=True)
            frames = game_page_frames(parse(401726992, summary), league="wnba")

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college play-by-play
            * `wehoop`_ -- the R companion for WNBA and women's college play-by-play

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lg = _resolve_league(league, None)
    mod = importlib.import_module(f"sportsdataverse.{lg}.{lg}_pbp")
    return getattr(mod, f"espn_{lg}_pbp"), getattr(mod, f"helper_{lg}_pbp")
