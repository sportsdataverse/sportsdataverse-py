"""Opponent-adjusted EPA for NFL play-by-play, laid out game by game.

:func:`nfl_adjusted_epa_ledger` is :func:`sportsdataverse.cfb.cfb_adjusted_epa_ledger` on an NFL frame: the same
plays rule, fit, penalty and per-game decomposition, with the NFL column names mapped onto the ones that function
reads. Which engine, and why: on a walk-forward backtest of NFL regular seasons 2014-2023 (ratings through week
w-1 predicting week w game margins, weeks 4 on, both teams with three or more prior games; the margin model's two
coefficients fit leave-one-season-out) this fit beat the one nfl-data's team summaries used until now (the
``0.1 <= wp <= 0.9`` band, ``cfb_adjusted_epa(method="pre598")``) by 0.088 points of MAE, 95% CI [0.015, 0.170]
over (season, week) clusters, with no season worse by more than 0.15 points, and by 0.127 on the 2024-2025
holdout. NFL-specific margin thresholds (28 / 24 / 21 / 17) and ``nfl_ratings.efficiency_ratings`` were scored on
the same games and did not clear the gate, so the plays kept are the college rule's: not garbage time by the
score margin at the snap (above 43 / 37 / 27 / 21 in Q1 / Q2 / Q3 / Q4 and overtime). The penalty is the college
default (``0.075`` per play of a 577-play team season) and was not re-tuned for the NFL.

Two input shapes are accepted, told apart by their columns:

* **nflverse** (``load_nfl_pbp``, ``nfl_model_pbp``): ``posteam`` present. Scrimmage plays are
  ``play_type`` ``"pass"`` or ``"run"`` with a ``down`` (kneels, spikes, penalties with no play, kicks and
  conversions are not), and a team is its ``posteam`` abbreviation.
* **ESPN processor** (``NFLPlayProcess`` output): the offense is ``pos_team_id`` in released frames and
  ``pos_team`` (the numeric team id) in a live run; the defense likewise ``def_pos_team_id`` / ``def_pos_team``.
  Plays are its ``pass`` / ``rush`` flags with kneel-downs dropped, and a team is its ESPN id as a string.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse.cfb.cfb_adjusted_epa import cfb_adjusted_epa_ledger

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nfl_adjusted_epa_ledger"]

_NFLVERSE_REQUIRED = (
    "game_id",
    "posteam",
    "defteam",
    "home_team",
    "epa",
    "qtr",
    "score_differential",
    "play_type",
    "down",
)
_ESPN_REQUIRED = ("game_id", "homeTeamId", "EPA", "pass", "rush", "period", "pos_score_diff_start", "neutral_site")


def _require(df: pl.DataFrame, cols: tuple[str, ...], shape: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"nfl_adjusted_epa_ledger: {shape} plays are missing required columns {missing}")


def _id(col: str) -> pl.Expr:
    """A team id as a string from its integer value (never a float, which would give ``"12.0"``)."""
    return pl.col(col).cast(pl.Int64).cast(pl.Utf8)


def _nfl_plays(plays: pl.DataFrame | pd.DataFrame) -> pl.DataFrame:
    """Map an nflverse or ESPN-processor NFL frame onto the columns ``cfb_adjusted_epa_ledger`` reads."""
    df = plays if isinstance(plays, pl.DataFrame) else pl.from_pandas(plays)
    if "posteam" in df.columns:
        _require(df, _NFLVERSE_REQUIRED, "nflverse")
        if "neutral_site" in df.columns:
            neutral = pl.col("neutral_site").cast(pl.Boolean)
        elif "location" in df.columns:
            neutral = pl.col("location") == "Neutral"
        else:
            raise KeyError("nfl_adjusted_epa_ledger: nflverse plays need neutral_site or location (schedule)")
        kept = df.filter(
            pl.col("play_type").is_in(["pass", "run"])
            & pl.col("down").is_not_null()
            & pl.col("posteam").is_not_null()
            & pl.col("epa").is_not_null()
        )
        out = kept.with_columns(
            game_id=pl.col("game_id").cast(pl.Utf8),
            pos_team=pl.col("posteam"),
            pos_team_id=pl.col("posteam"),
            def_pos_team_id=pl.col("defteam"),
            home=pl.col("home_team"),
            neutral_site=neutral.fill_null(False),
            EPA=pl.col("epa").cast(pl.Float64),
            period=pl.col("qtr"),
            pos_score_diff_start=pl.col("score_differential"),
            **{
                "pass": (pl.col("play_type") == "pass").cast(pl.Int8),
                "rush": (pl.col("play_type") == "run").cast(pl.Int8),
            },
        )
        # season_type ("REG" / "POST") is carried into the ledger as season_type
        return out.rename({"season_type": "seasonType"}) if "season_type" in out.columns else out
    off = "pos_team_id" if "pos_team_id" in df.columns else "pos_team"
    dfn = "def_pos_team_id" if "def_pos_team_id" in df.columns else "def_pos_team"
    _require(df, (off, dfn, *_ESPN_REQUIRED), "ESPN-processor")
    if "kneel_down" in df.columns:
        df = df.filter(pl.col("kneel_down").fill_null(False) == False)  # noqa: E712
    return df.with_columns(
        game_id=pl.col("game_id").cast(pl.Utf8),
        pos_team=_id(off),
        pos_team_id=_id(off),
        def_pos_team_id=_id(dfn),
        home=_id("homeTeamId"),
        **{
            "pass": pl.col("pass").fill_null(False).cast(pl.Int8),
            "rush": pl.col("rush").fill_null(False).cast(pl.Int8),
        },
    )


@overload
def nfl_adjusted_epa_ledger(
    plays: pl.DataFrame | pd.DataFrame,
    *,
    ridge_lambda: float | None = ...,
    influence: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def nfl_adjusted_epa_ledger(
    plays: pl.DataFrame | pd.DataFrame,
    *,
    ridge_lambda: float | None = ...,
    influence: bool = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nfl_adjusted_epa_ledger(
    plays: pl.DataFrame | pd.DataFrame,
    *,
    ridge_lambda: float | None = None,
    influence: bool = True,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """A team's season opponent-adjusted net EPA per play, game by game, for the NFL.

    Each team's season ``net_adj_epa`` is a mean over its games of adjusted offensive EPA minus a mean of
    adjusted defensive EPA, and each game's adjusted value is its raw EPA plus a schedule term (the opponent's
    fitted strengths). The frame lays that sum out per team-game: raw net EPA, the schedule term, the game's
    weight and its contribution, which sum to the season value. With ``influence`` it refits the season once per
    game without that game and reports how far the game moved each of its two teams. The fit, plays and columns
    are :func:`sportsdataverse.cfb.cfb_adjusted_epa_ledger`'s; the module docstring gives the backtest behind
    the choice and the two input shapes.

    Args:
        plays: One NFL season of play-by-play, polars or pandas, in either shape the module docstring lists, with
            only the season types to rate (nfl-data's team summaries rate the regular season alone). nflverse
            frames need ``game_id``, ``posteam``, ``defteam``, ``home_team``, ``epa``, ``qtr``,
            ``score_differential``, ``play_type``, ``down`` and ``neutral_site`` or ``location`` (the schedule's
            ``"Neutral"``); ``load_nfl_pbp`` carries all of them. ESPN-processor frames need ``game_id``, the
            offense and defense ids, ``homeTeamId``, ``EPA``, ``pass``, ``rush``, ``period``,
            ``pos_score_diff_start`` and ``neutral_site``. ``season``, ``week`` and ``season_type`` (ESPN
            ``seasonType``) are carried through when present.
        ridge_lambda: Ridge penalty per play of a 577-play team season (``None``: 0.075, as
            :func:`sportsdataverse.cfb.cfb_adjusted_epa`); must be > 0.
        influence: Add ``loo_net_adj_epa`` and ``game_influence`` (one refit per game; a few seconds a season).
        return_as_pandas: Return a pandas ``DataFrame`` instead of polars.

    Returns:
        One row per (game, team) for every team with two or more valid games, sorted by ``team_id``, ``week``,
        ``game_id``: the columns of :func:`sportsdataverse.cfb.cfb_adjusted_epa_ledger`. ``team_id``,
        ``pos_team`` and ``opponent_id`` are the nflverse abbreviation, or the ESPN team id as a string.

    Raises:
        KeyError: If ``plays`` is missing a required column for its shape.
        ValueError: If ``ridge_lambda`` is not positive or every play is garbage time.

    Example:
        Quick start::

            import polars as pl
            import sportsdataverse.nfl as nfl
            pbp = nfl.load_nfl_pbp(seasons=[2024]).filter(pl.col("season_type") == "REG")
            ledger = nfl.nfl_adjusted_epa_ledger(pbp)
            ledger.filter(pl.col("team_id") == "KC").select("week", "opponent_id", "raw_net_epa", "schedule_adj")

        The games that moved a team's season value the most::

            ledger.sort(pl.col("game_influence").abs(), descending=True).head()

    See Also:
        * `nflfastR`_ -- the per-play EPA this fit adjusts.

    .. _nflfastR: https://www.nflfastr.com
    """
    out = cfb_adjusted_epa_ledger(_nfl_plays(plays), ridge_lambda=ridge_lambda, influence=influence)
    return out.to_pandas() if return_as_pandas else out
