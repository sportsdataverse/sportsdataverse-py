"""Football game story: how the winner won it, in expected points (college; the NFL binds it too).

The football adapter of :mod:`sportsdataverse._common.game_story`. It reads the ESPN-processed play
frame (:class:`~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess` / ``NFLPlayProcess`` output, the
``espn_cfb_pbp`` / ``espn_nfl_pbp`` releases), whose ``EPA`` is the offense's expected points added
and ``wpa`` its win-probability change. :mod:`sportsdataverse.nfl.nfl_game_story` binds the NFL.

Each side's value for an EPA edge is the sum of its offense's ``EPA`` over the edge's plays, so a
defense's work shows as the other offense's lower EPA, and ``points_worth`` is the winner's less the
loser's.

* **Accounting** (every play with an EPA, once): ``passing`` (``pass`` scrimmage plays), ``rushing``
  (``rush`` scrimmage plays), ``special_teams`` (kickoffs, punts and field goals) and ``other``
  (penalties without a play, kneels, conversions). Their sum is the net EPA, which telescopes to the
  margin less the expected points that halves start and end with: close to it, not equal.
* **Splits**: ``turnovers`` (plays the offense turned over, ``is_pos_team_turnover``),
  ``explosive_plays`` (``EPA_explosive``), ``red_zone`` (scrimmage plays from 20 yards out or closer),
  ``late_downs`` (``late_down``: third and fourth down) and ``success_rate``. The success rate's value is
  each offense's rate on scrimmage plays (``EPA_success``); its points are
  ``(rate - game rate) * plays * (e_s - e_f)``, the successes above the game's pooled rate priced at the
  game's own mean EPA of a successful play less an unsuccessful one.

The moment is the play with the largest ``|wpa|`` (turned to the home side). The top performer is
the player with the most EPA as passer, rusher or receiver combined (a target credits both;
ties: touchdowns).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_story import game_story

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["cfb_game_story"]

_REGULATION = 4
_SPECIAL = r"(?i)kickoff|punt|field goal"
_SCRIM = pl.col("scrimmage_play").fill_null(False) == True
#: accounting edges -> the plays each covers (together, every play with an EPA once)
_ACCOUNTING: dict[str, pl.Expr] = {
    "passing": _SCRIM & (pl.col("pass").fill_null(False) == True),
    "rushing": _SCRIM & (pl.col("rush").fill_null(False) == True) & (pl.col("pass").fill_null(False) == False),
}
#: split edges -> their plays
_SPLITS: dict[str, pl.Expr] = {
    "turnovers": pl.col("is_pos_team_turnover").fill_null(False) == True,
    "explosive_plays": pl.col("EPA_explosive").fill_null(False) == True,
    "red_zone": _SCRIM & (pl.col("start.yardsToEndzone") <= 20).fill_null(False),
    "late_downs": _SCRIM & (pl.col("late_down").fill_null(False) == True),
}
_EDGE_ORDER = (
    "passing",
    "rushing",
    "special_teams",
    "other",
    "turnovers",
    "explosive_plays",
    "success_rate",
    "red_zone",
    "late_downs",
)


def _plays(pbp: pl.DataFrame) -> pl.DataFrame:
    """The plays with an ``_phase`` (the accounting edge each belongs to) and the ids cast."""
    # the processor's own frame names the offense ``pos_team``; the releases rename it ``pos_team_id``
    if "pos_team_id" not in pbp.columns and "pos_team" in pbp.columns:
        pbp = pbp.rename({"pos_team": "pos_team_id"})
    phase = (
        pl.when(_ACCOUNTING["passing"])
        .then(pl.lit("passing"))
        .when(_ACCOUNTING["rushing"])
        .then(pl.lit("rushing"))
        .when(~_SCRIM & pl.col("type.text").cast(pl.Utf8).str.contains(_SPECIAL).fill_null(False))
        .then(pl.lit("special_teams"))
        .otherwise(pl.lit("other"))
    )
    return pbp.with_columns(
        pl.col("game_id").cast(pl.Int64),
        pl.col("pos_team_id").cast(pl.Int64),
        pl.col("homeTeamId").cast(pl.Int64),
        pl.col("awayTeamId").cast(pl.Int64),
        pl.col("EPA").cast(pl.Float64),
        phase.alias("_phase"),
    )


def _edges(p: pl.DataFrame) -> pl.DataFrame:
    """:data:`~sportsdataverse._common.game_story.EDGE_ROWS_SCHEMA` rows for every game and offense."""
    off = p.filter(pl.col("pos_team_id").is_not_null() & pl.col("EPA").is_not_null() & pl.col("EPA").is_not_nan())
    by = ["game_id", pl.col("pos_team_id").alias("team_id")]
    phases = (
        off.group_by(*by, "_phase")
        .agg(pl.col("EPA").sum().alias("value"))
        .select("game_id", "team_id", pl.col("_phase").alias("edge"), "value")
    )
    splits = [
        off.filter(mask)
        .group_by(*by)
        .agg(pl.col("EPA").sum().alias("value"))
        .select("game_id", "team_id", pl.lit(name).alias("edge"), "value")
        for name, mask in _SPLITS.items()
    ]
    # every side gets every EPA edge, 0 when it had none of those plays
    teams = off.select("game_id", pl.col("pos_team_id").alias("team_id")).unique()
    grid = teams.join(pl.DataFrame({"edge": [e for e in _EDGE_ORDER if e != "success_rate"]}), how="cross")
    epa = grid.join(pl.concat([phases, *splits], how="vertical_relaxed"), on=["game_id", "team_id", "edge"], how="left")
    epa = epa.with_columns(pl.col("value").fill_null(0.0), pl.col("value").fill_null(0.0).alias("points"))
    scrim = off.filter(_SCRIM).with_columns((pl.col("EPA_success").fill_null(False) == True).alias("_s"))
    price = scrim.group_by("game_id").agg(
        pl.col("_s").mean().alias("_g_rate"),
        (pl.col("EPA").filter(pl.col("_s")).mean() - pl.col("EPA").filter(pl.col("_s") == False).mean()).alias(
            "_e_gap"
        ),
    )
    sr = (
        scrim.group_by(*by)
        .agg(pl.col("_s").mean().alias("value"), pl.len().alias("_n"))
        .join(price, on="game_id", how="left")
        .with_columns(
            ((pl.col("value") - pl.col("_g_rate")) * pl.col("_n") * pl.col("_e_gap")).alias("points"),
            pl.lit("success_rate").alias("edge"),
        )
    )
    cols = ["game_id", "team_id", "edge", "value", "points"]
    order = {e: i for i, e in enumerate(_EDGE_ORDER)}
    return pl.concat([epa.select(cols), sr.select(cols)], how="vertical_relaxed").with_columns(
        pl.col("edge").replace_strict(order, return_dtype=pl.Int64).alias("edge_order"),
        pl.when(pl.col("edge").is_in(["passing", "rushing", "special_teams", "other"]))
        .then(pl.lit("accounting"))
        .otherwise(pl.lit("split"))
        .alias("kind"),
        pl.lit("points").alias("unit"),
    )


def _players(p: pl.DataFrame) -> pl.DataFrame:
    """Each player's EPA as passer, rusher or receiver, and touchdowns (the tiebreak)."""
    roles = []
    for role, td in (("passer", "pass_td"), ("rusher", "rush_td"), ("receiver", "pass_td")):
        if f"{role}_player_id" not in p.columns:
            continue
        roles.append(
            p.filter(pl.col(f"{role}_player_id").is_not_null() & pl.col("EPA").is_not_null()).select(
                "game_id",
                pl.col("pos_team_id").alias("team_id"),
                pl.col(f"{role}_player_id").cast(pl.Int64).alias("player_id"),
                pl.col(f"{role}_player_name").cast(pl.Utf8).alias("player_name"),
                "EPA",
                (pl.col(td).fill_null(False) == True).cast(pl.Float64).alias("_td")
                if td in p.columns
                else pl.lit(0.0).alias("_td"),
            )
        )
    if not roles:
        return pl.DataFrame(
            schema={
                "game_id": pl.Int64,
                "team_id": pl.Int64,
                "player_id": pl.Int64,
                "player_name": pl.Utf8,
                "metric": pl.Utf8,
                "value": pl.Float64,
                "tiebreak": pl.Float64,
            }
        )
    return (
        pl.concat(roles)
        .group_by("game_id", "team_id", "player_id")
        .agg(
            pl.col("player_name").drop_nulls().first(),
            pl.col("EPA").sum().alias("value"),
            pl.col("_td").sum().alias("tiebreak"),
        )
        .with_columns(pl.lit("epa").alias("metric"))
    )


def _football_game_story(pbp: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """:func:`cfb_game_story` without the pandas option: the frames for the ESPN-processed play frame ``pbp``."""
    p = _plays(pbp)
    last = (
        p.sort("game_id", "game_play_number")
        .group_by("game_id", maintain_order=True)
        .agg(
            pl.col("homeTeamId", "awayTeamId").drop_nulls().first(),
            pl.col("homeScore").drop_nulls().last().cast(pl.Int64).alias("_h"),
            pl.col("awayScore").drop_nulls().last().cast(pl.Int64).alias("_a"),
            *(
                [
                    pl.col("homeFinalScore").drop_nulls().first().cast(pl.Int64).alias("_hf"),
                    pl.col("awayFinalScore").drop_nulls().first().cast(pl.Int64).alias("_af"),
                ]
                if "homeFinalScore" in p.columns and "awayFinalScore" in p.columns
                else [pl.lit(None, dtype=pl.Int64).alias("_hf"), pl.lit(None, dtype=pl.Int64).alias("_af")]
            ),
        )
    )
    games = last.select(
        "game_id",
        pl.col("homeTeamId").alias("home_team_id"),
        pl.col("awayTeamId").alias("away_team_id"),
        pl.coalesce("_hf", "_h").alias("home_score"),  # the release's final, else the last running score
        pl.coalesce("_af", "_a").alias("away_score"),
        pl.lit(_REGULATION, dtype=pl.Int64).alias("regulation_periods"),
    )
    home = pl.col("pos_team_id") == pl.col("homeTeamId")
    plays = p.select(
        "game_id",
        pl.col("id").cast(pl.Int64).alias("play_id"),
        pl.col("game_play_number").cast(pl.Int64).alias("order"),
        pl.col("period.number").cast(pl.Int64).alias("period"),
        pl.col("clock.displayValue").cast(pl.Utf8).alias("clock"),
        pl.col("pos_team_id").alias("team_id"),
        pl.col("text").cast(pl.Utf8),
        pl.col("homeScore").cast(pl.Int64).alias("home_score"),
        pl.col("awayScore").cast(pl.Int64).alias("away_score"),
        pl.when(pl.col("pos_team_id").is_null())
        .then(None)
        .when(home)
        .then(pl.col("wpa"))
        .otherwise(-pl.col("wpa"))
        .cast(pl.Float64)
        .alias("wpa"),
    )
    return game_story(games, _edges(p), plays, _players(p))


def cfb_game_story(
    pbp: pl.DataFrame, *, return_as_pandas: bool = False
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame]:
    """How the winner won it, for one or many college football games: edges in EPA, the moment, periods, runs, performers.

    Args:
        pbp: The ESPN-processed plays of one or many games: ``load_cfb_pbp`` (the ``espn_cfb_pbp``
            release) or ``CFBPlayProcess(...).run_processing_pipeline()``'s plays frame. Needs ``game_id``,
            ``id``, ``game_play_number``, ``period.number``, ``clock.displayValue``, ``text``,
            ``pos_team_id`` (the processor's frame calls it ``pos_team``), ``homeTeamId``, ``awayTeamId``, ``homeScore``, ``awayScore``, ``EPA``, ``wpa``,
            ``scrimmage_play``, ``pass``, ``rush``, ``type.text``, ``is_pos_team_turnover``,
            ``EPA_explosive``, ``EPA_success``, ``start.yardsToEndzone``, ``late_down`` and the
            ``passer`` / ``rusher`` / ``receiver`` ``_player_id`` / ``_player_name`` columns.
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"summary", "edges", "moment", "periods", "runs", "performers"}``, the frames of
        :data:`~sportsdataverse._common.game_story.GAME_STORY_SCHEMAS`. Edges are the accounting
        phases (``passing``, ``rushing``, ``special_teams``, ``other``) and the splits
        (``turnovers``, ``explosive_plays``, ``success_rate``, ``red_zone``, ``late_downs``), each in
        expected points and ranked by ``points_worth``. The final score is the release's
        ``homeFinalScore`` / ``awayFinalScore`` where present, else the last running score.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``pbp`` lacks a column named above.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb import load_cfb_pbp
            from sportsdataverse.cfb.cfb_game_story import cfb_game_story
            pbp = load_cfb_pbp([2025]).filter(pl.col("game_id") == 401752873)
            story = cfb_game_story(pbp)
            story["edges"].head(3)

        The deciding play of each game in a week::

            week = load_cfb_pbp([2025]).filter(pl.col("week") == 5)
            cfb_game_story(week)["moment"].select("game_id", "text", "winner_wpa")

        See Also:
            * `cfbfastR`_ -- the R companion for college football play-by-play

        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    story = _football_game_story(pbp)
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in story.items()}
    return story
