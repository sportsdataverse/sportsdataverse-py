"""(4) Expected turnovers -- ball-security expected-vs-actual residual model.

Mirrors :mod:`sportsdataverse.nba.nba_foul_drawing` (closed-form, no trained
artifact) with the sign convention flipped: fewer turnovers than the
play-type mix predicts is *positive* ball-security skill. See the design spec
(``2026-07-07-nba-playtype-impact-design.md`` §3.5).
"""

from __future__ import annotations

from typing import Optional, Union

import pandas as pd
import polars as pl

EXPECTED_TURNOVERS_SCHEMA: dict[str, type[pl.DataType]] = {
    "player_id": pl.Int64,
    "poss": pl.Float64,
    "tov": pl.Float64,
    "expected_tov": pl.Float64,
    "ball_security_skill": pl.Float64,
}


def _empty() -> pl.DataFrame:
    return pl.DataFrame({c: pl.Series([], dtype=d) for c, d in EXPECTED_TURNOVERS_SCHEMA.items()})


def nba_expected_turnovers(
    season: str,
    *,
    league_id: str = "00",
    base: Optional[pl.DataFrame] = None,
    player_mix: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Expected TOV + residual ball-security skill from Synergy play-type mix.

    ``lg_to_rate_t`` = poss-weighted league mean of ``turnover_freq`` for play
    type ``t``; ``expected_tov = Σ_t off_poss_t · lg_to_rate_t``;
    ``ball_security_skill = 100·(expected_tov − tov)/poss`` (fewer turnovers
    than expected ⇒ positive skill -- sign flipped vs. the foul-drawing model).

    Args:
        season: Season string, e.g. ``"2023-24"``.
        league_id: ``"00"`` NBA (default), ``"10"`` WNBA, ``"20"`` G-League.
        base: Injected ``nba_stats_leaguedashplayerstats`` (``Base`` measure)
            frame: ``player_id``, ``tov``, ``poss`` (bypasses the live fetch).
        player_mix: Injected Synergy player-level offensive mix: ``player_id``,
            ``play_type``, ``off_poss``, ``turnover_freq``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per player: ``player_id`` (Int64), ``poss``/``tov``/
        ``expected_tov``/``ball_security_skill`` (Float64). Zero-row frame
        with this schema when the inputs are empty (sparse-coverage leagues
        never raise).

    Example:
        Quick start::

            from sportsdataverse.nba import nba_expected_turnovers
            t = nba_expected_turnovers("2023-24")
            print(t.sort("ball_security_skill", descending=True).head(10))

        Injected offline (oracle / test) path::

            t = nba_expected_turnovers("2023-24", base=base_df, player_mix=mix_df)

        Pipeline next step::

            t.filter(pl.col("poss") >= 200).sort("ball_security_skill", descending=True)

        See Also:
            * `nba_api`_ -- upstream ``leaguedashplayerstats``/``synergyplaytypes`` source

        .. _nba_api: https://github.com/swar/nba_api
    """
    if base is None or player_mix is None:
        from sportsdataverse.nba.nba_playtype import _fetch_synergy_player
        from sportsdataverse.nba.nba_stats import nba_stats_leaguedashplayerstats

        if base is None:
            base = nba_stats_leaguedashplayerstats(
                league_id=league_id, season=season, measure_type_detailed_defense="Base"
            )
        if player_mix is None:
            player_mix = _fetch_synergy_player(league_id, season, "Offensive")

    if base is None or base.is_empty() or player_mix is None or player_mix.is_empty():
        return _empty()

    mix = player_mix.select(
        pl.col("player_id").cast(pl.Int64),
        pl.col("play_type"),
        pl.col("off_poss").cast(pl.Float64),
        pl.col("turnover_freq").cast(pl.Float64),
    )
    lg_rate = mix.group_by("play_type").agg(
        ((pl.col("turnover_freq") * pl.col("off_poss")).sum() / pl.col("off_poss").sum()).alias("lg_to_rate")
    )
    expected = (
        mix.join(lg_rate, on="play_type", how="left")
        .with_columns((pl.col("off_poss") * pl.col("lg_to_rate")).alias("component"))
        .group_by("player_id")
        .agg(pl.col("component").sum().alias("expected_tov"))
    )

    b = base.select(pl.col("player_id").cast(pl.Int64), pl.col("tov").cast(pl.Float64), pl.col("poss").cast(pl.Float64))
    assert b.schema["player_id"] == expected.schema["player_id"]  # join-key dtype guard

    out = (
        b.join(expected, on="player_id", how="left")
        .with_columns(pl.col("expected_tov").fill_null(0.0))
        .with_columns((100.0 * (pl.col("expected_tov") - pl.col("tov")) / pl.col("poss")).alias("ball_security_skill"))
        .select(list(EXPECTED_TURNOVERS_SCHEMA.keys()))
        .sort("player_id")
    )
    return out.to_pandas() if return_as_pandas else out
