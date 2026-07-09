"""(3) Foul-drawing / FT-generation -- expected-vs-actual residual model.

Closed-form (no trained artifact): computes an **expected** FTA count from a
player's Synergy play-type mix times the league's poss-weighted FTA rate for
that type, and reports observed minus expected as the residual foul-drawing
skill on a per-100-possession scale. League rates are computed from the
fetched data (self-normalizing), so calibration holds by construction and the
oracle gate checks that the code preserves it -- see the design spec
(``2026-07-07-nba-playtype-impact-design.md`` §3.5).
"""

from __future__ import annotations

from typing import Optional, Union

import pandas as pd
import polars as pl

FOUL_DRAWING_SCHEMA: dict[str, type[pl.DataType]] = {
    "player_id": pl.Int64,
    "poss": pl.Float64,
    "fta": pl.Float64,
    "expected_fta": pl.Float64,
    "foul_draw_skill": pl.Float64,
    "pfd": pl.Float64,
}


def _empty() -> pl.DataFrame:
    return pl.DataFrame({c: pl.Series([], dtype=d) for c, d in FOUL_DRAWING_SCHEMA.items()})


def nba_foul_drawing(
    season: str,
    *,
    league_id: str = "00",
    base: Optional[pl.DataFrame] = None,
    advanced: Optional[pl.DataFrame] = None,
    player_mix: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Expected FTA + residual foul-drawing skill from Synergy play-type mix.

    ``lg_ft_rate_t`` = poss-weighted league mean of ``ft_freq`` for play type
    ``t`` (``Σ_players(ft_freq_t·poss_t) / Σ_players(poss_t)``).
    ``expected_fta = Σ_t poss_t · lg_ft_rate_t``;
    ``foul_draw_skill = 100·(fta − expected_fta)/poss``.

    Args:
        season: Season string, e.g. ``"2023-24"``.
        league_id: ``"00"`` NBA (default), ``"10"`` WNBA, ``"20"`` G-League.
        base: Injected ``nba_stats_leaguedashplayerstats`` (``Base`` measure)
            frame: ``player_id``, ``fta``, ``poss`` (bypasses the live fetch).
        advanced: Injected ``Advanced``-measure frame with ``player_id``,
            ``pfd`` (personal fouls drawn); optional -- ``pfd`` is ``null``
            when omitted (``fta`` is the always-present proxy).
        player_mix: Injected Synergy player-level offensive mix: ``player_id``,
            ``play_type``, ``poss``, ``ft_freq``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per player: ``player_id`` (Int64), ``poss``/``fta``/
        ``expected_fta``/``foul_draw_skill`` (Float64), ``pfd`` (Float64,
        null when *advanced* has no data for that player or is omitted).
        Zero-row frame with this schema when the inputs are empty
        (sparse-coverage leagues never raise).

    Example:
        Quick start::

            from sportsdataverse.nba import nba_foul_drawing
            f = nba_foul_drawing("2023-24")
            print(f.sort("foul_draw_skill", descending=True).head(10))

        Injected offline (oracle / test) path::

            f = nba_foul_drawing("2023-24", base=base_df, player_mix=mix_df)

        Pipeline next step::

            f.filter(pl.col("poss") >= 200).sort("foul_draw_skill", descending=True)

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
        pl.col("poss").cast(pl.Float64),
        pl.col("ft_freq").cast(pl.Float64),
    )
    lg_rate = mix.group_by("play_type").agg(
        ((pl.col("ft_freq") * pl.col("poss")).sum() / pl.col("poss").sum()).alias("lg_ft_rate")
    )
    expected = (
        mix.join(lg_rate, on="play_type", how="left")
        .with_columns((pl.col("poss") * pl.col("lg_ft_rate")).alias("component"))
        .group_by("player_id")
        .agg(pl.col("component").sum().alias("expected_fta"))
    )

    b = base.select(pl.col("player_id").cast(pl.Int64), pl.col("fta").cast(pl.Float64), pl.col("poss").cast(pl.Float64))
    assert b.schema["player_id"] == expected.schema["player_id"]  # join-key dtype guard

    out = b.join(expected, on="player_id", how="left").with_columns(
        pl.col("expected_fta").fill_null(0.0),
        (100.0 * (pl.col("fta") - pl.col("expected_fta").fill_null(0.0)) / pl.col("poss")).alias("foul_draw_skill"),
    )

    if advanced is not None and not advanced.is_empty() and "pfd" in advanced.columns:
        adv = advanced.select(pl.col("player_id").cast(pl.Int64), pl.col("pfd").cast(pl.Float64))
        assert out.schema["player_id"] == adv.schema["player_id"]  # join-key dtype guard
        out = out.join(adv, on="player_id", how="left")
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias("pfd"))

    out = out.select(list(FOUL_DRAWING_SCHEMA.keys())).sort("player_id")
    return out.to_pandas() if return_as_pandas else out
