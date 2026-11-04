# tools/fit_basketball_gei_scale.py
"""Fit the per-league game excitement index (GEI) scale constant.

Football's GEI (game-on-paper ``calculateGEI`` / ``python/excitement.py``) is
``179.01777401608126 / n_plays * sum(|WP swing|)``. Upstream does not document the
constant; it reads as a fractional mean of integer play counts, so ``scale / n``
rescales a game's total WP movement to a game of average length. A basketball WP chart has several hundred points a game, so the
constant is refit per league the same way: the mean number of WP points (rows of
the ``score_win_prob`` series, which is every row of the league's pbp release)
over the league's completed games.

Data: the sportsdataverse-data ``espn_<league>_pbp`` releases (``load_nba_pbp``,
``load_wnba_pbp``, ``load_mbb_pbp``, ``load_wbb_pbp``), seasons :data:`SEASONS`,
regular season and postseason. A game counts when its pbp reaches the end of the
game (``nba_game_page._game_end``: an "End Game" / "End Period" row from the last
regulation period on with the score not tied, the rule ``score_win_prob`` settles
WP on), so a truncated pbp never enters the mean. The printed values are ``GEI_SCALE`` in
``sportsdataverse/nba/nba_game_page.py``; paste them verbatim.

Usage::

    uv run python tools/fit_basketball_gei_scale.py
"""

from __future__ import annotations

import importlib

import polars as pl

from sportsdataverse.nba.nba_game_page import _game_end, game_regulation_periods

#: The three most recent complete seasons of each league (as of 2026-11).
SEASONS: dict[str, tuple[int, ...]] = {
    "nba": (2024, 2025, 2026),
    "wnba": (2024, 2025, 2026),
    "mbb": (2024, 2025, 2026),
    "wbb": (2024, 2025, 2026),
}


def plays_per_game(pbp: pl.DataFrame, reg: int) -> pl.DataFrame:
    """``(game_id, n)`` for each game whose pbp reaches the end of the game."""
    filled = pbp.with_columns(pl.col("home_score", "away_score").forward_fill().over("game_id").fill_null(0))
    return (
        filled.group_by("game_id")
        .agg(pl.len().alias("n"), _game_end(reg).any().alias("final"))
        .filter(pl.col("final"))
        .select("game_id", "n")
    )


def main() -> dict[str, float]:
    out: dict[str, float] = {}
    for lg, seasons in SEASONS.items():
        load = getattr(importlib.import_module(f"sportsdataverse.{lg}.{lg}_loaders"), f"load_{lg}_pbp")
        counts = []
        for s in seasons:
            cols = ("game_id", "game_play_number", "type_text", "period_number", "home_score", "away_score")
            pbp = load([s]).select(cols).sort("game_id", "game_play_number")
            games = plays_per_game(pbp, game_regulation_periods(lg, season=s))
            dropped = pbp["game_id"].n_unique() - games.height
            print(f"{lg} {s}: {games.height} final games ({dropped} with a truncated pbp dropped)")
            counts.append(games)
        n = pl.concat(counts)["n"]
        out[lg] = float(n.mean())  # type: ignore[arg-type]
        print(f"{lg}: GEI_SCALE = {out[lg]!r}  (games={n.len()}, seasons={seasons}, median n={n.median()})")
    return out


if __name__ == "__main__":
    main()
