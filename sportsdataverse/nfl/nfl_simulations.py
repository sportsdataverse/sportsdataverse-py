"""NFL season simulation engine (nflseedR v2 simulations port).

Adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe),
https://github.com/nflverse/nflseedR — R sources ``simulations.R``,
``simulations_simulate_chunks.R``, ``simulations_utils.R`` (v2.0.2).

nflseedR parallelizes via furrr chunks; this port runs one vectorized pass
over all simulated seasons (the ``chunks`` argument is intentionally
dropped) and threads an explicit numpy RNG for reproducibility.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Mapping, Optional, Tuple, Union

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_season_standings import (
    _EXIT_TO_INT,
    _INT_TO_EXIT,
    _add_draft_ranks,
    _standings_core,
    _teams_frame,
    _with_frank,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = ["nfl_simulations", "nfl_compute_results"]

_PLAYOFF_WEEKS: Tuple[str, ...] = ("WC", "DIV", "CON", "SB")
_PLAYOFF_SUMMANDS: Dict[str, int] = {w: i + 1 for i, w in enumerate(_PLAYOFF_WEEKS)}
_SIM_DEPTH_MAP: Dict[str, int] = {"RANDOM": 0, "PRE-SOV": 1, "SOS": 2}
_SIM_INCLUDE_MAP: Dict[str, int] = {"REG": 0, "POST": 1, "DRAFT": 2}

_REQUIRED_VARS: Tuple[str, ...] = (
    "game_type",
    "week",
    "away_team",
    "home_team",
    "away_rest",
    "home_rest",
    "location",
    "result",
)

ComputeResultsFn = Callable[..., Dict[str, pl.DataFrame]]


def _round_out(x: pl.Expr) -> pl.Expr:
    """Round away from zero (``round_out`` in nflseedR_compute_results)."""
    return (x.abs().ceil() * x.sign()).cast(pl.Int64)


def nfl_compute_results(
    teams: pl.DataFrame,
    games: pl.DataFrame,
    week_num: Union[str, int],
    *,
    rng: Optional[np.random.Generator] = None,
    elo: Optional[Mapping[str, float]] = None,
    **kwargs: Any,
) -> Dict[str, pl.DataFrame]:
    """Compute NFL game results for one week of a season simulation.

    Faithful port of ``nflseedR_compute_results`` (simulations_utils.R
    L183-290) — the 538-style dynamic ELO model initially coded by Lee
    Sharpe and rewritten by Sebastian Carl: home/away ELO difference plus
    rest (+25 per extra week), home field (+20), and a 1.2x postseason
    multiplier produce a win probability and a point spread ``estimate``
    (``elo_diff / 25``); missing results for ``week_num`` are drawn from
    ``Normal(estimate, 13)`` and rounded away from zero. ELO ratings are
    updated from all of the week's results and carried to the next week
    via the returned ``teams`` frame.

    Args:
        teams: Teams frame with ``sim`` and ``team`` columns. An ``elo``
            column is added on first call (from ``elo`` or random
            ``Normal(1500, 150)`` initial ratings shared across sims) and
            must be carried between calls.
        games: Games frame with ``sim``, ``week``, ``game_type``,
            ``location``, ``home_team``/``away_team``, ``home_rest``/
            ``away_rest``, and ``result`` columns.
        week_num: The week to simulate. Only rows with ``week == week_num``
            and a missing ``result`` are filled.
        rng: numpy random generator; a fresh one is created when ``None``.
        elo: Optional mapping of team abbreviation to initial ELO rating.
        **kwargs: Ignored (forward compatibility with custom callers).

    Returns:
        ``{"teams": teams, "games": games}`` with updated ELO ratings and
        filled results.

    Example:
        Simulate one week on a replicated schedule::

            from sportsdataverse.nfl.nfl_simulations import nfl_compute_results
            out = nfl_compute_results(teams, games, week_num="5")
            teams, games = out["teams"], out["games"]

    See Also:
        * `nflseedR`_ -- the canonical R implementation this ports.

    .. _nflseedR: https://nflseedr.com
    """
    if rng is None:
        rng = np.random.default_rng()
    if "elo" not in teams.columns:
        if elo is not None:
            ratings = {str(k): float(v) for k, v in dict(elo).items()}
        else:
            uniq = teams["team"].unique(maintain_order=True).to_list()
            vals = rng.normal(1500.0, 150.0, len(uniq))
            ratings = {t: float(v) for t, v in zip(uniq, vals)}
        teams = teams.with_columns(
            pl.col("team").replace_strict(ratings, default=None, return_dtype=pl.Float64).alias("elo")
        )

    wk = pl.col("week") == week_num
    g = games.join(
        teams.select("sim", pl.col("team").alias("home_team"), pl.col("elo").alias("__home_elo")),
        on=["sim", "home_team"],
        how="left",
    ).join(
        teams.select("sim", pl.col("team").alias("away_team"), pl.col("elo").alias("__away_elo")),
        on=["sim", "away_team"],
        how="left",
    )
    elo_diff = (
        pl.col("__home_elo")
        - pl.col("__away_elo")
        + (pl.col("home_rest") - pl.col("away_rest")) / 7.0 * 25.0
        + pl.when(pl.col("location") == "Home").then(20.0).otherwise(0.0)
    ) * pl.when(pl.col("game_type").is_in(list(_PLAYOFF_WEEKS))).then(1.2).otherwise(1.0)
    g = g.with_columns(pl.when(wk).then(elo_diff).otherwise(None).alias("__elo_diff"))
    g = g.with_columns(
        (1.0 / ((10.0 ** (-pl.col("__elo_diff") / 400.0)) + 1.0)).alias("__wp"),
        (pl.col("__elo_diff") / 25.0).alias("__estimate"),
    )
    g = g.with_columns(pl.Series("__noise", rng.normal(0.0, 13.0, g.height)))
    g = g.with_columns(
        pl.when(wk & pl.col("result").is_null())
        .then(_round_out(pl.col("__estimate") + pl.col("__noise")))
        .otherwise(pl.col("result"))
        .alias("result")
    )
    g = g.with_columns(
        pl.when((wk == False) | pl.col("result").is_null())  # noqa: E712
        .then(None)
        .when(pl.col("result") > 0)
        .then(1.0)
        .when(pl.col("result") < 0)
        .then(0.0)
        .otherwise(0.5)
        .alias("__outcome"),
        pl.when((wk == False) | pl.col("result").is_null())  # noqa: E712
        .then(None)
        .when(pl.col("result") > 0)
        .then(pl.col("__elo_diff") * 0.001 + 2.2)
        .when(pl.col("result") < 0)
        .then(-pl.col("__elo_diff") * 0.001 + 2.2)
        .otherwise(1.0)
        .alias("__elo_input"),
    )
    g = g.with_columns(
        ((pl.col("result").abs().clip(lower_bound=1).cast(pl.Float64) + 1.0).log() * 2.2 / pl.col("__elo_input")).alias(
            "__elo_mult"
        )
    )
    g = g.with_columns((20.0 * pl.col("__elo_mult") * (pl.col("__outcome") - pl.col("__wp"))).alias("__elo_shift"))
    wk_rows = g.filter(wk)
    shifts = (
        pl.concat(
            [
                wk_rows.select(
                    "sim",
                    pl.col("home_team").alias("team"),
                    pl.col("__elo_shift").alias("__shift"),
                ),
                wk_rows.select(
                    "sim",
                    pl.col("away_team").alias("team"),
                    (-pl.col("__elo_shift")).alias("__shift"),
                ),
            ]
        )
        .group_by("sim", "team")
        .agg(pl.col("__shift").sum())
    )
    teams = (
        teams.join(shifts, on=["sim", "team"], how="left")
        .with_columns((pl.col("elo") + pl.col("__shift").fill_null(0.0)).alias("elo"))
        .drop("__shift")
    )
    g = g.drop(
        [
            "__home_elo",
            "__away_elo",
            "__elo_diff",
            "__wp",
            "__estimate",
            "__noise",
            "__outcome",
            "__elo_input",
            "__elo_mult",
            "__elo_shift",
        ]
    )
    return {"teams": teams, "games": g}


def _validate_sim_games(games: pl.DataFrame) -> pl.DataFrame:
    """Port of ``sims_validate_games`` (simulations_utils.R L42-88)."""
    cols = games.columns
    if "sim" not in cols and "season" not in cols:
        raise ValueError(f"games must include 'sim' or 'season' plus all of {list(_REQUIRED_VARS)}")
    missing = [c for c in _REQUIRED_VARS if c not in cols]
    if missing:
        raise ValueError(f"games is missing required columns: {missing}")
    ident = "sim" if "sim" in cols else "season"
    if games[ident].n_unique() > 1:
        raise ValueError(
            f"games contains more than one unique '{ident}' value; nfl_simulations can only handle one season."
        )
    games = games.select(list(_REQUIRED_VARS)).with_columns(
        pl.col("result").cast(pl.Int64),
        pl.col("away_rest").cast(pl.Int64),
        pl.col("home_rest").cast(pl.Int64),
        pl.col("week").cast(pl.Int64).alias("old_week"),
    )
    # week becomes a string key: REG week numbers as strings, else game_type
    games = games.with_columns(
        pl.when(pl.col("game_type") == "REG")
        .then(pl.col("old_week").cast(pl.Utf8))
        .otherwise(pl.col("game_type"))
        .alias("week")
    )
    return games


def _playoff_dummy(byes_per_conf: int) -> pl.DataFrame:
    """Port of ``sims_compute_playoff_dummy`` (simulations_utils.R L90-145)."""
    n_wc = 2**3 - byes_per_conf * 2
    n_games = {"WC": n_wc, "DIV": 4, "CON": 2, "SB": 1}
    game_type: List[str] = []
    conf: List[Optional[str]] = []
    for gt in _PLAYOFF_WEEKS:
        n = n_games[gt]
        game_type.extend([gt] * n)
        if gt == "SB":
            conf.append(None)
        else:
            conf.extend(["AFC"] * (n // 2) + ["NFC"] * (n // 2))
    df = pl.DataFrame(
        {
            "game_type": game_type,
            "week": game_type,
            "conf": conf,
            "away_team": pl.Series([None] * len(game_type), dtype=pl.Utf8),
            "home_team": pl.Series([None] * len(game_type), dtype=pl.Utf8),
            "away_rest": [7] * len(game_type),
            "home_rest": [7] * len(game_type),
            "location": ["Home"] * len(game_type),
            "result": pl.Series([None] * len(game_type), dtype=pl.Int64),
        }
    ).with_columns(pl.col("away_rest").cast(pl.Int64), pl.col("home_rest").cast(pl.Int64))
    wc_home_seeds = list(range(1 + byes_per_conf, 1 + byes_per_conf + n_wc // 2))
    wc_away_seeds = list(reversed([s + n_wc // 2 for s in wc_home_seeds]))
    per_conf = n_wc // 2
    home_ids: List[Optional[str]] = [None] * len(game_type)
    away_ids: List[Optional[str]] = [None] * len(game_type)
    # WC ids: seeds recycle within each conference block
    idx = 0
    for i, gt in enumerate(game_type):
        if gt != "WC":
            continue
        pos = idx % per_conf
        home_ids[i] = f"{conf[i]}-{wc_home_seeds[pos]}"
        away_ids[i] = f"{conf[i]}-{wc_away_seeds[pos]}"
        idx += 1
    df = df.with_columns(
        pl.Series("home_round_id", home_ids, dtype=pl.Utf8),
        pl.Series("away_round_id", away_ids, dtype=pl.Utf8),
    )
    # SB is simulated as a neutral site game with 14 days rest
    df = df.with_columns(
        pl.when(pl.col("game_type") == "SB").then(pl.lit("Neutral")).otherwise(pl.col("location")).alias("location"),
        pl.when(pl.col("game_type") == "SB").then(14).otherwise(pl.col("away_rest")).cast(pl.Int64).alias("away_rest"),
        pl.when(pl.col("game_type") == "SB").then(14).otherwise(pl.col("home_rest")).cast(pl.Int64).alias("home_rest"),
    )
    return df


def nfl_simulations(
    games: pl.DataFrame,
    compute_results: Optional[ComputeResultsFn] = None,
    *,
    simulations: int = 10000,
    playoff_seeds: int = 7,
    byes_per_conf: int = 1,
    tiebreaker_depth: str = "SOS",
    sim_include: str = "DRAFT",
    seed: Optional[int] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Dict[str, Union[pl.DataFrame, "pd.DataFrame"]]:
    """Simulate an NFL season from a schedule with (partially) missing results.

    Faithful port of ``nflseedR::nfl_simulations()`` +
    ``simulate_chunk()`` (simulations.R L140-409,
    simulations_simulate_chunks.R L1-284). Missing regular season results
    are filled week by week via ``compute_results``; standings, division
    ranks and playoff seeds are then computed with the full NFL tiebreakers,
    the postseason is simulated round by round (with reseeding and
    ``byes_per_conf`` byes), and the draft order is derived. nflseedR's
    furrr chunking is replaced by one vectorized pass over all simulated
    seasons, so there is no ``chunks`` argument; reproducibility comes from
    ``seed``.

    Args:
        games: Schedule frame for ONE season with columns ``sim`` or
            ``season``, ``game_type``, ``week``, ``away_team``,
            ``home_team``, ``away_rest``, ``home_rest``, ``location``, and
            ``result`` (home margin; missing = not yet played).
        compute_results: Function filling results for one week, called as
            ``compute_results(teams, games, week_num, rng=rng, **kwargs)``
            and returning ``{"teams": ..., "games": ...}``. Defaults to
            :func:`nfl_compute_results` (dynamic ELO + Normal(estimate, 13)
            margins). Must only fill results where ``week == week_num`` and
            ``result`` is missing, and must not produce postseason ties.
        simulations: Number of seasons to simulate.
        playoff_seeds: Number of playoff seeds per conference.
        byes_per_conf: First-round byes per conference (drives the number
            of wildcard games).
        tiebreaker_depth: ``'SOS'`` (default), ``'PRE-SOV'``, or
            ``'RANDOM'`` (``'POINTS'`` is unavailable because simulated
            games carry margins, not scores).
        sim_include: ``'REG'`` (standings/seeds only), ``'POST'`` (+
            postseason), or ``'DRAFT'`` (default; + draft order).
        seed: Seed for the numpy RNG driving results and coin tosses.
        return_as_pandas: If ``True``, return pandas DataFrames.
        **kwargs: Passed through to ``compute_results`` (e.g. ``elo=``).

    Returns:
        Dict of frames mirroring the nflseedR simulation list:
        ``standings`` (one row per sim x team), ``games`` (all simulated
        games), ``overall`` (per-team probabilities: wins, playoff, div1,
        seed1, won_conf, won_sb, draft1, draft5), ``team_wins``
        (over/under probabilities vs. half-win lines), and
        ``game_summary`` (per-matchup home/away win rates).

    Raises:
        ValueError: On missing columns, more than one season, no games to
            simulate, or invalid argument values.

    Example:
        Simulate the remainder of a season::

            import sportsdataverse.nfl as nfl
            games = nfl.load_schedules([2024])
            sim = nfl.nfl_simulations(games, simulations=1000, seed=42)
            print(sim["overall"].head())

        Custom initial ELO ratings::

            sim = nfl.nfl_simulations(games, simulations=500, seed=1,
                                      elo={"KC": 1700, "BUF": 1650})

        Pipeline next step (one line)::

            sim["overall"].sort("won_sb", descending=True).head()

    See Also:
        * `nflseedR`_ -- the canonical R implementation this ports.
        * `nflreadpy`_ -- nflverse schedules/data in Python.

    .. _nflseedR: https://nflseedr.com
    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if tiebreaker_depth not in _SIM_DEPTH_MAP:
        raise ValueError("tiebreaker_depth must be one of 'SOS', 'PRE-SOV', 'RANDOM'")
    if sim_include not in _SIM_INCLUDE_MAP:
        raise ValueError("sim_include must be one of 'REG', 'POST', 'DRAFT'")
    depth = _SIM_DEPTH_MAP[tiebreaker_depth]
    include = _SIM_INCLUDE_MAP[sim_include]
    if compute_results is None:
        compute_results = nfl_compute_results
    if simulations < 1:
        raise ValueError("simulations must be >= 1")
    rng = np.random.default_rng(seed)

    games = _validate_sim_games(games)

    # Append missing playoff weeks (simulations.R L211-226)
    if include > 0:
        games = games.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("conf"),
            pl.lit(None, dtype=pl.Utf8).alias("home_round_id"),
            pl.lit(None, dtype=pl.Utf8).alias("away_round_id"),
        )
        dummy = _playoff_dummy(byes_per_conf)
        dummy = dummy.filter(pl.col("week").is_in(games["week"].implode()) == False)  # noqa: E712
        if dummy.height > 0:
            max_reg_week = int(games.filter(pl.col("game_type") == "REG")["old_week"].max())
            dummy = dummy.with_columns(
                (max_reg_week + pl.col("game_type").replace_strict(_PLAYOFF_SUMMANDS, return_dtype=pl.Int64)).alias(
                    "old_week"
                )
            )
            games = pl.concat([games, dummy.select(games.columns)], how="vertical")

    weeks_to_simulate = games.filter(pl.col("result").is_null())["week"].unique(maintain_order=True).to_list()
    if len(weeks_to_simulate) == 0:
        if include == 0:
            raise ValueError("sim_include is 'REG' but there are no missing values in the result column of games.")
        raise ValueError(
            "There are no games left to simulate (no missing results). "
            "If you want standings, please see nfl_season_standings."
        )
    if include == 0 and any(w in _PLAYOFF_WEEKS for w in weeks_to_simulate):
        raise ValueError("Detected post-season games to simulate but sim_include is 'REG'.")
    reg_weeks = sorted((w for w in weeks_to_simulate if w not in _PLAYOFF_WEEKS), key=int)
    post_weeks = [w for w in _PLAYOFF_WEEKS if w in weeks_to_simulate]

    present = set(games["home_team"].drop_nulls().to_list()) | set(games["away_team"].drop_nulls().to_list())
    teams = _teams_frame().filter(pl.col("team").is_in(sorted(present)))

    sims = pl.DataFrame({"sim": pl.Series(range(1, simulations + 1), dtype=pl.Int64)})
    sim_games = sims.join(games, how="cross").with_row_index("__gid")
    sim_teams = sims.join(teams, how="cross")

    # REMAINDER OF REGULAR SEASON --------------------------------------------
    for week_num in reg_weeks:
        out = compute_results(sim_teams, sim_games, week_num, rng=rng, **kwargs)
        sim_teams = out["teams"]
        sim_games = out["games"]

    # STANDINGS AFTER REG SEASON ---------------------------------------------
    st, h2h, _ = _standings_core(
        sim_games.filter(pl.col("result").is_not_null()),
        ranks="CONF",
        depth=depth,
        playoff_seeds=16 if include > 1 else playoff_seeds,
        rng=rng,
        has_scores=False,
    )
    st = st.with_columns(
        pl.when(pl.col("conf_rank").is_null() | (pl.col("conf_rank") > playoff_seeds))
        .then(0)
        .otherwise(None)
        .cast(pl.Int64)
        .alias("exit")
    )

    # Fill exits from already played playoff games (simulate_chunks L111-119)
    po_results = sim_games.filter(pl.col("week").is_in(list(_PLAYOFF_WEEKS)) & pl.col("result").is_not_null())
    if po_results.height > 0:
        losers = po_results.select(
            "sim",
            pl.when(pl.col("result") > 0).then(pl.col("away_team")).otherwise(pl.col("home_team")).alias("team"),
            pl.col("game_type").replace_strict(_EXIT_TO_INT, return_dtype=pl.Int64).alias("__exit_new"),
        )
        st = st.join(losers, on=["sim", "team"], how="left")
        st = st.with_columns(pl.coalesce(pl.col("exit"), pl.col("__exit_new")).alias("exit")).drop("__exit_new")

    # PLAYOFFS -----------------------------------------------------------------
    if include > 0 and len(post_weeks) > 0:
        st = st.with_columns(
            pl.when(pl.col("exit").is_null())
            .then(pl.format("{}-{}-{}", "sim", "conf", "conf_rank"))
            .otherwise(None)
            .alias("__playoff_id")
        )
        for week_num in post_weeks:
            wk = pl.col("week") == week_num
            wk_rows = sim_games.filter(wk)
            if wk_rows["home_team"].null_count() > 0 or wk_rows["away_team"].null_count() > 0:
                remaining = st.filter(pl.col("exit").is_null())
                if week_num == "WC":
                    round_teams = remaining.select(pl.col("__playoff_id").alias("__rid"), "team")
                    sim_games = sim_games.with_columns(
                        pl.when(wk)
                        .then(pl.format("{}-{}", "sim", "home_round_id"))
                        .otherwise(pl.col("home_round_id"))
                        .alias("home_round_id"),
                        pl.when(wk)
                        .then(pl.format("{}-{}", "sim", "away_round_id"))
                        .otherwise(pl.col("away_round_id"))
                        .alias("away_round_id"),
                    )
                else:
                    remaining = _with_frank(
                        remaining,
                        ["sim", "conf"],
                        [("conf_rank", False)],
                        "min",
                        out="__new_rank",
                    )
                    round_teams = remaining.select(
                        pl.format("{}-{}-{}", "sim", "conf", "__new_rank").alias("__rid"),
                        "team",
                    )
                    if week_num == "SB":
                        sim_games = sim_games.with_columns(
                            pl.when(wk)
                            .then(pl.format("{}-AFC-1", "sim"))
                            .otherwise(pl.col("home_round_id"))
                            .alias("home_round_id"),
                            pl.when(wk)
                            .then(pl.format("{}-NFC-1", "sim"))
                            .otherwise(pl.col("away_round_id"))
                            .alias("away_round_id"),
                        )
                    else:
                        sub = wk_rows.select("__gid", "sim", "conf")
                        sub = sub.with_columns(
                            pl.col("__gid").cum_count().over(["sim", "conf"]).alias("__i"),
                            pl.len().over(["sim", "conf"]).alias("__n"),
                        )
                        sub = sub.select(
                            "__gid",
                            pl.format("{}-{}-{}", "sim", "conf", "__i").alias("__hid"),
                            pl.format(
                                "{}-{}-{}",
                                "sim",
                                "conf",
                                2 * pl.col("__n") + 1 - pl.col("__i"),
                            ).alias("__aid"),
                            (pl.col("__i") == 1).alias("__top_seed_home"),
                        )
                        sim_games = sim_games.join(sub, on="__gid", how="left")
                        sim_games = sim_games.with_columns(
                            pl.coalesce(pl.col("__hid"), pl.col("home_round_id")).alias("home_round_id"),
                            pl.coalesce(pl.col("__aid"), pl.col("away_round_id")).alias("away_round_id"),
                        )
                        if week_num == "DIV":
                            # bye team has 14 days rest in the DIV round
                            sim_games = sim_games.with_columns(
                                pl.when(pl.col("__top_seed_home") == True)  # noqa: E712
                                .then(14)
                                .otherwise(pl.col("home_rest"))
                                .cast(pl.Int64)
                                .alias("home_rest")
                            )
                        sim_games = sim_games.drop(["__hid", "__aid", "__top_seed_home"])
                # fill the matchups from the round id lookup
                sim_games = (
                    sim_games.join(
                        round_teams.rename({"team": "__ht"}),
                        left_on="home_round_id",
                        right_on="__rid",
                        how="left",
                    )
                    .join(
                        round_teams.rename({"team": "__at"}),
                        left_on="away_round_id",
                        right_on="__rid",
                        how="left",
                    )
                    .with_columns(
                        pl.when(wk & pl.col("__ht").is_not_null())
                        .then(pl.col("__ht"))
                        .otherwise(pl.col("home_team"))
                        .alias("home_team"),
                        pl.when(wk & pl.col("__at").is_not_null())
                        .then(pl.col("__at"))
                        .otherwise(pl.col("away_team"))
                        .alias("away_team"),
                    )
                    .drop(["__ht", "__at"])
                )

            out = compute_results(sim_teams, sim_games, week_num, rng=rng, **kwargs)
            sim_teams = out["teams"]
            sim_games = out["games"]

            round_loser = sim_games.filter(wk).select(
                "sim",
                pl.when(pl.col("result") < 0).then(pl.col("home_team")).otherwise(pl.col("away_team")).alias("team"),
                pl.lit(_EXIT_TO_INT[week_num], dtype=pl.Int64).alias("__exit_new"),
            )
            st = st.join(round_loser, on=["sim", "team"], how="left")
            st = st.with_columns(pl.coalesce(pl.col("exit"), pl.col("__exit_new")).alias("exit")).drop("__exit_new")
            if week_num == "SB":
                st = st.with_columns(pl.col("exit").fill_null(_EXIT_TO_INT["SB_WIN"]))
        st = st.drop("__playoff_id")

    # restore integer weeks and drop helpers
    sim_games = sim_games.with_columns(pl.col("old_week").alias("week")).drop(
        [c for c in ("old_week", "home_round_id", "away_round_id", "conf", "__gid") if c in sim_games.columns]
    )

    # DRAFT RANKS --------------------------------------------------------------
    if include > 1:
        st = _add_draft_ranks(st, h2h, None, depth, rng)
    st = st.drop([c for c in ("conf_pd",) if c in st.columns]).sort(["sim", "division", "div_rank"])

    # AGGREGATION (simulations.R L302-382) --------------------------------------
    sb_exit = max(_EXIT_TO_INT.values())
    agg_exprs = [
        pl.col("wins").mean().alias("wins"),
        (pl.col("conf_rank") <= playoff_seeds)
        .fill_null(False)  # noqa: FBT003
        .cast(pl.Float64)
        .mean()
        .alias("playoff"),
        (pl.col("div_rank") == 1).cast(pl.Float64).mean().alias("div1"),
        (pl.col("conf_rank") == 1).fill_null(False).cast(pl.Float64).mean().alias("seed1"),
    ]
    if include > 0:
        agg_exprs += [
            (pl.col("exit") >= sb_exit - 1).cast(pl.Float64).mean().alias("won_conf"),
            (pl.col("exit") == sb_exit).cast(pl.Float64).mean().alias("won_sb"),
        ]
    else:
        agg_exprs += [
            pl.lit(None, dtype=pl.Float64).alias("won_conf"),
            pl.lit(None, dtype=pl.Float64).alias("won_sb"),
        ]
    if include > 1:
        agg_exprs += [
            (pl.col("draft_rank") == 1).cast(pl.Float64).mean().alias("draft1"),
            (pl.col("draft_rank") <= 5).cast(pl.Float64).mean().alias("draft5"),
        ]
    else:
        agg_exprs += [
            pl.lit(None, dtype=pl.Float64).alias("draft1"),
            pl.lit(None, dtype=pl.Float64).alias("draft5"),
        ]
    overall = st.group_by("conf", "division", "team").agg(agg_exprs).sort(["conf", "division", "team"])

    st = st.with_columns(pl.col("exit").replace_strict(_INT_TO_EXIT, default=None, return_dtype=pl.Utf8))

    max_games = int(st["games"].max())
    ladder = pl.DataFrame({"wins_line": pl.Series(np.arange(0, max_games * 2 + 1) * 0.5, dtype=pl.Float64)})
    team_wins = (
        st.select("sim", "team", "true_wins")
        .join(ladder, how="cross")
        .group_by("team", "wins_line")
        .agg(
            (pl.col("true_wins") > pl.col("wins_line")).cast(pl.Float64).mean().alias("over_prob"),
            (pl.col("true_wins") < pl.col("wins_line")).cast(pl.Float64).mean().alias("under_prob"),
        )
        .rename({"wins_line": "wins"})
        .sort(["team", "wins"])
    )

    game_summary = (
        sim_games.group_by("game_type", "week", "away_team", "home_team")
        .agg(
            (pl.col("result") < 0).sum().cast(pl.Int64).alias("away_wins"),
            (pl.col("result") > 0).sum().cast(pl.Int64).alias("home_wins"),
            (pl.col("result") == 0).sum().cast(pl.Int64).alias("ties"),
            pl.col("result").mean().alias("result"),
        )
        .with_columns((pl.col("away_wins") + pl.col("home_wins") + pl.col("ties")).alias("games_played"))
        .with_columns(
            ((pl.col("away_wins") + 0.5 * pl.col("ties")) / pl.col("games_played")).alias("away_percentage"),
            ((pl.col("home_wins") + 0.5 * pl.col("ties")) / pl.col("games_played")).alias("home_percentage"),
        )
        .sort(["game_type", "week", "away_team", "home_team"])
    )

    out_frames: Dict[str, Union[pl.DataFrame, "pd.DataFrame"]] = {
        "standings": st,
        "games": sim_games,
        "overall": overall,
        "team_wins": team_wins,
        "game_summary": game_summary,
    }
    if return_as_pandas:
        out_frames = {k: v.to_pandas() for k, v in out_frames.items()}
    return out_frames
