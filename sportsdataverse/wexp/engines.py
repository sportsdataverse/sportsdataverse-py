"""Vintage-ratings engines (Axis A ridge core) + rating-to-WP maps (Axis E).

``ridge_margin_vintages`` refits the shared opponent-adjusted ridge
(:func:`sportsdataverse._common.ratings.opponent_adjusted_ridge`) per
``(season, week)`` over that season's completed prior games, emitting a
vintage-keyed ratings table for
:class:`~sportsdataverse.wexp.store.VintageStore`. The response column is
an argument: the home margin works from the market oracle alone; the
play-level EPA responses (Axis B weighting) plug in when the pbp vintage
captures land.

``ratings_predictor`` turns any registered ratings vintage into a week
predictor for :func:`~sportsdataverse.wexp.backtest.run_backtest` via the
Axis E map — ``margin_normal`` (E2, normal CDF at a tunable sigma) or
``isotonic`` (E3, refit each week on the history's own leak-free expected
margins).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import polars as pl

from sportsdataverse._common.ratings import opponent_adjusted_ridge
from sportsdataverse.cfb.cfb_advanced_constants import GARBAGE_TIME_MARGIN
from sportsdataverse.wexp.backtest import WeekPredictor, elo_predictor, normalize_walk_weeks
from sportsdataverse.wexp.elo import EloConfig
from sportsdataverse.wexp.store import VintageStore
from sportsdataverse.wexp.variants import VariantConfig

__all__ = [
    "GSConfig",
    "build_predictor",
    "cfb_continuity_shifts",
    "cfb_drive_deltas",
    "cfb_drive_ep_responses",
    "glickman_stern_predictor",
    "market_prior_shifts",
    "net_vintages_view",
    "nfl_qb_change_events",
    "ratings_predictor",
    "response_ridge_vintages",
    "ridge_margin_vintages",
]

_VINTAGE_SCHEMA: dict[str, type[pl.DataType]] = {
    "season": pl.Int32,
    "as_of_week": pl.Int32,
    "team_id": pl.Utf8,
    "off_coef": pl.Float64,
    "def_coef": pl.Float64,
    "intercept": pl.Float64,
    "hfa": pl.Float64,
    "lam": pl.Float64,
    "cap": pl.Float64,
    "close_filter": pl.Float64,
}


def ridge_margin_vintages(
    oracle: pl.DataFrame,
    *,
    lam: float,
    resp_col: str = "home_margin",
    cap: Optional[float] = None,
    close_filter: Optional[float] = None,
    history_seasons: int = 0,
    season_decay: float = 0.5,
) -> pl.DataFrame:
    """Build per-week opponent-adjusted ridge rating vintages from an oracle frame.

    For every ``(season, week)`` present, fits the ridge on that season's
    completed games in strictly earlier (walk-normalized) weeks — so the
    vintage at ``as_of_week = W`` satisfies the store's EXCLUSIVE
    convention by construction. Weeks with no prior completed games (week
    1) emit no rows. Neutral-site games carry no HFA indicator in the fit.

    Args:
        oracle: Market-oracle frame (module contract columns).
        lam: Ridge penalty on the team coefficients.
        resp_col: Response column (default ``"home_margin"``; a team-game
            EPA margin column slots in unchanged).
        cap: Axis B2 capped response — clip the response to ``[-cap, cap]``
            before the fit (blowout damping). ``None`` = raw (B1).
        close_filter: Axis B4 garbage filter at game granularity — DROP
            games with ``|response| > close_filter`` from the fit entirely
            (both mirrored rows, so offense and defense sides go
            together), rating teams on competitive games only. Distinct
            from ``cap``: a filtered blowout contributes nothing; a capped
            one still contributes at the cap. ``None`` = keep all.
        history_seasons: Prior seasons folded into each fit (default 0 =
            the within-season-only convention, which rates week 2 off a
            single week of games). Prior-season games enter down-weighted
            by ``season_decay ** seasons_back``, applied as integer row
            replication of the CURRENT season (weight 1 -> the current
            season repeats ``1/season_decay ** history_seasons`` times),
            so the shared unweighted solver is reused unchanged.
        season_decay: Per-season-back weight for ``history_seasons``
            (0.5 = last season counts half).

    Returns:
        A vintage table — ``season`` / ``as_of_week`` / ``team_id`` (Utf8)
        / ``off_coef`` / ``def_coef`` plus the per-vintage ``intercept``
        and ``hfa`` scalars denormalized onto every row. Expected margin
        for (home h, away a): ``h.off_coef + a.def_coef + intercept
        [+ hfa]``.

    Example:
        Quick start::

            from sportsdataverse.wexp.engines import ridge_margin_vintages
            from sportsdataverse.wexp.store import VintageStore
            store = VintageStore()
            store.register("ridge", ridge_margin_vintages(oracle, lam=100.0), entity_key="team_id")
    """
    base = normalize_walk_weeks(oracle).filter(pl.col(resp_col).is_not_null())
    if close_filter is not None:
        base = base.filter(pl.col(resp_col).abs() <= close_filter)
    if cap is not None:
        base = base.with_columns(pl.col(resp_col).clip(-cap, cap))
    # Symmetrize: each game from both perspectives. With offense always the
    # home team the HFA indicator would be constant 1 and collinear with the
    # unpenalized intercept (singular normal equations); the mirrored away
    # view makes it vary and pins the intercept ~0 by construction.
    games = pl.concat(
        [
            base.select(
                "season",
                "week",
                off=pl.col("home_team"),
                deft=pl.col("away_team"),
                resp=pl.col(resp_col),
                homeflag=pl.when(pl.col("neutral_site") == True).then(pl.lit("")).otherwise(pl.col("home_team")),  # noqa: E712
            ),
            base.select(
                "season",
                "week",
                off=pl.col("away_team"),
                deft=pl.col("home_team"),
                resp=-pl.col(resp_col),
                homeflag=pl.lit(""),
            ),
        ]
    )
    frames: list[pl.DataFrame] = []
    for season, week in (
        normalize_walk_weeks(oracle).select("season", "week").unique().sort("season", "week").iter_rows()
    ):
        in_season = games.filter((pl.col("season") == season) & (pl.col("week") < week))
        if history_seasons <= 0:
            fit = in_season
        else:
            # integer-replication weighting: current season repeats
            # `reps` times, each season back one decay step fewer, so the
            # unweighted shared solver sees the intended relative weights
            reps = max(1, round(1.0 / max(season_decay, 1e-6) ** history_seasons))
            parts = [in_season] * reps
            for back in range(1, history_seasons + 1):
                prior = games.filter(pl.col("season") == season - back)
                if prior.height == 0:
                    continue
                parts += [prior] * max(1, round(reps * season_decay**back))
            fit = pl.concat(parts, how="vertical")
        if fit.height == 0:
            continue
        coefs, intercept, hfa = opponent_adjusted_ridge(
            fit,
            off_col="off",
            def_col="deft",
            home_col="homeflag",
            resp_col="resp",
            lam=lam,
        )
        frames.append(
            coefs.with_columns(
                season=pl.lit(season, dtype=pl.Int32),
                as_of_week=pl.lit(week, dtype=pl.Int32),
                intercept=pl.lit(intercept, dtype=pl.Float64),
                hfa=pl.lit(hfa, dtype=pl.Float64),
                lam=pl.lit(lam, dtype=pl.Float64),
                cap=pl.lit(cap, dtype=pl.Float64),
                close_filter=pl.lit(close_filter, dtype=pl.Float64),
            ).select(list(_VINTAGE_SCHEMA))
        )
    if not frames:
        return pl.DataFrame(schema=_VINTAGE_SCHEMA)
    return pl.concat(frames, how="vertical")


def cfb_drive_deltas(pbp: pl.DataFrame) -> pl.DataFrame:
    """Per-drive EP deltas from play-by-play (the shared drive substrate).

    Per drive: ``delta = sum of play EPA`` (perspective-corrected — see
    :func:`cfb_drive_ep_responses`), plus the drive's starting EP and the
    share-of-available ``pct``. Feeds both the team-game response
    aggregation and the post-game WE resampling (G2/G3).

    Inclusion/exclusion classifiers (callers filter, the substrate keeps
    every drive):

    - ``garbage`` — the drive STARTED in Connelly garbage time
      (``|margin_start|`` exceeds the documented per-quarter
      :data:`~sportsdataverse.cfb.cfb_advanced_constants.GARBAGE_TIME_MARGIN`
      band; OT clipped to the Q4 band).
    - ``clock_kill`` — the half's final drive with <= 3 plays and a
      non-positive delta (kneel / clock-kill signature).

    Args:
        pbp: Play frame with ``game_id``, ``pos_team``, ``def_pos_team``,
            ``drive.id``, ``EPA``, ``EP_start``, ``game_play_number``,
            ``period``, ``pos_team_score``, ``def_pos_team_score``.

    Returns:
        One row per (game, drive): ``game_id`` / ``drive.id``, ``off`` /
        ``deft`` (raw Int64 ids), ``delta``, ``start_ep``, ``pct``,
        ``period`` / ``margin_start`` / ``plays``, and the ``garbage`` /
        ``clock_kill`` flags.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.wexp.engines import cfb_drive_deltas
            drives = cfb_drive_deltas(pl.read_parquet("pbp_2019.parquet"))
    """
    return (
        pbp.drop_nulls(["EPA", "EP_start", "drive.id", "pos_team", "def_pos_team"])
        # dtype boundary: early-season releases ship ids as Float64 — pin
        # Int64 here so multi-season concats agree (never stringify a float)
        .with_columns(
            pl.col("game_id").cast(pl.Int64),
            pl.col("pos_team").cast(pl.Int64),
            pl.col("def_pos_team").cast(pl.Int64),
            pl.col("EPA").cast(pl.Float64),
            pl.col("EP_start").cast(pl.Float64),
        )
        .sort("game_play_number")
        .group_by("game_id", "drive.id", maintain_order=True)
        .agg(
            off=pl.col("pos_team").first(),
            deft=pl.col("def_pos_team").first(),
            delta=pl.col("EPA").sum(),
            start_ep=pl.col("EP_start").first(),
            period=pl.col("period").cast(pl.Int64).first(),
            margin_start=(pl.col("pos_team_score") - pl.col("def_pos_team_score")).cast(pl.Int64).first(),
            plays=pl.len().cast(pl.Int64),
            last_play=pl.col("game_play_number").max(),
        )
        .with_columns(
            pct=pl.col("delta") / (7.0 - pl.col("start_ep")).clip(lower_bound=0.5),
            half=pl.when(pl.col("period") <= 2).then(1).otherwise(2),
            # the documented Connelly per-quarter bands; OT clipped to Q4's
            garbage=pl.col("margin_start").abs()
            > pl.col("period").clip(1, 4).replace_strict(GARBAGE_TIME_MARGIN, return_dtype=pl.Int64),
        )
        .with_columns(
            clock_kill=(pl.col("last_play") == pl.col("last_play").max().over("game_id", "half"))
            & (pl.col("plays") <= 3)
            & (pl.col("delta") <= 0.0)
        )
        .drop("last_play", "half")
    )


def cfb_drive_ep_responses(pbp: pl.DataFrame) -> pl.DataFrame:
    """Per team-game drive-EP efficiency from play-by-play (both sides rated).

    The user-specified "starting drive EP to end drive EP" perspective:
    per drive, ``delta = sum of play EPA over the drive`` — the telescoped
    start-to-end EP change WITH the library's possession-perspective
    corrections. (A naive ``EP_end(last) - EP_start(first)`` is wrong:
    on drive-ending plays the raw ``EP_end`` is stated from the NEW
    possession team's perspective, which flips the sign on punt/turnover
    drives — it fit a NEGATIVE margin scale on the real tune window.)
    Two responses per team-game, both MEANS over that offense's drives:

    - ``resp`` — the raw per-drive delta (per-drive efficiency,
      pace-free, the FEI-style granularity).
    - ``resp_pct`` — the user-specified share of AVAILABLE drive EP:
      ``delta / (7 - EP_start of the drive's first play)`` (7 = max drive
      EP). A drive starting at the opponent 5 has little left to gain, so
      capturing it counts more per point. ``available`` is floored at 0.5
      so goal-line starts cannot blow the ratio up. Rate x possessions
      carries the volume downstream (the fitted margin scale absorbs the
      average pace).

    Each game emits one row per offense with the defense alongside, so
    the weekly ridge downstream fits an ``off_coef`` AND a ``def_coef``
    for every team, on either response.

    Args:
        pbp: Play frame with ``game_id``, ``pos_team``, ``def_pos_team``,
            ``drive.id``, ``EPA``, ``EP_start``, ``game_play_number``
            (the espn_cfb_pbp release columns; prune before loading).

    Returns:
        One row per (game, offense): ``game_id`` (Utf8), ``off_team_id`` /
        ``def_team_id`` (Utf8, cast from the raw Int64 ESPN ids),
        ``resp`` / ``resp_pct`` (Float64), ``drives`` (Int64 count).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.wexp.engines import cfb_drive_ep_responses
            rows = cfb_drive_ep_responses(pl.read_parquet("pbp_2019.parquet"))
    """
    drives = cfb_drive_deltas(pbp)
    return (
        drives.group_by("game_id", "off", "deft")
        .agg(resp=pl.col("delta").mean(), resp_pct=pl.col("pct").mean(), drives=pl.len())
        .select(
            game_id=pl.col("game_id").cast(pl.Int64).cast(pl.Utf8),
            off_team_id=pl.col("off").cast(pl.Int64).cast(pl.Utf8),
            def_team_id=pl.col("deft").cast(pl.Int64).cast(pl.Utf8),
            resp=pl.col("resp"),
            resp_pct=pl.col("resp_pct"),
            drives=pl.col("drives").cast(pl.Int64),
        )
    )


def response_ridge_vintages(
    responses: pl.DataFrame,
    oracle: pl.DataFrame,
    *,
    lam: float,
    close_filter: Optional[float] = None,
    resp_col: str = "resp",
) -> pl.DataFrame:
    """Weekly opponent-adjusted ridge on per-team-game responses (off + def).

    Joins the responses to the oracle by ``game_id`` (walk-normalized
    week, home flag, final margin), then per ``(season, week)`` fits
    :func:`~sportsdataverse._common.ratings.opponent_adjusted_ridge` on
    that season's rows from strictly earlier weeks — EXCLUSIVE vintages,
    same convention as :func:`ridge_margin_vintages`. Each game
    contributes one row per side, so offense and defense are separately
    identified; ``adj_net = off_coef - def_coef`` is the margin-driving
    team strength (register the output and serve it through
    :func:`net_vintages_view`).

    Args:
        responses: Frame from :func:`cfb_drive_ep_responses` (``game_id``,
            ``off_team_id``, ``def_team_id``, ``resp``).
        oracle: Market-oracle frame (contract columns incl. team ids).
        lam: Ridge penalty on the team coefficients.
        close_filter: Axis B4 — drop games with final ``|home_margin| >
            close_filter`` from the fit (both sides go together).
        resp_col: Which response to fit (``"resp"`` = raw per-drive delta;
            ``"resp_pct"`` = share of available drive EP).

    Returns:
        Vintage table: ``season`` / ``as_of_week`` / ``team_id`` (Utf8) /
        ``off_coef`` / ``def_coef`` / ``adj_net`` plus per-vintage
        ``intercept`` / ``hfa`` and the ``lam`` / ``close_filter`` stamps.

    Example:
        Quick start::

            from sportsdataverse.wexp.engines import response_ridge_vintages
            vint = response_ridge_vintages(rows, oracle, lam=1.0)
    """
    games = normalize_walk_weeks(oracle).select(
        "game_id", "season", "week", "home_team_id", "neutral_site", "home_margin"
    )
    # Drop null RESPONSES as well as null margins. The ridge solver has no
    # null handling: one null response makes every coefficient in that
    # vintage NaN, and NaN survives the downstream `drop_nulls` guards
    # (polars null != NaN), so the run finishes with silently-missing
    # predictions instead of raising. `cfb_scoring_opportunities` emits a
    # deliberate null `points_per_opp` for teams with no scoring
    # opportunity, which is exactly this path. `ridge_margin_vintages`
    # already filters its response; these two must not be asymmetric.
    rows = responses.join(games, on="game_id", how="inner").filter(
        pl.col("home_margin").is_not_null() & pl.col(resp_col).is_not_null()
    )
    if close_filter is not None:
        rows = rows.filter(pl.col("home_margin").abs() <= close_filter)
    rows = rows.with_columns(
        homeflag=pl.when((pl.col("off_team_id") == pl.col("home_team_id")) & (pl.col("neutral_site") == False))  # noqa: E712
        .then(pl.col("off_team_id"))
        .otherwise(pl.lit(""))
    )
    frames: list[pl.DataFrame] = []
    for season, week in (
        normalize_walk_weeks(oracle).select("season", "week").unique().sort("season", "week").iter_rows()
    ):
        fit = rows.filter((pl.col("season") == season) & (pl.col("week") < week))
        if fit.height == 0:
            continue
        coefs, intercept, hfa = opponent_adjusted_ridge(
            fit,
            off_col="off_team_id",
            def_col="def_team_id",
            home_col="homeflag",
            resp_col=resp_col,
            lam=lam,
        )
        frames.append(
            coefs.with_columns(
                adj_net=pl.col("off_coef") - pl.col("def_coef"),
                season=pl.lit(season, dtype=pl.Int32),
                as_of_week=pl.lit(week, dtype=pl.Int32),
                intercept=pl.lit(intercept, dtype=pl.Float64),
                hfa=pl.lit(hfa, dtype=pl.Float64),
                lam=pl.lit(lam, dtype=pl.Float64),
                close_filter=pl.lit(close_filter, dtype=pl.Float64),
            )
        )
    if not frames:
        # empty frames carry the documented schema (project convention)
        return pl.DataFrame(
            schema={
                "team_id": pl.Utf8,
                "off_coef": pl.Float64,
                "def_coef": pl.Float64,
                "adj_net": pl.Float64,
                "season": pl.Int32,
                "as_of_week": pl.Int32,
                "intercept": pl.Float64,
                "hfa": pl.Float64,
                "lam": pl.Float64,
                "close_filter": pl.Float64,
            }
        )
    return pl.concat(frames, how="vertical")


def net_vintages_view(
    vintages: pl.DataFrame,
    *,
    scale: float = 1.0,
    hfa: float = 0.0,
    st_weight: float = 0.0,
    net_col: str = "adj_net",
) -> pl.DataFrame:
    """Adapt a net-rating vintage table to the :func:`ratings_predictor` contract.

    For tables like ``load_nfl_ratings_weekly`` (one ``adj_net`` per team
    per vintage): expected margin becomes ``scale * (net_h - net_a) + hfa``
    via ``off_coef = scale*net``, ``def_coef = -scale*net``, ``intercept =
    0``. Under the isotonic map ``scale`` is irrelevant (monotone); it
    matters only for ``margin_normal``. Keys are normalized to the store
    contract (``season`` Int32, ``team_id`` Utf8).

    Args:
        vintages: Frame with ``season``, ``as_of_week``, ``team_id``,
            ``adj_net``.
        scale: Margin points per unit of net-rating difference (fit on the
            tune window; a tunable in the variant hash).
        hfa: Home-field advantage in margin points.
        st_weight: Weight on ``adj_st_epa`` added to the net (``adj_net``
            excludes special teams; 0 = ignore ST).
        net_col: The net-rating column (``"adj_net"`` for the A3 EPA core;
            ``"fei_net"`` on the CFB vintages for the A5 FEI core).

    Returns:
        A vintage table registrable on :class:`~sportsdataverse.wexp.store.VintageStore`.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_ratings_weekly
            from sportsdataverse.wexp.engines import net_vintages_view
            from sportsdataverse.wexp.store import VintageStore
            store = VintageStore()
            store.register("epa", net_vintages_view(load_nfl_ratings_weekly(seasons=[2024]),
                                                    scale=24.0, hfa=2.3), entity_key="team_id")
    """
    net = pl.col(net_col)
    if st_weight:
        net = net + st_weight * pl.col("adj_st_epa")
    out = vintages.select(
        season=pl.col("season").cast(pl.Int32),
        as_of_week=pl.col("as_of_week").cast(pl.Int32),
        team_id=pl.col("team_id").cast(pl.Utf8),
        off_coef=scale * net,
        def_coef=-scale * net,
        intercept=pl.lit(0.0),
        hfa=pl.lit(hfa, dtype=pl.Float64),
    )
    # pass build-param stamps through so the predictor's mismatch refusal
    # still protects tables served via this view
    stamps = [c for c in ("lam", "cap", "close_filter") if c in vintages.columns]
    if stamps:
        out = pl.concat([out, vintages.select(stamps)], how="horizontal")
    return out


def ratings_predictor(
    table: str,
    *,
    wp_map: str = "margin_normal",
    sigma: float = 13.45,
    iso_min_fit: int = 100,
    lam: Optional[float] = None,
    cap: Optional[float] = None,
    close_filter: Optional[float] = None,
    join_on: tuple[str, str] = ("home_team", "away_team"),
) -> WeekPredictor:
    """Wrap a registered ratings vintage table as a week predictor.

    Joins home and away ratings through the store's leak-free
    ``join_asof``, reconstructs the expected home margin, and maps it to
    P(home) per the Axis E ``wp_map``. Games where either team lacks a
    served vintage get a null probability (scored-on-coverage, never
    imputed).

    Args:
        table: Table name registered on the store
            (:func:`ridge_margin_vintages` schema).
        wp_map: ``"margin_normal"`` (E2) or ``"isotonic"`` (E3,
            walk-forward refit on history each week).
        sigma: Margin SD for the normal link.
        iso_min_fit: Minimum history games with a rated margin before the
            isotonic map will fit; below it the week predicts null.
        lam: The ridge penalty the variant hash claims. When given, the
            predictor refuses (ValueError) a table stamped with a
            different ``lam`` — a store/variant mismatch must error, not
            write mislabeled leaderboard rows.
        cap: The Axis B2 response cap the variant claims (``None`` = raw
            response). Checked BIDIRECTIONALLY against the table's ``cap``
            stamp when present — a raw variant served from a capped table
            is just as mislabeled as the reverse.
        close_filter: The Axis B4 close-game fit filter the variant
            claims (``None`` = unfiltered). Same bidirectional stamp
            check as ``cap``.
        join_on: The (home, away) oracle columns matched against the
            table's ``team_id``. Default team-NAME columns fit tables
            keyed by name (ridge_margin_vintages; NFL, where the abbr IS
            the id). Tables keyed by provider id (CFB ratings vintages)
            need ``("home_team_id", "away_team_id")`` — same dtype either
            way, so a wrong choice fails as zero matches, which the
            driver refuses loudly.

    Returns:
        A predictor callable for :func:`~sportsdataverse.wexp.backtest.run_backtest`.

    Raises:
        ValueError: If ``wp_map`` is unknown, or the predictor is invoked
            without a store.

    Example:
        Quick start::

            from sportsdataverse.wexp.backtest import run_backtest
            from sportsdataverse.wexp.engines import ratings_predictor
            probs, rows = run_backtest(oracle, ratings_predictor("ridge"),
                                       model_id="ridge_margin", store=store)
    """
    if wp_map not in ("margin_normal", "isotonic"):
        raise ValueError(f"unknown wp_map {wp_map!r}; one of ('margin_normal', 'isotonic')")

    def _expected_margin(games: pl.DataFrame, store: VintageStore) -> pl.DataFrame:
        g = store.join_asof(games, table, on={join_on[0]: "team_id"}, prefix="rt_home_")
        g = store.join_asof(g, table, on={join_on[1]: "team_id"}, prefix="rt_away_")
        hfa = pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(pl.col("rt_home_hfa"))  # noqa: E712
        return g.with_columns(
            __exp_margin=pl.col("rt_home_off_coef") + pl.col("rt_away_def_coef") + pl.col("rt_home_intercept") + hfa
        )

    def predict(history: pl.DataFrame, slate: pl.DataFrame, store: Optional[VintageStore]) -> pl.Series:
        if store is None:
            raise ValueError("ratings_predictor requires a VintageStore")
        tbl = store.table(table)
        # the variant hash claims these build params; refuse a mismatched
        # table rather than writing wrong-but-plausible leaderboard rows
        if lam is not None and ("lam" not in tbl.columns or tbl["lam"].unique().to_list() != [lam]):
            served = tbl["lam"].unique().to_list() if "lam" in tbl.columns else []
            raise ValueError(f"variant claims lam={lam} but table {table!r} was built with lam={served}")
        if "cap" in tbl.columns and tbl["cap"].unique().to_list() != [cap]:
            raise ValueError(
                f"variant claims cap={cap} but table {table!r} was built with cap={tbl['cap'].unique().to_list()}"
            )
        if "close_filter" in tbl.columns and tbl["close_filter"].unique().to_list() != [close_filter]:
            raise ValueError(
                f"variant claims close_filter={close_filter} but table {table!r} was built with "
                f"close_filter={tbl['close_filter'].unique().to_list()}"
            )
        margins = _expected_margin(slate, store)["__exp_margin"].to_numpy()
        if wp_map == "margin_normal":
            from scipy.stats import norm

            return pl.Series(norm.cdf(margins / sigma)).fill_nan(None)
        fit = _expected_margin(history, store).drop_nulls(["__exp_margin", "home_win"])
        if fit.height < iso_min_fit:
            return pl.Series([None] * slate.height, dtype=pl.Float64)
        from sklearn.isotonic import IsotonicRegression

        iso = IsotonicRegression(y_min=0.01, y_max=0.99, out_of_bounds="clip", increasing=True)
        iso.fit(fit["__exp_margin"].to_numpy(), fit["home_win"].to_numpy())
        # sklearn rejects NaN at predict; unrated games stay null
        out = np.full(len(margins), np.nan)
        rated = ~np.isnan(margins)
        if rated.any():
            out[rated] = iso.predict(margins[rated])
        return pl.Series(out).fill_nan(None)

    return predict


@dataclass(frozen=True)
class GSConfig:
    """Glickman-Stern state-space tunables (Axis A6; all seeds, none sacred).

    Attributes:
        sigma_w: Week-to-week strength innovation SD (RESEARCH seed ~0.9).
        sigma_s: Season-to-season innovation SD (seed ~2.35).
        season_shrink: AR(1) shrink toward the league mean at each season
            boundary (the model's own carryover; 0 = full reset).
        sigma_y: Observation noise SD on the game margin.
        hfa: Home-field advantage in margin points (0 at neutral sites).
        init_sd: Prior SD for a team's first-ever strength state.
    """

    sigma_w: float = 0.9
    sigma_s: float = 2.35
    season_shrink: float = 0.82
    sigma_y: float = 13.45
    hfa: float = 2.3
    init_sd: float = 5.0


def glickman_stern_predictor(
    config: GSConfig = GSConfig(),
    season_priors: Optional[pl.DataFrame] = None,
    variance_events: Optional[pl.DataFrame] = None,
) -> WeekPredictor:
    """Wrap the Glickman-Stern (1998) Kalman filter as a week predictor.

    Team strengths follow a weekly random walk (``sigma_w`` innovation)
    with an AR(1) season transition (``season_shrink`` + ``sigma_s``);
    each week the filter batch-updates on that week's observed margins.
    The filter re-runs FORWARD over the driver's history every week —
    filtering only, never smoothing, so predictions are walk-forward by
    construction. P(home) uses the model's own predictive margin
    distribution: ``Phi(mu / sqrt(state var + sigma_y^2))`` — the
    margin_normal map with a per-game predictive SD.

    Args:
        config: Tunable parameters.
        season_priors: Optional continuity table ``(season, team,
            prior_shift)`` in MARGIN points (Axis D3 composed into A6):
            each team's shift is added to its state mean at season entry
            — the start season at filter init, then every boundary after
            the AR shrink. Preseason knowledge only; never derive shifts
            from the season they apply to.
        variance_events: Optional ``(season, week, team, extra_var)``
            table of state-uncertainty events in squared margin points —
            e.g. a starting-QB change known at kickoff. The team's state
            VARIANCE is inflated by ``extra_var`` when the filter arrives
            at that week, widening the update gate without moving the
            mean. Events must be pre-game knowledge for their week.

    Returns:
        A predictor callable for :func:`~sportsdataverse.wexp.backtest.run_backtest`.

    Example:
        Quick start::

            from sportsdataverse.wexp.backtest import run_backtest
            from sportsdataverse.wexp.engines import GSConfig, glickman_stern_predictor
            probs, rows = run_backtest(oracle, glickman_stern_predictor(GSConfig()), model_id="gs")
    """
    priors: dict[tuple[int, str], float] = {}
    if season_priors is not None:
        n_dup = season_priors.height - season_priors.unique(subset=["season", "team"]).height
        if n_dup:
            raise ValueError(f"season_priors has {n_dup} duplicate (season, team) row(s)")
        priors = {
            (int(r["season"]), str(r["team"])): float(r["prior_shift"]) for r in season_priors.iter_rows(named=True)
        }
    var_events: dict[tuple[int, int, str], float] = {}
    if variance_events is not None:
        n_dup = variance_events.height - variance_events.unique(subset=["season", "week", "team"]).height
        if n_dup:
            raise ValueError(f"variance_events has {n_dup} duplicate (season, week, team) row(s)")
        var_events = {
            (int(r["season"]), int(r["week"]), str(r["team"])): float(r["extra_var"])
            for r in variance_events.iter_rows(named=True)
        }

    def predict(history: pl.DataFrame, slate: pl.DataFrame, store: Optional[VintageStore]) -> pl.Series:
        from scipy.stats import norm

        teams = sorted(
            set(history["home_team"].to_list())
            | set(history["away_team"].to_list())
            | set(slate["home_team"].to_list())
            | set(slate["away_team"].to_list())
        )
        idx = {t: i for i, t in enumerate(teams)}
        n = len(teams)
        # a prior table keyed on the wrong entity space (ids vs names) matches
        # NOTHING and silently degrades to the no-prior filter — refuse it
        if priors and not any(team in idx for _season, team in priors):
            raise ValueError(
                "season_priors matched no team in the filter's state space — "
                "priors must be keyed on the same team column as the oracle's "
                "home_team/away_team (names), not team ids"
            )
        x = np.zeros(n)
        p_cov = np.eye(n) * config.init_sd**2
        cur: Optional[tuple[int, int]] = None

        def shift_vec(season: int) -> np.ndarray:
            v = np.zeros(n)
            for (s, team), val in priors.items():
                if s == season and team in idx:
                    v[idx[team]] = val
            return v

        def advance(season: int, week: int) -> None:
            nonlocal x, p_cov, cur
            if cur is None:
                x = x + shift_vec(season)  # start-season continuity priors
            else:
                s0, w0 = cur
                if season > s0:
                    for s_new in range(s0 + 1, season + 1):
                        x = config.season_shrink * x + shift_vec(s_new)
                        p_cov = config.season_shrink**2 * p_cov + config.sigma_s**2 * np.eye(n)
                    drift = max(week - 1, 0)
                else:
                    drift = week - w0
                p_cov = p_cov + config.sigma_w**2 * drift * np.eye(n)
            if var_events:
                # state-uncertainty events for the arrival week (e.g. QB
                # change): inflate the team's variance, not its mean
                for (s, w, team), extra in var_events.items():
                    if s == season and w == week and team in idx:
                        p_cov[idx[team], idx[team]] += extra
            cur = (season, week)

        def design(games: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
            m = games.height
            h_mat = np.zeros((m, n))
            rows = games.select("home_team", "away_team", "neutral_site").iter_rows()
            hfa_vec = np.zeros(m)
            for i, (home, away, neutral) in enumerate(rows):
                h_mat[i, idx[home]] = 1.0
                h_mat[i, idx[away]] = -1.0
                hfa_vec[i] = 0.0 if neutral else config.hfa
            return h_mat, hfa_vec

        for season, week in history.select("season", "week").unique().sort("season", "week").iter_rows():
            group = history.filter((pl.col("season") == season) & (pl.col("week") == week))
            advance(season, week)
            h_mat, hfa_vec = design(group)
            y = group["home_margin"].to_numpy() - hfa_vec
            s_mat = h_mat @ p_cov @ h_mat.T + config.sigma_y**2 * np.eye(group.height)
            k_gain = np.linalg.solve(s_mat, h_mat @ p_cov).T
            x = x + k_gain @ (y - h_mat @ x)
            p_cov = p_cov - k_gain @ h_mat @ p_cov
            p_cov = (p_cov + p_cov.T) / 2.0  # enforce symmetry

        advance(int(slate["season"][0]), int(slate["week"][0]))
        h_mat, hfa_vec = design(slate)
        mu = h_mat @ x + hfa_vec
        var = np.einsum("ij,jk,ik->i", h_mat, p_cov, h_mat) + config.sigma_y**2
        return pl.Series(norm.cdf(mu / np.sqrt(var)))

    return predict


def market_prior_shifts(
    oracle: pl.DataFrame,
    *,
    seed_weeks: int = 3,
    lam: float = 5.0,
    market_col: str = "spread_close",
) -> pl.DataFrame:
    """Season-entry priors distilled from the market's early-season lines (Axis D4).

    For each season, fits the shared opponent-adjusted ridge on the
    MARKET'S expected margin (the line, not the observed result) over
    that season's first ``seed_weeks`` weeks, yielding a market-implied
    strength per team in margin points. Shaped like
    :func:`cfb_continuity_shifts` so it drops straight into
    :func:`glickman_stern_predictor`'s ``season_priors``.

    **Scoring contract (the caller's responsibility):** the seed weeks
    are NOT pre-game knowledge for themselves — a week-2 line reflects
    week-1 results. Score only games in weeks ``> seed_weeks`` when using
    these priors, or the early games are predicted by a prior their own
    lines built. The ablation driver enforces this.

    Args:
        oracle: Market-oracle frame (contract columns incl. the market
            column and team ids).
        seed_weeks: Weeks of lines distilled into the prior. Week 1 alone
            is too thin once FCS opponents are excluded (691 FBS-vs-FBS
            week-1 games across 2004-2021), so the default reaches 3.
        lam: Ridge penalty on the team coefficients.
        market_col: Market expected-margin column (``spread_close`` has
            the widest coverage; ``spread_open`` is thin pre-2021).

    Returns:
        ``(season, team, prior_shift)`` — one row per team the market
        priced in the seed window. Teams the market did not price get no
        row and therefore no shift (never imputed).

    Example:
        Quick start::

            from sportsdataverse.wexp.engines import market_prior_shifts
            shifts = market_prior_shifts(oracle, seed_weeks=3)
    """
    # NOTE: keyed on team NAME, not id — glickman_stern_predictor's state
    # vector is built from home_team/away_team, and cfb_continuity_shifts
    # uses the same space. An id-keyed prior matches nothing, silently.
    base = normalize_walk_weeks(oracle).drop_nulls([market_col, "home_team", "away_team"])
    frames: list[pl.DataFrame] = []
    for season in sorted(base["season"].unique().to_list()):
        seed = base.filter((pl.col("season") == season) & (pl.col("week") <= seed_weeks))
        if seed.height == 0:
            continue
        rows = pl.concat(
            [
                seed.select(
                    off=pl.col("home_team"),
                    deft=pl.col("away_team"),
                    resp=pl.col(market_col),
                    homeflag=pl.when(pl.col("neutral_site") == True)  # noqa: E712
                    .then(pl.lit(""))
                    .otherwise(pl.col("home_team")),
                ),
                seed.select(
                    off=pl.col("away_team"),
                    deft=pl.col("home_team"),
                    resp=-pl.col(market_col),
                    homeflag=pl.lit(""),
                ),
            ]
        )
        coefs, _intercept, _hfa = opponent_adjusted_ridge(
            rows, off_col="off", def_col="deft", home_col="homeflag", resp_col="resp", lam=lam
        )
        frames.append(
            coefs.select(
                season=pl.lit(season, dtype=pl.Int32),
                team=pl.col("team_id"),
                prior_shift=pl.col("off_coef") - pl.col("def_coef"),
            )
        )
    if not frames:
        return pl.DataFrame(schema={"season": pl.Int32, "team": pl.Utf8, "prior_shift": pl.Float64})
    return pl.concat(frames, how="vertical")


def cfb_continuity_shifts(
    oracle: pl.DataFrame,
    talent: pl.DataFrame,
    returning: pl.DataFrame,
    *,
    beta_talent: float = 0.0,
    beta_returning: float = 0.0,
) -> pl.DataFrame:
    """Compile CFB talent + returning production into Elo prior shifts (Axis D3).

    ``prior_shift = beta_talent * talent_z + beta_returning * returning_c``
    in rating points, where ``talent_z`` is the within-season z-score of
    the 247 talent composite and ``returning_c`` is the within-season
    centered overall returning-production share. Both are preseason
    knowledge for their season (leak-free by construction). Teams missing
    one input take 0 for that term — a neutral default, never imputed
    from games. The id -> display-name map comes from the oracle itself
    (``home_team_id``/``home_team`` pairs), so no external crosswalk.

    Args:
        oracle: Market-oracle frame (contract columns incl. team ids).
        talent: ``load_cfb_team_talent`` frame (``season``, ``team_id``,
            ``talent_composite``).
        returning: ``load_cfb_returning_production`` frame (``season``,
            ``team_id``, ``overall_returning``).
        beta_talent: Rating points per talent z-score.
        beta_returning: Rating points per centered returning share.

    Returns:
        ``(season, team, prior_shift)`` for
        :func:`~sportsdataverse.wexp.elo.elo_ratings` — one row per
        oracle team with at least one input.

    Example:
        Quick start::

            from sportsdataverse.wexp.engines import cfb_continuity_shifts
            shifts = cfb_continuity_shifts(oracle, talent, returning,
                                           beta_talent=50.0, beta_returning=100.0)
    """
    id_map = pl.concat(
        [
            oracle.select(team_id=pl.col("home_team_id"), team=pl.col("home_team")),
            oracle.select(team_id=pl.col("away_team_id"), team=pl.col("away_team")),
        ]
    ).unique()
    t = talent.select(
        season=pl.col("season").cast(pl.Int32),
        team_id=pl.col("team_id").cast(pl.Int64).cast(pl.Utf8),
        talent_z=(pl.col("talent_composite") - pl.col("talent_composite").mean().over("season"))
        / pl.col("talent_composite").std().over("season"),
    )
    r = returning.select(
        season=pl.col("season").cast(pl.Int32),
        team_id=pl.col("team_id").cast(pl.Int64).cast(pl.Utf8),
        returning_c=pl.col("overall_returning") - pl.col("overall_returning").mean().over("season"),
    )
    feat = t.join(r, on=["season", "team_id"], how="full", coalesce=True)
    assert feat.schema["team_id"] == id_map.schema["team_id"]
    out = (
        feat.join(id_map, on="team_id", how="inner")
        .with_columns(
            prior_shift=beta_talent * pl.col("talent_z").fill_null(0.0)
            + beta_returning * pl.col("returning_c").fill_null(0.0)
        )
        .select("season", "team", "prior_shift")
    )
    n_dup = out.height - out.unique(subset=["season", "team"]).height
    if n_dup:
        raise ValueError(f"continuity shifts have {n_dup} duplicate (season, team) row(s) — id map is not 1:1")
    return out


def nfl_qb_change_events(schedule: pl.DataFrame, *, extra_var: float) -> pl.DataFrame:
    """Within-season starting-QB changes as GS state-uncertainty events.

    From an nflverse schedule frame (``season``, ``week``, ``home_team`` /
    ``away_team``, ``home_qb_id`` / ``away_qb_id``): a team-week is an
    event when its starter differs from the SAME team's previous game in
    the SAME season (openers exempt — the season transition already
    carries offseason uncertainty). The starter ids are pre-game
    knowledge, so the events are walk-forward safe.

    Args:
        schedule: nflverse schedule frame.
        extra_var: Variance added to the team's state at the event week
            (squared margin points; FPI's ~3.3-pt QB adjustments suggest
            seeds around 9-16).

    Returns:
        ``(season, week, team, extra_var)`` — one row per changed
        team-week, for
        :func:`glickman_stern_predictor`'s ``variance_events``.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_schedule
            from sportsdataverse.wexp.engines import nfl_qb_change_events
            events = nfl_qb_change_events(load_nfl_schedule([2024]), extra_var=9.0)
    """
    team_games = pl.concat(
        [
            schedule.select(
                season=pl.col("season").cast(pl.Int32),
                week=pl.col("week").cast(pl.Int32),
                team=pl.col("home_team").cast(pl.Utf8),
                qb=pl.col("home_qb_id").cast(pl.Utf8),
            ),
            schedule.select(
                season=pl.col("season").cast(pl.Int32),
                week=pl.col("week").cast(pl.Int32),
                team=pl.col("away_team").cast(pl.Utf8),
                qb=pl.col("away_qb_id").cast(pl.Utf8),
            ),
        ]
    ).sort("season", "team", "week")
    return (
        team_games.with_columns(prev_qb=pl.col("qb").shift(1).over("season", "team"))
        .filter(pl.col("qb").is_not_null() & pl.col("prev_qb").is_not_null() & (pl.col("qb") != pl.col("prev_qb")))
        .select("season", "week", "team")
        .with_columns(extra_var=pl.lit(extra_var, dtype=pl.Float64))
    )


def build_predictor(
    config: VariantConfig,
    *,
    table: str = "ridge",
    sigma: float = 13.45,
    season_priors: Optional[pl.DataFrame] = None,
    variance_events: Optional[pl.DataFrame] = None,
) -> WeekPredictor:
    """Dispatch a variant config to its implemented week predictor.

    Implemented cells: ``elo_margin`` (prior ``flat`` = full season reset,
    ``carryover`` = the ``carryover`` tunable; ``wp_map`` must be
    ``elo_logistic``) and ``ridge_epa`` with ``response="raw"`` /
    ``opponent_adjust="ridge"`` / ``prior="flat"`` served from a
    registered ridge vintage table with ``wp_map`` in ``margin_normal`` /
    ``isotonic``. Every other valid config cell raises
    ``NotImplementedError`` until its engine lands — never a silent
    fallback to a different model.

    Args:
        config: The variant to build.
        table: Ratings vintage table name (EPA-family cores).
        sigma: Default margin SD for the normal link (``sigma`` in
            ``config.params`` wins).
        season_priors: Continuity-prior table (from
            :func:`cfb_continuity_shifts`); REQUIRED when
            ``prior="carryover_continuity"`` and ignored otherwise.

    Returns:
        A predictor callable for :func:`~sportsdataverse.wexp.backtest.run_backtest`.

    Raises:
        NotImplementedError: For a valid config cell whose engine has not
            landed yet.

    Example:
        Quick start::

            from sportsdataverse.wexp.engines import build_predictor
            from sportsdataverse.wexp.variants import VariantConfig
            predict = build_predictor(VariantConfig(
                core="elo_margin", response="raw", opponent_adjust="none",
                prior="carryover", wp_map="elo_logistic", hfa="fixed"))
    """
    params = dict(config.params)
    if (
        config.core == "glickman_stern"
        and config.wp_map == "margin_normal"  # native map: Phi(mu / predictive sd)
        and config.hfa == "fixed"
    ):
        if config.prior in ("carryover_continuity", "market_open_informed") and season_priors is None:
            raise ValueError(f"prior={config.prior!r} requires a season_priors table")
        shrink = 0.0 if config.prior == "flat" else params.get("season_shrink", 0.82)
        return glickman_stern_predictor(
            GSConfig(
                sigma_w=params.get("sigma_w", 0.9),
                sigma_s=params.get("sigma_s", 2.35),
                season_shrink=shrink,
                sigma_y=params.get("sigma_y", 13.45),
                hfa=params.get("hfa", 2.3),
                init_sd=params.get("init_sd", 5.0),
            ),
            season_priors=season_priors if config.prior in ("carryover_continuity", "market_open_informed") else None,
            variance_events=variance_events,
        )
    if (
        config.core == "elo_margin"
        and config.wp_map == "elo_logistic"
        and config.hfa in ("fixed", "per_era")  # team_specific: no engine yet
        and config.prior in ("flat", "carryover", "carryover_continuity")  # market_open_informed: no engine yet
    ):
        if config.prior == "carryover_continuity" and season_priors is None:
            raise ValueError("prior='carryover_continuity' requires a season_priors table (cfb_continuity_shifts)")
        hfa_map: Optional[dict[int, float]] = None
        if config.hfa == "per_era":
            # minimal era split: the no-fans COVID season gets its own HFA
            if "hfa_covid" not in params:
                raise ValueError("hfa='per_era' requires an 'hfa_covid' entry in params")
            hfa_map = {2020: params["hfa_covid"]}
        carryover = 0.0 if config.prior == "flat" else params.get("carryover", 0.67)
        return elo_predictor(
            EloConfig(
                k=params.get("k", 20.0),
                z=params.get("z", 400.0),
                hfa=params.get("hfa", 65.0),
                carryover=carryover,
            ),
            season_priors=season_priors if config.prior == "carryover_continuity" else None,
            hfa_by_season=hfa_map,
        )
    if (
        config.core == "ridge_epa"
        and config.response in ("raw", "capped", "garbage_filtered")
        and config.opponent_adjust == "ridge"
        and config.prior == "flat"
        and config.hfa == "fixed"
        and config.wp_map in ("margin_normal", "isotonic")
    ):
        cap = params.get("cap")
        close = params.get("close_filter")
        if config.response == "capped" and cap is None:
            raise ValueError("response='capped' requires a 'cap' entry in params")
        if config.response == "garbage_filtered" and close is None:
            raise ValueError("response='garbage_filtered' requires a 'close_filter' entry in params")
        if config.response == "raw" and (cap is not None or close is not None):
            raise ValueError("response='raw' must not carry a 'cap' or 'close_filter' param")
        return ratings_predictor(
            table,
            wp_map=config.wp_map,
            sigma=params.get("sigma", sigma),
            lam=params.get("lam"),
            cap=cap,
            close_filter=close,
        )
    raise NotImplementedError(f"no engine landed yet for variant {config}")
