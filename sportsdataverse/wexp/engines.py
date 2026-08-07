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
from sportsdataverse.wexp.backtest import WeekPredictor, elo_predictor, normalize_walk_weeks
from sportsdataverse.wexp.elo import EloConfig
from sportsdataverse.wexp.store import VintageStore
from sportsdataverse.wexp.variants import VariantConfig

__all__ = [
    "GSConfig",
    "build_predictor",
    "cfb_continuity_shifts",
    "glickman_stern_predictor",
    "ratings_predictor",
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
}


def ridge_margin_vintages(
    oracle: pl.DataFrame, *, lam: float, resp_col: str = "home_margin", cap: Optional[float] = None
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
                homeflag=pl.when(pl.col("neutral_site") == True)  # noqa: E712
                .then(pl.lit(""))
                .otherwise(pl.col("home_team")),
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
        fit = games.filter((pl.col("season") == season) & (pl.col("week") < week))
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
            ).select(list(_VINTAGE_SCHEMA))
        )
    if not frames:
        return pl.DataFrame(schema=_VINTAGE_SCHEMA)
    return pl.concat(frames, how="vertical")


def ratings_predictor(
    table: str,
    *,
    wp_map: str = "margin_normal",
    sigma: float = 13.45,
    iso_min_fit: int = 100,
    lam: Optional[float] = None,
    cap: Optional[float] = None,
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
        g = store.join_asof(games, table, on={"home_team": "team_id"}, prefix="rt_home_")
        g = store.join_asof(g, table, on={"away_team": "team_id"}, prefix="rt_away_")
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


def glickman_stern_predictor(config: GSConfig = GSConfig()) -> WeekPredictor:
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

    Returns:
        A predictor callable for :func:`~sportsdataverse.wexp.backtest.run_backtest`.

    Example:
        Quick start::

            from sportsdataverse.wexp.backtest import run_backtest
            from sportsdataverse.wexp.engines import GSConfig, glickman_stern_predictor
            probs, rows = run_backtest(oracle, glickman_stern_predictor(GSConfig()), model_id="gs")
    """

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
        x = np.zeros(n)
        p_cov = np.eye(n) * config.init_sd**2
        cur: Optional[tuple[int, int]] = None

        def advance(season: int, week: int) -> None:
            nonlocal x, p_cov, cur
            if cur is not None:
                s0, w0 = cur
                if season > s0:
                    for _ in range(season - s0):
                        x = config.season_shrink * x
                        p_cov = config.season_shrink**2 * p_cov + config.sigma_s**2 * np.eye(n)
                    drift = max(week - 1, 0)
                else:
                    drift = week - w0
                p_cov = p_cov + config.sigma_w**2 * drift * np.eye(n)
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


def build_predictor(
    config: VariantConfig,
    *,
    table: str = "ridge",
    sigma: float = 13.45,
    season_priors: Optional[pl.DataFrame] = None,
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
        shrink = 0.0 if config.prior == "flat" else params.get("season_shrink", 0.82)
        return glickman_stern_predictor(
            GSConfig(
                sigma_w=params.get("sigma_w", 0.9),
                sigma_s=params.get("sigma_s", 2.35),
                season_shrink=shrink,
                sigma_y=params.get("sigma_y", 13.45),
                hfa=params.get("hfa", 2.3),
                init_sd=params.get("init_sd", 5.0),
            )
        )
    if (
        config.core == "elo_margin"
        and config.wp_map == "elo_logistic"
        and config.hfa == "fixed"
        and config.prior in ("flat", "carryover", "carryover_continuity")  # market_open_informed: no engine yet
    ):
        if config.prior == "carryover_continuity" and season_priors is None:
            raise ValueError("prior='carryover_continuity' requires a season_priors table (cfb_continuity_shifts)")
        carryover = 0.0 if config.prior == "flat" else params.get("carryover", 0.67)
        return elo_predictor(
            EloConfig(
                k=params.get("k", 20.0),
                z=params.get("z", 400.0),
                hfa=params.get("hfa", 65.0),
                carryover=carryover,
            ),
            season_priors=season_priors if config.prior == "carryover_continuity" else None,
        )
    if (
        config.core == "ridge_epa"
        and config.response in ("raw", "capped")
        and config.opponent_adjust == "ridge"
        and config.prior == "flat"
        and config.hfa == "fixed"
        and config.wp_map in ("margin_normal", "isotonic")
    ):
        cap = params.get("cap")
        if config.response == "capped" and cap is None:
            raise ValueError("response='capped' requires a 'cap' entry in params")
        if config.response == "raw" and cap is not None:
            raise ValueError("response='raw' must not carry a 'cap' param")
        return ratings_predictor(
            table, wp_map=config.wp_map, sigma=params.get("sigma", sigma), lam=params.get("lam"), cap=cap
        )
    raise NotImplementedError(f"no engine landed yet for variant {config}")
