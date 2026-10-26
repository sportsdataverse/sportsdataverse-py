"""Fit the cricket win-probability model artifacts + format constants (T7.3).

Reads the Cricsheet per-legal-ball corpus (built by ``tools/models/cricket/fetch_cricsheet.py``)
and fits, per format (T20 / ODI), the three things the shipped win-probability
model needs, plus the committed calibration fixture:

1. **Resource surface** ``resource(overs_left, wickets_left)`` in ``[0, 1]`` — the
   mean fraction of the innings total still to be scored from a state, smoothed
   monotone non-decreasing in ``overs_left`` (fixed wickets) AND ``wickets_left``
   (fixed overs) via cyclic isotonic regression. Keyed on
   ``(fmt, overs_left, wickets_left)`` — the resource at a given ``overs_left``
   differs by format, so format is part of the key (a deliberate correction to
   the fmt-less join in the original plan sketch), and on ``regime`` -- the
   pooled 2002-2026 grid (``regime = "pooled"``) plus one grid per ODI
   fielding-restriction regime (``cricket_model_constants.ODI_REGIMES``, from the
   corpus ``match_date``) and the single T20 regime, each with the per-cell
   ``support`` (innings-states) the runtime's thin-grid fallback reads
   (``cricket_win_prob.MIN_REGIME_CELLS`` / ``MIN_CELL_SUPPORT``). Written to
   ``sportsdataverse/cricket/models/cricket_resource_surface.parquet``.

2. **Format constants** ``par_score`` (mean first-innings total) and
   ``sigma_set`` / ``sigma_chase`` (probit scale that minimises training log-loss
   for the parametric raw win-prob ``Phi((proj_final - benchmark) / sigma)``),
   printed for pasting into ``FORMAT_TABLE`` (pooled) and ``REGIME_CONSTANTS``
   (per ODI regime with 100+ training matches and a thick grid -- the runtime's
   ``_usable_regimes`` rule -- fitted on that regime's matches projected through
   its own grid; rules/cricket.yaml#cricket-2012-odi-two-new-balls-four-out).
   The calibrator (3) is fitted on the raw probabilities the runtime produces,
   regime sigma where a regime is applied and pooled sigma elsewhere.

3. **Isotonic calibration surface** per ``(fmt, phase)`` mapping the parametric
   raw win-prob to the empirical win rate — this is what makes the shipped
   ``win_prob`` calibrated (a scalar probit alone lands ~0.07 per-decile; the
   monotone recalibration brings it under the 0.05 gate). Written to
   ``sportsdataverse/cricket/models/cricket_winprob_calibration.parquet`` as a
   101-point lookup per group.

Validation split is a **random 15% match-level holdout** (seed fixed) — the
correct instrument for a *calibration* gate, since a single-season holdout would
conflate calibration error with genuine temporal scoring drift (T20/ODI totals
have risen over 2002-2026). No match appears in both sides (true holdout, no
leakage). The held-out states + match-outcome label are written to
``tests/fixtures/league_ports/cricket_holdout.parquet``; the script also prints
the holdout Brier + per-decile calibration so the Phase-2 gate floor is set from
the observed value.

**Regenerate together, always.** A single run of this script writes all four
coupled artifacts from the SAME train/holdout split — the two committed model
artifacts (``cricket_resource_surface.parquet``, ``cricket_winprob_calibration.parquet``)
and the two committed holdout fixtures (``cricket_holdout.parquet``,
``cricket_wpa_holdout.parquet``). Never regenerate one without the others: the
holdout is the complement of the training matches, so a stale mix would let the
holdout silently overlap training (leakage).

Run::

    uv run python tools/models/league_ports/fit_cricket_resource_surface.py
"""

from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import norm
from sklearn.isotonic import IsotonicRegression

from sportsdataverse.cricket.cricket_model_constants import ODI_REGIMES, POOLED_REGIME, T20_REGIME
from sportsdataverse.cricket.cricket_win_prob import _usable_regimes

ROOT = Path(__file__).resolve().parents[3]
CORPUS = ROOT / "dev/cricket/.cache/deliveries_all.parquet"
SURFACE_OUT = ROOT / "sportsdataverse/cricket/models/cricket_resource_surface.parquet"
CALIB_OUT = ROOT / "sportsdataverse/cricket/models/cricket_winprob_calibration.parquet"
HOLDOUT_OUT = ROOT / "tests/fixtures/league_ports/cricket_holdout.parquet"
WPA_HOLDOUT_OUT = ROOT / "tests/fixtures/league_ports/cricket_wpa_holdout.parquet"

OVER_LIMIT = {"t20": 20, "odi": 50}
HOLDOUT_FRACTION = 0.15
HOLDOUT_SEED = 7
CALIB_GRID = 101  # points in the [0,1] calibration lookup per (fmt, phase)
WPA_HOLDOUT_MATCHES = 200  # committed WPA-reconciliation fixture size (per-over + terminal states)


# --------------------------------------------------------------------------- #
# resource surface
# --------------------------------------------------------------------------- #
def _complete_first_innings(df: pl.DataFrame, over_limit: int) -> pl.DataFrame:
    balls_total = over_limit * 6
    return df.filter(
        (pl.col("innings_number") == 1) & ((pl.col("wickets") >= 10) | (pl.col("innings_final_balls") >= balls_total))
    )


def _empirical_grid(states: pl.DataFrame, over_limit: int) -> tuple[np.ndarray, np.ndarray]:
    """``(resource, support)`` grids over ``(overs_left, wickets_left)``: cell mean and cell count."""
    s = states.with_columns(
        overs_left=((pl.col("balls_total") - pl.col("legal_balls")) // 6).cast(pl.Int64),
        wickets_left=(10 - pl.col("wickets")).cast(pl.Int64),
        resource=((pl.col("innings_final_runs") - pl.col("runs")) / pl.col("innings_final_runs")).clip(0.0, 1.0),
    ).filter((pl.col("overs_left") >= 0) & (pl.col("wickets_left") >= 0))
    agg = s.group_by(["overs_left", "wickets_left"]).agg(
        pl.col("resource").mean().alias("resource"), pl.len().alias("support")
    )
    grid = np.full((over_limit + 1, 11), np.nan)
    support = np.zeros((over_limit + 1, 11), dtype=np.int64)
    for row in agg.iter_rows(named=True):
        o, w = row["overs_left"], row["wickets_left"]
        if 0 <= o <= over_limit and 0 <= w <= 10:
            grid[o, w] = row["resource"]
            support[o, w] = row["support"]
    return grid, support


def _regime_expr() -> pl.Expr:
    """The corpus row's regime from ``fmt`` and ``match_date`` (``cricket_model_constants.ODI_REGIMES``)."""
    odi = pl.lit(ODI_REGIMES[0][0])
    for name, start in ODI_REGIMES[1:]:
        odi = pl.when(pl.col("match_date") >= pl.lit(start)).then(pl.lit(name)).otherwise(odi)
    return pl.when(pl.col("fmt") == T20_REGIME).then(pl.lit(T20_REGIME)).otherwise(odi)


def _cyclic_isotonic(grid: np.ndarray, over_limit: int, n_iter: int = 60) -> np.ndarray:
    g = grid.copy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)  # all-NaN rows -> filled globally next
        row_mean = np.nanmean(g, axis=1, keepdims=True)
    g = np.where(np.isnan(g), row_mean, g)
    g = np.where(np.isnan(g), np.nanmean(g), g)
    ox = np.arange(over_limit + 1, dtype=float)
    wx = np.arange(11, dtype=float)
    prev = None
    for _ in range(n_iter):
        for w in range(11):
            g[:, w] = IsotonicRegression(increasing=True, out_of_bounds="clip").fit_transform(ox, g[:, w])
        for o in range(over_limit + 1):
            g[o, :] = IsotonicRegression(increasing=True, out_of_bounds="clip").fit_transform(wx, g[o, :])
        if prev is not None and np.max(np.abs(g - prev)) < 1e-6:
            break
        prev = g.copy()
    g[0, :] = 0.0  # no balls left -> nothing remaining
    g[:, 0] = 0.0  # all out -> nothing remaining
    g[over_limit, 10] = 1.0
    return np.clip(g, 0.0, 1.0)


def _surface_frame(grid: np.ndarray, over_limit: int, fmt: str, regime: str, support: np.ndarray) -> pl.DataFrame:
    rows = [
        {
            "fmt": fmt,
            "regime": regime,
            "overs_left": o,
            "wickets_left": w,
            "resource": float(grid[o, w]),
            "support": int(support[o, w]),
        }
        for o in range(over_limit + 1)
        for w in range(11)
    ]
    return pl.DataFrame(rows).with_columns(
        pl.col("overs_left").cast(pl.Int64),
        pl.col("wickets_left").cast(pl.Int64),
        pl.col("resource").cast(pl.Float64),
        pl.col("support").cast(pl.Int64),
    )


# --------------------------------------------------------------------------- #
# projection + parametric raw win-prob
# --------------------------------------------------------------------------- #
def _project(
    states: pl.DataFrame,
    surface: pl.DataFrame,
    par: dict[str, float],
    regime_par: dict[str, float] | None = None,
    usable: set[str] | None = None,
) -> pl.DataFrame:
    """Projection as the runtime does it: a usable regime's own grid and par, else the pooled ones.

    ``use_regime`` marks the rows whose regime constants apply (ODI regimes in ``regime_par``).
    """
    regime_par = regime_par or {}
    usable = usable or set()
    par_expr = pl.col("fmt").replace_strict(par, default=None).cast(pl.Float64)
    for regime, value in regime_par.items():
        if regime in usable:
            par_expr = pl.when(pl.col("regime") == regime).then(pl.lit(value)).otherwise(par_expr)
    grid = surface.rename({"regime": "surface_regime"}).select(
        "fmt", "surface_regime", "overs_left", "wickets_left", "resource"
    )
    s = (
        states.with_columns(
            overs_left=((pl.col("balls_total") - pl.col("legal_balls")) // 6).cast(pl.Int64),
            wickets_left=(10 - pl.col("wickets")).cast(pl.Int64),
            surface_regime=pl.when(pl.col("regime").is_in(list(usable)) if usable else pl.lit(False))
            .then(pl.col("regime"))
            .otherwise(pl.lit(POOLED_REGIME)),
            use_regime=pl.col("regime").is_in([r for r in regime_par if r in usable] or [""]),
        )
        .join(grid, on=["fmt", "surface_regime", "overs_left", "wickets_left"], how="left")
        .drop("surface_regime")
    )
    s = s.with_columns(resources_left=pl.col("resource").fill_null(0.0))
    s = s.with_columns(proj_final=(pl.col("runs") + pl.col("resources_left") * par_expr).cast(pl.Float64))
    benchmark = pl.when(pl.col("innings_number") == 2).then(pl.col("target").cast(pl.Float64)).otherwise(par_expr)
    return s.with_columns(z=(pl.col("proj_final") - benchmark)).filter(pl.col("z").is_not_null())


def _fit_sigma(z: np.ndarray, y: np.ndarray, sigma_hi: float) -> float:
    """Probit scale minimising training log-loss of ``Phi(z / sigma)``."""
    best_sigma, best_ll = sigma_hi, np.inf
    for sigma in np.arange(5.0, sigma_hi + 0.1, 1.0):
        p = np.clip(norm.cdf(z / sigma), 1e-6, 1 - 1e-6)
        ll = float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))
        if ll < best_ll:
            best_ll, best_sigma = ll, float(sigma)
    return best_sigma


def _phase(innings_number: pl.Expr) -> pl.Expr:
    return pl.when(innings_number == 2).then(pl.lit("chase")).otherwise(pl.lit("set"))


def maxcalib(y: np.ndarray, p: np.ndarray, nb: int = 10) -> float:
    b = np.clip((p * nb).astype(int), 0, nb - 1)
    mx = 0.0
    for k in range(nb):
        m = b == k
        if m.sum() > 0:
            mx = max(mx, abs(float(p[m].mean()) - float(y[m].mean())))
    return mx


def _holdout_states(states: pl.DataFrame) -> pl.DataFrame:
    return states.filter(pl.col("legal_balls") % 6 == 0).select(
        event_id=pl.col("match_id"),
        innings_number=pl.col("innings_number"),
        batting_team_id=pl.col("batting_team"),
        runs=pl.col("runs"),
        wickets=pl.col("wickets"),
        balls_bowled=pl.col("legal_balls"),
        balls_total=pl.col("balls_total"),
        target=pl.col("target"),
        fmt=pl.col("fmt"),
        match_date=pl.col("match_date"),
        chasing_won=pl.col("batting_team_won"),
    )


def _wpa_holdout_states(states: pl.DataFrame, match_ids: list[str]) -> pl.DataFrame:
    """Per-over PLUS terminal (last legal ball per innings) states for WPA reconciliation.

    Includes ``innings_final_runs`` (for E[runs] calibration) and the terminal
    ball of each innings so the win-prob trajectory reaches the pinned outcome.
    """
    hold = states.filter(pl.col("match_id").is_in(match_ids)).filter(
        (pl.col("legal_balls") % 6 == 0) | (pl.col("legal_balls") == pl.col("innings_final_balls"))
    )
    return hold.select(
        event_id=pl.col("match_id"),
        innings_number=pl.col("innings_number"),
        batting_team_id=pl.col("batting_team"),
        runs=pl.col("runs"),
        wickets=pl.col("wickets"),
        balls_bowled=pl.col("legal_balls"),
        balls_total=pl.col("balls_total"),
        target=pl.col("target"),
        fmt=pl.col("fmt"),
        match_date=pl.col("match_date"),
        chasing_won=pl.col("batting_team_won"),
        innings_final_runs=pl.col("innings_final_runs"),
    )


def main() -> None:
    df = pl.read_parquet(CORPUS).filter(pl.col("batting_team_won").is_not_null())
    if "match_date" not in df.columns:
        raise SystemExit(
            "corpus has no match_date: re-run tools/models/cricket/fetch_cricsheet.py (it now keeps info.dates[0])"
        )
    df = df.with_columns(regime=_regime_expr())

    # random match-level holdout (no match in both sides)
    mids = df.select("match_id").unique().sort("match_id").to_series().to_list()
    rng = np.random.default_rng(HOLDOUT_SEED)
    test_ids = set(rng.choice(mids, size=int(HOLDOUT_FRACTION * len(mids)), replace=False).tolist())
    train_df = df.filter(~pl.col("match_id").is_in(list(test_ids)))
    test_df = df.filter(pl.col("match_id").is_in(list(test_ids)))

    surfaces: list[pl.DataFrame] = []
    par: dict[str, float] = {}
    sigmas: dict[str, tuple[float, float]] = {}

    # 1) surface + par + sigma per format (train): the pooled grid, then one grid per regime
    #    with its cell support (the runtime falls back to the pooled grid when a regime is thin)
    for fmt, over_limit in OVER_LIMIT.items():
        ftrain = train_df.filter(pl.col("fmt") == fmt)
        grid, support = _empirical_grid(_complete_first_innings(ftrain, over_limit), over_limit)
        surfaces.append(_surface_frame(_cyclic_isotonic(grid, over_limit), over_limit, fmt, POOLED_REGIME, support))
        for regime in sorted(ftrain["regime"].unique().to_list()):
            rtrain = ftrain.filter(pl.col("regime") == regime)
            grid, support = _empirical_grid(_complete_first_innings(rtrain, over_limit), over_limit)
            surfaces.append(_surface_frame(_cyclic_isotonic(grid, over_limit), over_limit, fmt, regime, support))
        par[fmt] = float(
            ftrain.filter(pl.col("innings_number") == 1)
            .group_by("match_id")
            .agg(pl.col("innings_final_runs").first())["innings_final_runs"]
            .mean()
        )
    surface = pl.concat(surfaces, how="vertical")

    # pooled sigma per format, on the pooled projection (what a fallen-back row gets at runtime)
    proj_pooled = _project(train_df, surface, par).with_columns(phase=_phase(pl.col("innings_number")))
    for fmt in OVER_LIMIT:
        sigma_hi = 70.0 if fmt == "t20" else 120.0
        s_set = proj_pooled.filter((pl.col("fmt") == fmt) & (pl.col("phase") == "set"))
        s_ch = proj_pooled.filter((pl.col("fmt") == fmt) & (pl.col("phase") == "chase"))
        sigmas[fmt] = (
            _fit_sigma(s_set["z"].to_numpy(), s_set["batting_team_won"].to_numpy().astype(float), sigma_hi),
            _fit_sigma(s_ch["z"].to_numpy(), s_ch["batting_team_won"].to_numpy().astype(float), sigma_hi),
        )

    # 1b) per-regime par (100+ training matches), then the runtime's whole-regime rule (thick grid AND
    #     constants) decides which regimes are applied; their sigma is fitted on their own projection
    regime_par: dict[str, float] = {}
    for regime in sorted(r for r in train_df["regime"].unique().to_list() if r != T20_REGIME):
        rtrain = train_df.filter(pl.col("regime") == regime)
        if rtrain["match_id"].n_unique() < 100:
            continue
        regime_par[regime] = round(
            float(
                rtrain.filter(pl.col("innings_number") == 1)
                .group_by("match_id")
                .agg(pl.col("innings_final_runs").first())["innings_final_runs"]
                .mean()
            ),
            1,
        )
    usable = _usable_regimes(surface, {r: {} for r in regime_par})
    regime_par = {r: v for r, v in regime_par.items() if r in usable}
    proj_train = _project(train_df, surface, par, regime_par, usable).with_columns(
        phase=_phase(pl.col("innings_number"))
    )
    regime_consts: dict[str, dict[str, float]] = {}
    for regime, r_par in regime_par.items():
        rtrain = proj_train.filter(pl.col("regime") == regime)
        r_set = rtrain.filter(pl.col("phase") == "set")
        r_ch = rtrain.filter(pl.col("phase") == "chase")
        regime_consts[regime] = {
            "par_score": r_par,
            "sigma_set": _fit_sigma(r_set["z"].to_numpy(), r_set["batting_team_won"].to_numpy().astype(float), 120.0),
            "sigma_chase": _fit_sigma(r_ch["z"].to_numpy(), r_ch["batting_team_won"].to_numpy().astype(float), 120.0),
        }

    # 2) parametric raw win-prob exactly as the runtime computes it (regime sigma where a regime is
    #    applied, pooled sigma elsewhere) + per-(fmt, phase) isotonic calibrator on it
    def raw_prob(frame: pl.DataFrame) -> np.ndarray:
        fa = frame["fmt"].to_numpy()
        ph = frame["phase"].to_numpy()
        rg = frame["regime"].to_numpy()
        ur = frame["use_regime"].to_numpy()
        za = frame["z"].to_numpy()
        out = np.zeros(len(za))
        for fmt, (sig_set, sig_ch) in sigmas.items():
            m_set = (fa == fmt) & (ph == "set") & ~ur
            m_ch = (fa == fmt) & (ph == "chase") & ~ur
            out[m_set] = norm.cdf(za[m_set] / sig_set)
            out[m_ch] = norm.cdf(za[m_ch] / sig_ch)
        for regime, consts in regime_consts.items():
            m_set = ur & (rg == regime) & (ph == "set")
            m_ch = ur & (rg == regime) & (ph == "chase")
            out[m_set] = norm.cdf(za[m_set] / consts["sigma_set"])
            out[m_ch] = norm.cdf(za[m_ch] / consts["sigma_chase"])
        return out

    ptr = raw_prob(proj_train)
    ytr = proj_train["batting_team_won"].to_numpy().astype(float)
    fa_tr, ph_tr = proj_train["fmt"].to_numpy(), proj_train["phase"].to_numpy()
    grid_x = np.linspace(0.0, 1.0, CALIB_GRID)
    calib_rows: list[dict] = []
    calibrators: dict[tuple[str, str], IsotonicRegression] = {}
    for fmt in OVER_LIMIT:
        for phase in ("set", "chase"):
            m = (fa_tr == fmt) & (ph_tr == phase)
            iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0).fit(ptr[m], ytr[m])
            calibrators[(fmt, phase)] = iso
            for x, yv in zip(grid_x, iso.predict(grid_x)):
                calib_rows.append({"fmt": fmt, "phase": phase, "x": float(x), "y": float(yv)})
    calib = pl.DataFrame(calib_rows).with_columns(pl.col("x").cast(pl.Float64), pl.col("y").cast(pl.Float64))

    # 3) evaluate on holdout (report + gate floor)
    proj_test = _project(test_df, surface, par, regime_par, usable).with_columns(phase=_phase(pl.col("innings_number")))
    proj_test = proj_test.filter(pl.col("legal_balls") % 6 == 0)  # over boundaries, mirrors the fixture
    pte = raw_prob(proj_test)
    fa_te, ph_te = proj_test["fmt"].to_numpy(), proj_test["phase"].to_numpy()
    cal = np.zeros(len(pte))
    for (fmt, phase), iso in calibrators.items():
        m = (fa_te == fmt) & (ph_te == phase)
        cal[m] = iso.predict(pte[m])
    yte = proj_test["batting_team_won"].to_numpy().astype(float)
    brier = float(np.mean((cal - yte) ** 2))
    mc = maxcalib(yte, cal)

    # write artifacts
    SURFACE_OUT.parent.mkdir(parents=True, exist_ok=True)
    surface.write_parquet(SURFACE_OUT)
    calib.write_parquet(CALIB_OUT)
    HOLDOUT_OUT.parent.mkdir(parents=True, exist_ok=True)
    hold_fixture = _holdout_states(test_df)
    hold_fixture.write_parquet(HOLDOUT_OUT)
    wpa_ids = sorted(test_df["match_id"].unique().to_list())[:WPA_HOLDOUT_MATCHES]
    wpa_fixture = _wpa_holdout_states(test_df, wpa_ids)
    wpa_fixture.write_parquet(WPA_HOLDOUT_OUT)

    # report
    print("=== fitted cricket format constants (paste into FORMAT_TABLE) ===")
    for fmt in OVER_LIMIT:
        sig_set, sig_ch = sigmas[fmt]
        print(
            f'  "{fmt}": FormatConstants(name="{fmt}", balls_total={OVER_LIMIT[fmt] * 6}, max_wickets=10, '
            f"par_score={par[fmt]:.1f}, sigma_set={sig_set:.1f}, sigma_chase={sig_ch:.1f}),"
        )
    print("\n=== fitted ODI regime constants (paste into REGIME_CONSTANTS) ===")
    print(f"  applied whole (thick grid + 100 matches): {sorted(usable)}; the rest fall back to pooled")
    for regime, consts in regime_consts.items():
        print(f'    "{regime}": {consts},')
    print(f"\nwrote {SURFACE_OUT}")
    print(f"wrote {CALIB_OUT}")
    print(f"wrote {HOLDOUT_OUT} ({hold_fixture.height} states, {hold_fixture['event_id'].n_unique()} matches)")
    print(f"wrote {WPA_HOLDOUT_OUT} ({wpa_fixture.height} states, {wpa_fixture['event_id'].n_unique()} matches)")
    print("\n=== HOLDOUT gate metrics (calibrated win_prob) ===")
    print(f"  train_matches={train_df['match_id'].n_unique()} holdout_matches={test_df['match_id'].n_unique()}")
    print(f"  Brier={brier:.4f}  (no-skill 0.5 = 0.2500)")
    print(f"  max per-decile calibration={mc:.4f}  (gate <= 0.05)")


if __name__ == "__main__":
    main()
