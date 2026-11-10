# tools/fit_shot_zone_gates.py
"""Fit the rank sample gates of the shot-zone profiles (BB-D35): each zone rate's split-half r = 0.5 point.

The fitted values are frozen into :mod:`sportsdataverse._common.shot_zone_profiles`
(``PLAYER_MIN_N`` and ``TEAM_MIN_N``) -- never tuned ad hoc. The method is
``tools/fit_rate_gates.py``'s, whose ``fit_m`` / ``gate`` / ``_half_rates`` this reuses:

* **Teams**: a team-season's games in ``game_id`` order, split odd / even and truncated at ``n``
  units of the rate's own denominator (``fit_rate_gates.team_curve``'s grid), for the team's own
  attempts and for its opponents'.
* **Players**: a player's games with his team in ``game_id`` order, split odd / even, whole halves
  (a player's sample cannot be truncated game by game without losing the zone mix), binned on the
  smaller half's ``_n`` (``fit_rate_gates.on_off_curve``'s binning).

``r(n)`` is the correlation of the two half rates (each demeaned within its season); ``M`` is the
fit of ``r(n) = n / (n + M)``; an ``M`` past twice the largest sample is ``inf`` (never ranked). Each
league is fitted on its TRAIN seasons and re-fitted on its later HOLDOUT seasons; a train gate the
holdout puts under a third signal is withheld (:func:`frozen_gates`, the rate is never ranked: the
rule ``fit_rate_gates`` leaves to an owner exception is a fixed rule here). ``--write`` saves
every curve to ``tests/fixtures/bb_shot_zone_gates/`` and the test suite re-runs the fit offline.

Usage::

    uv run python tools/fit_shot_zone_gates.py --league nba --data /tmp/zone_cache --write
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fit_rate_gates import _cached, _corr, _half_rates, fit_m, gate  # noqa: E402

from sportsdataverse._common.shot_zone_profiles import RATES, ZONES  # noqa: E402

#: league -> (train, holdout) seasons by END year, disjoint, the holdout after the train.
SPANS: dict[str, tuple[range, range]] = {
    "nba": (range(2019, 2024), range(2024, 2027)),
    "wnba": (range(2018, 2023), range(2023, 2026)),
    "mbb": (range(2022, 2025), range(2025, 2027)),
    "wbb": (range(2022, 2025), range(2025, 2027)),
}
#: league -> (quantile bins, fewest players in a bin) for the player fit.
PLAYER_BINS = {"nba": (12, 60), "wnba": (10, 40), "mbb": (12, 60), "wbb": (12, 60)}
MIN_TEAMS = 30
MIN_HALF_N = 5  # a player half under this many units of the rate's denominator is left out
FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "bb_shot_zone_gates"


def _rows(league: str, season: int) -> pl.DataFrame:
    """One league-season's shot rows, through the producers' own adapters."""
    if league in ("nba", "wnba"):
        from sportsdataverse.nba.nba_shot_zone_profiles import _rows as rows

        return rows(season, "00" if league == "nba" else "10")
    from sportsdataverse.mbb.mbb_shot_zone_profiles import _rows as rows

    return rows(season, "mens" if league == "mbb" else "womens")


def _game_counts(r: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    """Per ``keys`` + game: located attempts and, per zone, attempts / makes / assisted / known."""
    made = pl.col("made") == True  # noqa: E712
    cols = [pl.col("zone").is_not_null().sum().alias("loc")]
    for z in ZONES:
        inz = pl.col("zone") == z
        cols += [
            inz.sum().alias(f"{z}_fga"),
            (inz & made).sum().alias(f"{z}_fgm"),
            (inz & made & (pl.col("assisted") == True)).sum().alias(f"{z}_ast"),  # noqa: E712
            (inz & made & pl.col("assisted").is_not_null()).sum().alias(f"{z}_known"),
        ]
    return (
        r.group_by("season", *keys, "game_id")
        .agg(cols)
        .with_columns(pl.exclude("season", *keys, "game_id").cast(pl.Float64))
    )


def _parts(z: str, rate: str) -> tuple[str, str]:
    """(numerator, denominator = unit) of a zone rate."""
    return {
        "fg_pct": (f"{z}_fgm", f"{z}_fga"),
        "freq": (f"{z}_fga", "loc"),
        "ast_pct": (f"{z}_ast", f"{z}_known"),
    }[rate]


def team_games(league: str, seasons: range, data: Path | None) -> pl.DataFrame:
    """Per (season, team, game): the team's own counts and its opponents' (``opp_``)."""
    r = pl.concat([_cached(data, f"{league}_zone_rows_{s}.parquet", lambda s=s: _rows(league, s)) for s in seasons])
    own = _game_counts(r.filter(pl.col("team_id").is_not_null()), ["team_id"])
    opp = _game_counts(
        r.filter(pl.col("opp_team_id").is_not_null()).with_columns(pl.col("opp_team_id").alias("team_id")), ["team_id"]
    )
    opp = opp.rename({c: f"opp_{c}" for c in opp.columns if c not in ("season", "team_id", "game_id")})
    t = own.join(opp, on=["season", "team_id", "game_id"], how="full", coalesce=True).fill_null(0)
    t = t.sort("season", "team_id", "game_id")
    idx = pl.int_range(pl.len()).over(["season", "team_id"])
    return t.with_columns((idx % 2).alias("half"), (idx // 2).alias("ord"))


def team_curve(t: pl.DataFrame, num: str, den: str) -> list[tuple[float, float, int]]:
    """``(n, r, teams)`` over ``fit_rate_gates.team_curve``'s log grid of ``n``."""
    totals = t.group_by("season", "team_id").agg(pl.col(den).sum())[den].to_numpy() / 2
    lo, hi = np.quantile(totals, 0.15), np.quantile(totals, 0.97)
    pts = []
    for n in np.unique(np.geomspace(max(lo / 4, 1.0), hi * 0.9, 14)):
        w = _half_rates(t, num, den, den, float(n))
        if w.height >= MIN_TEAMS:
            pts.append((float(((w["u_0"] + w["u_1"]) / 2).mean()), _corr(w["a"], w["b"]), w.height))
    return pts


def player_halves(league: str, seasons: range, data: Path | None) -> pl.DataFrame:
    """Per (season, team, player, half): the zone counts; halves are his odd / even games."""
    r = pl.concat([_cached(data, f"{league}_zone_rows_{s}.parquet", lambda s=s: _rows(league, s)) for s in seasons])
    g = _game_counts(
        r.filter(pl.col("player_id").is_not_null() & pl.col("team_id").is_not_null()), ["team_id", "player_id"]
    )
    g = g.sort("season", "team_id", "player_id", "game_id").with_columns(
        (pl.int_range(pl.len()).over(["season", "team_id", "player_id"]) % 2).alias("half")
    )
    return g.group_by("season", "team_id", "player_id", "half").agg(pl.exclude("game_id").sum())


def player_curve(h: pl.DataFrame, num: str, den: str, *, bins: int, min_bin: int) -> list[tuple[float, float, int]]:
    """``(median half _n, r, players)`` per quantile bin of the smaller half's ``_n``."""
    v = h.with_columns((pl.col(num) / pl.col(den)).alias("v"), pl.col(den).alias("n")).filter(pl.col("n") > 0)
    w = v.pivot(on="half", index=["season", "team_id", "player_id"], values=["v", "n"]).drop_nulls()
    w = w.with_columns(
        pl.min_horizontal("n_0", "n_1").alias("g"),
        (pl.col("v_0") - pl.col("v_0").mean().over("season")).alias("a"),
        (pl.col("v_1") - pl.col("v_1").mean().over("season")).alias("b"),
    ).filter(pl.col("g") >= MIN_HALF_N)
    if w.height == 0:
        return []
    edges = np.unique(np.quantile(w["g"].to_numpy(), np.linspace(0, 1, bins + 1)))
    idx = np.clip(np.searchsorted(edges, w["g"].to_numpy(), side="right") - 1, 0, len(edges) - 2)
    w = w.with_columns(pl.Series("bin", idx))
    return sorted(
        (float(p["g"].median()), _corr(p["a"], p["b"]), p.height) for _, p in w.group_by("bin") if p.height >= min_bin
    )


def _row(kind: str, league: str, rate: str, span: str, seasons: range, pts: list) -> dict:
    """One fits-table row: the curve, its ``M`` and the span it was measured on."""
    m = fit_m(pts)
    print(f"{league} {kind} {rate} {span}: M={m:.1f} ({len(pts)} points)", flush=True)
    return {
        "kind": kind,
        "league": league,
        "rate": rate,
        "span": span,
        "seasons": f"{seasons.start}-{seasons.stop - 1}",
        "m": m,
        "curve": ";".join(f"{n!r}:{r!r}:{k}" for n, r, k in pts),
    }


def fit_league(league: str, data: Path | None = None) -> pl.DataFrame:
    """Every curve and ``M`` of a league: one row per (kind, rate, span)."""
    rows = []
    bins, min_bin = PLAYER_BINS[league]
    for span, seasons in zip(("train", "holdout"), SPANS[league], strict=True):
        t = team_games(league, seasons, data)
        h = player_halves(league, seasons, data)
        for z in ZONES:
            for rate in RATES:
                num, den = _parts(z, rate)
                for p in ("", "opp_"):
                    rows.append(_row("team", league, f"{p}{z}_{rate}", span, seasons, team_curve(t, p + num, p + den)))
                pts = player_curve(h, num, den, bins=bins, min_bin=min_bin)
                rows.append(_row("player", league, f"{z}_{rate}", span, seasons, pts))
    return pl.DataFrame(rows)


#: A train gate is withheld (the rate never ranked) when the holdout seasons put it under this
#: reliability: their own ``M`` is infinite, or ``gate / (gate + M_holdout)`` is below a third. A
#: holdout curve too short to fit (``nan``) keeps the gate.
MIN_HOLDOUT_R = 1 / 3


def frozen_gates(fits: pl.DataFrame, kind: str) -> dict[str, int]:
    """The ``PLAYER_MIN_N`` / ``TEAM_MIN_N`` entries of one league from its fits: rate -> gate."""
    out = {}
    for r in fits.filter((pl.col("span") == "train") & (pl.col("kind") == kind)).iter_rows(named=True):
        m = r["m"]
        if m is None or not math.isfinite(m):
            continue
        g = gate(m)
        hold = fits.filter((pl.col("span") == "holdout") & (pl.col("kind") == kind) & (pl.col("rate") == r["rate"]))
        mh = hold["m"].item() if hold.height else float("nan")
        if mh is not None and not math.isnan(mh) and (math.isinf(mh) or g / (g + mh) < MIN_HOLDOUT_R):
            continue
        out[r["rate"]] = g
    return out


def main(argv: list[str] | None = None) -> int:
    """CLI: fit a league, print each gate and its holdout reliability, optionally write the curves."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--league", choices=sorted(SPANS), required=True)
    ap.add_argument("--data", type=Path, default=None, help="cache the shot rows here")
    ap.add_argument("--write", action="store_true", help=f"save the curves to {FIXTURES}")
    args = ap.parse_args(argv)
    fits = fit_league(args.league, args.data)
    for r in fits.filter(pl.col("span") == "train").iter_rows(named=True):
        hold = fits.filter(
            (pl.col("span") == "holdout") & (pl.col("kind") == r["kind"]) & (pl.col("rate") == r["rate"])
        )
        mh = hold["m"].item()
        g = gate(r["m"]) if math.isfinite(r["m"]) else None
        rel = g / (g + mh) if g and math.isfinite(mh) else float("nan")
        print(f"{r['kind']:6} {r['rate']:28} M={r['m']:9.1f} gate={g} holdout M={mh:9.1f} holdout r at gate={rel:.2f}")
    for kind in ("player", "team"):
        print(f"{kind}: {frozen_gates(fits, kind)}")
    if args.write:
        FIXTURES.mkdir(parents=True, exist_ok=True)
        fits.write_csv(FIXTURES / f"{args.league}_fits.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
