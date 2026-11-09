# tools/fit_rate_gates.py
"""Fit the rank sample gates of the basketball season tables: each rate's split-half r = 0.5 point.

The fitted values are frozen into :mod:`sportsdataverse._common.rate_gates` (``TEAM_MIN_N`` and
``ON_OFF_MIN_N``) with a comment citing this script -- never tuned ad hoc. The study behind it
(both units compared, the published conventions) is summarised in sportsdataverse-py #1126.

**Method** (Blackport's truncated split-half, at game granularity). A team-season's regular-season
games are ordered by date and split odd / even. For a sample of ``n`` units of the rate's own
denominator, each half keeps its games in order until the running count first reaches ``n``; a
team counts only if both halves do. ``r(n)`` is the correlation of the two half rates across
teams (each half demeaned within its season): the reliability of the rate measured on ``n`` units.
``M`` is the least-squares fit of ``r(n) = n / (n + M)`` over a log grid of ``n`` (weighted by the
teams at each point), so ``r(M) = 0.5``; an ``M`` past twice the largest sample on the curve is
``inf`` (never ranked), not an extrapolation. The gate is ``M`` rounded UP to two significant
figures (:func:`gate`), so a ranked row is at least half signal on the train seasons; on the
holdout seasons the median rate is about half signal at its gate (single rates scatter).

* **Team rates** (``TEAM_MIN_N``): the ESPN team box scores (``load_{lg}_team_boxscore``), with the
  box possession estimate ``FGA + 0.44 FTA - OREB + TOV`` averaged over both teams. A rating's unit
  is that estimate; the table's ``net_rtg_n`` counts both sides, so its curve is scaled by 2. Pace
  is fitted per game; the table counts the clock in regulation games, so an overtime game counts a
  little over 1 there (about 0.6% more clock in an NBA season: the gate is that much lenient).
* **Player on/off** (``ON_OFF_MIN_N``): the BB-D6 game impact box (``{nba,mbb}_game_player_impact``);
  the team's game totals are its on-court sums over five players. Halves are the team's odd / even
  games in ``game_id`` order (the impact box carries no date; any split that ignores the results is
  a valid split-half); the reliability is binned on the half's ``_n`` (12 quantile bins, 10 for the WNBA's smaller
  league) because a player's on/off sample cannot be truncated game by game without his team's.

Each league is fitted on its TRAIN seasons; the HOLDOUT seasons (later, never seen) are fitted
again and their ``M`` recorded, so the test suite can report the reliability the frozen gate
reaches out of sample. ``--write`` saves every curve to ``tests/fixtures/bb_rate_gates/`` and the
test suite re-runs the ``M`` fit offline from them.

Usage::

    uv run python tools/fit_rate_gates.py --league nba --write
    uv run python tools/fit_rate_gates.py --league wbb --data /tmp/rate_gate_cache
"""

from __future__ import annotations

import argparse
import math
import sys
from collections.abc import Callable
from pathlib import Path

import numpy as np
import polars as pl

#: league -> kind -> (train, holdout) seasons by END year, disjoint, the holdout after the train.
SPANS: dict[str, dict[str, tuple[range, range]]] = {
    "nba": {"team": (range(2015, 2024), range(2024, 2027)), "on_off": (range(2019, 2024), range(2024, 2027))},
    "wnba": {"team": (range(2015, 2023), range(2023, 2026)), "on_off": (range(2018, 2023), range(2023, 2026))},
    "mbb": {"team": (range(2022, 2025), range(2025, 2027)), "on_off": (range(2022, 2025), range(2025, 2027))},
    "wbb": {"team": (range(2022, 2025), range(2025, 2027)), "on_off": (range(2022, 2025), range(2025, 2027))},
}
#: Team rate -> (numerator, denominator, the ``_n`` unit, the table's ``_n`` per unit).
TEAM_RATES: dict[str, tuple[str, str, str, int]] = {
    "off_rtg": ("pts", "poss", "poss", 1),
    "def_rtg": ("o_pts", "poss", "poss", 1),
    "net_rtg": ("margin", "poss", "poss", 2),
    "pace": ("poss", "games", "games", 1),
    "efg_pct": ("efgm", "fga", "fga", 1),
    "tov_pct": ("tov", "poss", "poss", 1),
    "orb_pct": ("oreb", "orb_ch", "orb_ch", 1),
    "ft_rate": ("fta", "fga", "fga", 1),
    "opp_efg_pct": ("o_efgm", "o_fga", "o_fga", 1),
    "opp_tov_pct": ("o_tov", "poss", "poss", 1),
    "opp_orb_pct": ("o_oreb", "o_orb_ch", "o_orb_ch", 1),
    "opp_ft_rate": ("o_fta", "o_fga", "o_fga", 1),
    "fg3_pct": ("fg3m", "fg3a", "fg3a", 1),
    "opp_fg3_pct": ("o_fg3m", "o_fg3a", "o_fg3a", 1),
}
ON_OFF_RATES = ("off_rtg_on", "def_rtg_on", "net_rtg_on", "off_rtg_diff", "def_rtg_diff", "net_rtg_diff")
#: league -> (quantile bins, fewest players in a bin) for the on/off fit.
ON_OFF_BINS = {"nba": (12, 60), "wnba": (10, 40), "mbb": (12, 60), "wbb": (12, 60)}
MIN_TEAMS = 30  # teams at a grid point of the team curve (the WNBA holdout has 36)
FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "bb_rate_gates"
_BOX = {
    "field_goals_made": "fgm",
    "field_goals_attempted": "fga",
    "three_point_field_goals_made": "fg3m",
    "three_point_field_goals_attempted": "fg3a",
    "free_throws_made": "ftm",
    "free_throws_attempted": "fta",
    "offensive_rebounds": "oreb",
    "defensive_rebounds": "dreb",
    "total_turnovers": "tov",
}


def gate(m: float) -> int:
    """``m`` rounded up to two significant figures, then to a whole count (never below ``m``)."""
    if not math.isfinite(m) or m <= 0:
        raise ValueError(f"no gate for M={m}")
    step = 10.0 ** (math.floor(math.log10(m)) - 1)
    return math.ceil(math.ceil(round(m / step, 9)) * step)


#: An ``M`` past this many times the largest observed sample is not measured but extrapolated: the
#: curve never got above r = 1/(1 + MAX_EXTRAPOLATION) = 1/3, so the rate is ``inf`` (never ranked).
MAX_EXTRAPOLATION = 2.0


def fit_m(points: list[tuple[float, float, int]]) -> float:
    """Least-squares ``M`` of ``r(n) = n / (n + M)`` over ``(n, r, entities)`` points.

    ``inf`` when the fit lands past :data:`MAX_EXTRAPOLATION` times the largest ``n`` (the curve
    never comes near 0.5, so a finite ``M`` would be a guess); ``nan`` under three points.
    """
    pts = [p for p in points if np.isfinite(p[1])]
    if len(pts) < 3:
        return float("nan")
    n, r, k = (np.array(x, dtype=float) for x in zip(*pts, strict=True))
    grid = np.geomspace(n.min() / 50, n.max() * 200, 6000)
    sse = (k * (r - n / (n + grid[:, None])) ** 2).sum(axis=1)
    m = float(grid[int(np.argmin(sse))])
    return float("inf") if m > MAX_EXTRAPOLATION * n.max() else m


def _corr(a: pl.Series, b: pl.Series) -> float:
    return float(np.corrcoef(a.to_numpy(), b.to_numpy())[0, 1]) if a.len() >= 3 else float("nan")


def _cached(data: Path | None, name: str, build: Callable[[], pl.DataFrame]) -> pl.DataFrame:
    if data is None:
        return build()
    f = data / name
    if not f.exists():
        data.mkdir(parents=True, exist_ok=True)
        build().write_parquet(f)
    return pl.read_parquet(f)


def _team_box(league: str, season: int) -> pl.DataFrame:
    from sportsdataverse.mbb.mbb_loaders import load_mbb_team_boxscore
    from sportsdataverse.nba.nba_loaders import load_nba_team_boxscore
    from sportsdataverse.wbb.wbb_loaders import load_wbb_team_boxscore
    from sportsdataverse.wnba.wnba_loaders import load_wnba_team_boxscore

    load = {
        "nba": load_nba_team_boxscore,
        "wnba": load_wnba_team_boxscore,
        "mbb": load_mbb_team_boxscore,
        "wbb": load_wbb_team_boxscore,
    }[league]
    keep = [
        "game_id",
        "season",
        "season_type",
        "game_date",
        "team_id",
        "opponent_team_id",
        "team_score",
        "turnovers",
        *_BOX,
    ]
    return load([season]).select(keep)


def team_games(league: str, seasons: range, data: Path | None = None) -> pl.DataFrame:
    """One row per regular-season team-game: own (``x``) and opponent (``o_x``) counts, ``poss``."""
    frames = []
    for s in seasons:
        t = _cached(data, f"{league}_team_box_{s}.parquet", lambda s=s: _team_box(league, s))
        t = (
            t.filter(pl.col("season_type").cast(pl.Int64) == 2)
            .rename(_BOX)
            .with_columns(
                pl.col("game_id", "team_id", "opponent_team_id", "season").cast(pl.Int64),
                pl.col("tov").fill_null(pl.col("turnovers")),
            )
        )
        cnt = ["fgm", "fga", "fg3m", "fg3a", "fta", "oreb", "dreb", "tov"]
        t = t.select(
            "game_id",
            "season",
            "game_date",
            "team_id",
            "opponent_team_id",
            *[pl.col(c).cast(pl.Float64) for c in [*cnt, "team_score"]],
        )
        t = t.rename({"team_score": "pts"}).with_columns(
            (pl.col("fga") + 0.44 * pl.col("fta") - pl.col("oreb") + pl.col("tov")).alias("raw_poss")
        )
        opp = t.select(
            "game_id",
            pl.col("team_id").alias("opponent_team_id"),
            *[pl.col(c).alias(f"o_{c}") for c in [*cnt, "pts", "raw_poss"]],
        )
        assert t.schema["opponent_team_id"] == opp.schema["opponent_team_id"], "team id dtype differs"
        frames.append(t.join(opp, on=["game_id", "opponent_team_id"], how="inner", validate="1:1"))
    t = pl.concat(frames).with_columns((0.5 * (pl.col("raw_poss") + pl.col("o_raw_poss"))).alias("poss"))
    return t.filter(pl.col("poss") > 20).with_columns(
        pl.lit(1.0).alias("games"),
        (pl.col("pts") - pl.col("o_pts")).alias("margin"),
        (pl.col("fgm") + 0.5 * pl.col("fg3m")).alias("efgm"),
        (pl.col("o_fgm") + 0.5 * pl.col("o_fg3m")).alias("o_efgm"),
        (pl.col("oreb") + pl.col("o_dreb")).alias("orb_ch"),
        (pl.col("o_oreb") + pl.col("dreb")).alias("o_orb_ch"),
    )


def _halves(t: pl.DataFrame) -> pl.DataFrame:
    t = t.sort("season", "team_id", "game_date", "game_id")
    idx = pl.int_range(pl.len()).over(["season", "team_id"])
    return t.with_columns((idx % 2).alias("half"), (idx // 2).alias("ord"))


def _half_rates(t: pl.DataFrame, num: str, den: str, unit: str, n: float) -> pl.DataFrame:
    """Per team-season: the two half rates on the first ``n`` units, demeaned within the season."""
    d = t.select(
        "season", "team_id", "half", "ord", pl.col(num).alias("x"), pl.col(den).alias("d"), pl.col(unit).alias("u")
    )
    before = pl.col("u").cum_sum().over(["season", "team_id", "half"], order_by="ord") - pl.col("u")
    g = (
        d.filter(before < n)
        .group_by("season", "team_id", "half")
        .agg(pl.col("x").sum(), pl.col("d").sum(), pl.col("u").sum())
        .filter((pl.col("u") >= n) & (pl.col("d") > 0))
        .with_columns((pl.col("x") / pl.col("d")).alias("r"))
    )
    if g["half"].n_unique() < 2:
        return pl.DataFrame(schema={"u_0": pl.Float64, "u_1": pl.Float64, "a": pl.Float64, "b": pl.Float64})
    w = g.pivot(on="half", index=["season", "team_id"], values=["r", "u"]).drop_nulls()
    return w.with_columns(
        (pl.col("r_0") - pl.col("r_0").mean().over("season")).alias("a"),
        (pl.col("r_1") - pl.col("r_1").mean().over("season")).alias("b"),
    )


def team_curve(t: pl.DataFrame, rate: str) -> list[tuple[float, float, int]]:
    """``(n, r, teams)`` over a log grid of ``n`` in the table's ``_n`` units."""
    num, den, unit, scale = TEAM_RATES[rate]
    t = _halves(t)
    totals = t.group_by("season", "team_id").agg(pl.col(unit).sum())[unit].to_numpy() / 2
    lo, hi = np.quantile(totals, 0.15), np.quantile(totals, 0.97)
    pts = []
    for n in np.unique(np.geomspace(max(lo / 4, 1.0), hi * 0.9, 14)):
        w = _half_rates(t, num, den, unit, float(n))
        if w.height >= MIN_TEAMS:
            pts.append((scale * float(((w["u_0"] + w["u_1"]) / 2).mean()), _corr(w["a"], w["b"]), w.height))
    return pts


def _impact(league: str, season: int) -> pl.DataFrame:
    if league in ("mbb", "wbb"):
        from sportsdataverse.mbb.mbb_game_impact import mbb_game_player_impact

        df = mbb_game_player_impact(season, league="mens" if league == "mbb" else "womens")
    else:
        from sportsdataverse.nba.nba_game_impact import nba_game_player_impact

        df = nba_game_player_impact(season, league_id="00" if league == "nba" else "10")
    return df.select(
        "season", "game_id", "team_id", pl.col("player_id").cast(pl.Utf8), "poss", "opp_poss", "pts", "opp_pts"
    )


def on_off_halves(league: str, seasons: range, data: Path | None = None) -> pl.DataFrame:
    """Per (season, team, player, half): on-court and team totals; halves are the team's odd / even games."""
    g = pl.concat([_cached(data, f"{league}_impact_{s}.parquet", lambda s=s: _impact(league, s)) for s in seasons])
    g = g.with_columns(pl.col("season").cast(pl.Int64))
    tot = ("poss", "opp_poss", "pts", "opp_pts")
    team = (
        g.group_by("season", "team_id", "game_id")
        .agg(*[(pl.col(c).sum() / 5).alias(f"t_{c}") for c in tot])
        .sort("season", "team_id", "game_id")
        .with_columns((pl.int_range(pl.len()).over(["season", "team_id"]) % 2).alias("half"))
    )
    x = (
        g.select("season", "team_id", "player_id")
        .unique()
        .join(team, on=["season", "team_id"])
        .join(g, on=["season", "team_id", "game_id", "player_id"], how="left")
        .fill_null(0)
    )
    return x.group_by("season", "team_id", "player_id", "half").agg(pl.col(*tot, *(f"t_{c}" for c in tot)).sum())


def _on_off_exprs() -> dict[str, tuple[pl.Expr, pl.Expr]]:
    """On/off rating -> (value, its ``_n`` as :mod:`sportsdataverse._common.player_on_off` counts it)."""
    off = {c: pl.col(f"t_{c}") - pl.col(c) for c in ("poss", "opp_poss", "pts", "opp_pts")}
    o_on, d_on = 100 * pl.col("pts") / pl.col("poss"), 100 * pl.col("opp_pts") / pl.col("opp_poss")
    o_off, d_off = 100 * off["pts"] / off["poss"], 100 * off["opp_pts"] / off["opp_poss"]
    both_on, both_off = pl.col("poss") + pl.col("opp_poss"), off["poss"] + off["opp_poss"]
    return {
        "off_rtg_on": (o_on, pl.col("poss")),
        "def_rtg_on": (d_on, pl.col("opp_poss")),
        "net_rtg_on": (o_on - d_on, both_on),
        "off_rtg_diff": (o_on - o_off, pl.min_horizontal(pl.col("poss"), off["poss"])),
        "def_rtg_diff": (d_on - d_off, pl.min_horizontal(pl.col("opp_poss"), off["opp_poss"])),
        "net_rtg_diff": ((o_on - d_on) - (o_off - d_off), pl.min_horizontal(both_on, both_off)),
    }


def on_off_curve(h: pl.DataFrame, rate: str, *, bins: int, min_bin: int) -> list[tuple[float, float, int]]:
    """``(median half _n, r, players)`` per quantile bin of the smaller half's ``_n``."""
    value, n = _on_off_exprs()[rate]
    v = h.with_columns(value.alias("v"), n.alias("n")).filter(pl.col("v").is_finite())
    w = v.pivot(on="half", index=["season", "team_id", "player_id"], values=["v", "n"]).drop_nulls()
    w = w.with_columns(
        pl.min_horizontal("n_0", "n_1").alias("g"),
        (pl.col("v_0") - pl.col("v_0").mean().over("season")).alias("a"),
        (pl.col("v_1") - pl.col("v_1").mean().over("season")).alias("b"),
    ).filter(pl.col("g") >= 25)
    edges = np.unique(np.quantile(w["g"].to_numpy(), np.linspace(0, 1, bins + 1)))
    idx = np.clip(np.searchsorted(edges, w["g"].to_numpy(), side="right") - 1, 0, len(edges) - 2)
    w = w.with_columns(pl.Series("bin", idx))
    return sorted(
        (float(p["g"].median()), _corr(p["a"], p["b"]), p.height) for _, p in w.group_by("bin") if p.height >= min_bin
    )


def fit_league(league: str, data: Path | None = None) -> pl.DataFrame:
    """Every curve and ``M`` of a league: one row per (kind, rate, span)."""
    rows = []
    for span, seasons in zip(("train", "holdout"), SPANS[league]["team"], strict=True):
        t = team_games(league, seasons, data)
        for rate in TEAM_RATES:
            pts = team_curve(t, rate)
            rows.append(_row("team", league, rate, span, seasons, pts))
    bins, min_bin = ON_OFF_BINS[league]
    for span, seasons in zip(("train", "holdout"), SPANS[league]["on_off"], strict=True):
        h = on_off_halves(league, seasons, data)
        for rate in ON_OFF_RATES:
            rows.append(_row("on_off", league, rate, span, seasons, on_off_curve(h, rate, bins=bins, min_bin=min_bin)))
    return pl.DataFrame(rows)


def _row(kind: str, league: str, rate: str, span: str, seasons: range, pts: list) -> dict:
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


def parse_curve(text: str) -> list[tuple[float, float, int]]:
    """The ``curve`` column of the fixture back into ``(n, r, entities)`` points."""
    return [(float(n), float(r), int(k)) for n, r, k in (p.split(":") for p in text.split(";") if p)]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--league", choices=sorted(SPANS), required=True)
    ap.add_argument("--data", type=Path, default=None, help="cache the downloaded box scores here")
    ap.add_argument("--write", action="store_true", help=f"save the curves to {FIXTURES}")
    args = ap.parse_args(argv)
    fits = fit_league(args.league, args.data)
    train = fits.filter(pl.col("span") == "train")
    for r in train.iter_rows(named=True):
        hold = fits.filter(
            (pl.col("span") == "holdout") & (pl.col("kind") == r["kind"]) & (pl.col("rate") == r["rate"])
        )
        mh = hold["m"].item()
        g = gate(r["m"]) if math.isfinite(r["m"]) else None
        rel = g / (g + mh) if g and math.isfinite(mh) else float("nan")
        print(f"{r['kind']:7} {r['rate']:13} M={r['m']:9.1f} gate={g} holdout M={mh:9.1f} holdout r at gate={rel:.2f}")
    if args.write:
        FIXTURES.mkdir(parents=True, exist_ok=True)
        fits.write_csv(FIXTURES / f"{args.league}_fits.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
