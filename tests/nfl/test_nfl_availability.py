"""Unit + oracle tests for the availability model (Phase 4)."""

from pathlib import Path

import polars as pl

from sportsdataverse.nfl.nfl_availability import season_availability

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_projection"


def _mini_snaps():
    rows = []
    for week in range(1, 13):  # 12 games with snaps
        rows.append(
            {"player_id": "P1", "season": 2023, "week": week, "team": "A", "offense_snaps": 40.0, "offense_pct": 0.6}
        )
    rows.append({"player_id": "P1", "season": 2023, "week": 13, "team": "A", "offense_snaps": 0.0, "offense_pct": 0.0})
    return pl.DataFrame(rows)


def _mini_rosters():
    return pl.DataFrame({"player_id": ["P1"], "season": [2023], "position": ["RB"], "age": [25.0]})


def test_season_availability_rate():
    out = season_availability(_mini_snaps(), _mini_rosters(), team_games=17)
    r = out.row(0, named=True)
    assert r["games_available"] == 12  # week 13 had zero snaps
    assert abs(r["availability_rate"] - 12.0 / 17.0) < 1e-9
    assert r["position"] == "RB"
    assert abs(r["age"] - 25.0) < 1e-9


def test_season_availability_empty_schema():
    out = season_availability(_mini_snaps().head(0), _mini_rosters().head(0))
    assert out.height == 0
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["availability_rate"] == pl.Float64


def test_availability_projection_shrinks_toward_base(monkeypatch):
    import sportsdataverse.nfl.nfl_availability as mod
    from sportsdataverse.nfl.nfl_projection_constants import get_position_constants

    # QB with an injury-shortened prior season (6 of 17 games)
    snaps = pl.DataFrame(
        [
            {"player_id": "Q1", "season": 2023, "week": w, "team": "A", "offense_snaps": 60.0, "offense_pct": 0.9}
            for w in range(1, 7)
        ]
    )
    rosters = pl.DataFrame({"player_id": ["Q1"], "season": [2023], "position": ["QB"], "age": [27.0]})
    monkeypatch.setattr(mod, "load_nfl_snap_counts", lambda *a, **k: snaps)
    monkeypatch.setattr(mod, "load_nfl_rosters", lambda *a, **k: rosters)
    out = mod.nfl_availability_projection([2023], 2024)
    r = out.filter(pl.col("player_id") == "Q1").row(0, named=True)
    base = get_position_constants("QB").base_availability
    raw = 6.0 / 17.0
    assert raw < r["proj_availability"] < base  # shrunk up toward base, below base
    assert abs(r["proj_games"] - r["proj_availability"] * 17.0) < 1e-9
    assert abs(r["proj_games_missed"] - (17.0 - r["proj_games"])) < 1e-9


def test_availability_projection_chronically_available_qb_near_base(monkeypatch):
    import sportsdataverse.nfl.nfl_availability as mod
    from sportsdataverse.nfl.nfl_projection_constants import get_position_constants

    rows = []
    for season in [2021, 2022, 2023]:
        for w in range(1, 18):
            rows.append(
                {"player_id": "Q2", "season": season, "week": w, "team": "A", "offense_snaps": 60.0, "offense_pct": 0.9}
            )
    snaps = pl.DataFrame(rows)
    rosters = pl.DataFrame(
        {"player_id": ["Q2"] * 3, "season": [2021, 2022, 2023], "position": ["QB"] * 3, "age": [25.0, 26.0, 27.0]}
    )
    monkeypatch.setattr(mod, "load_nfl_snap_counts", lambda *a, **k: snaps)
    monkeypatch.setattr(mod, "load_nfl_rosters", lambda *a, **k: rosters)
    out = mod.nfl_availability_projection([2021, 2022, 2023], 2024)
    r = out.filter(pl.col("player_id") == "Q2").row(0, named=True)
    base = get_position_constants("QB").base_availability
    assert r["proj_availability"] >= base  # 100% history projects at or above base
    assert r["proj_availability"] <= 1.0


def test_availability_leakage_boundary(monkeypatch):
    import sportsdataverse.nfl.nfl_availability as mod

    snaps = pl.DataFrame(
        [
            {"player_id": "Q1", "season": 2023, "week": w, "team": "A", "offense_snaps": 60.0, "offense_pct": 0.9}
            for w in range(1, 18)
        ]
    )
    poisoned = pl.concat(
        [
            snaps,
            pl.DataFrame(
                [{"player_id": "Q1", "season": 2024, "week": 1, "team": "A", "offense_snaps": 60.0, "offense_pct": 0.9}]
            ),
        ]
    )
    rosters = pl.DataFrame({"player_id": ["Q1"], "season": [2023], "position": ["QB"], "age": [27.0]})
    monkeypatch.setattr(mod, "load_nfl_rosters", lambda *a, **k: rosters)
    monkeypatch.setattr(mod, "load_nfl_snap_counts", lambda *a, **k: snaps)
    clean = mod.nfl_availability_projection([2023, 2024], 2024)
    monkeypatch.setattr(mod, "load_nfl_snap_counts", lambda *a, **k: poisoned)
    dirty = mod.nfl_availability_projection([2023, 2024], 2024)
    a = clean.filter(pl.col("player_id") == "Q1")["proj_availability"][0]
    b = dirty.filter(pl.col("player_id") == "Q1")["proj_availability"][0]
    assert abs(a - b) < 1e-12


def test_oracle_availability_vs_realized_2024():
    """Availability oracle vs realized 2024 snap counts (offline fixtures).

    Population: "recent regulars" — players with >= 8 available games in the
    most recent visible season (as-of-clean; the composition-relevant
    population). Observed 2026-07-08 with EB_PRIOR_SEASONS=0.2 +
    AVAIL_RECAL=(0.1838, 0.7146) (both fit on 2022/2023 as-of folds, see
    dev/nfl_projection/fit_availability.py): games MAE 3.5422 (floor 3.6),
    max decile calibration gap 0.0458 (gate 0.05).

    FINDING (plan Task 4.2 base-order sanity check): "RB base < QB base" is
    INVERTED in 2021-2023 snap data at every conditioning tried; crosswalk
    investigated and verified clean (star QBs exact vs weekly stats, 0/247 QB
    seasons missing). QB is winner-take-all, so snap-derived availability folds
    benching/depth churn into QB unavailability. Documented in
    POSITION_CONSTANTS; the order assert is therefore not applicable to this
    operationalization.
    """
    import sportsdataverse.nfl.nfl_availability as mod
    from sportsdataverse.nfl.nfl_projection_constants import calibration_table, mae

    snaps = pl.read_parquet(FIX / "snap_counts_2020_2023.parquet")
    rosters = pl.read_parquet(FIX / "rosters_2020_2023.parquet")
    snap24 = pl.read_parquet(FIX / "snap_counts_2024.parquet")
    orig_s, orig_r = mod.load_nfl_snap_counts, mod.load_nfl_rosters
    mod.load_nfl_snap_counts = lambda *a, **k: snaps
    mod.load_nfl_rosters = lambda *a, **k: rosters
    try:
        out = mod.nfl_availability_projection([2020, 2021, 2022, 2023], 2024)
        sa = mod.season_availability(snaps, rosters, team_games=17)
    finally:
        mod.load_nfl_snap_counts = orig_s
        mod.load_nfl_rosters = orig_r
    regulars = sa.filter((pl.col("season") == 2023) & (pl.col("games_available") >= 8)).select("player_id")
    realized = (
        snap24.filter(pl.col("offense_snaps") > 0)
        .group_by("player_id")
        .agg(
            (pl.col("week").n_unique() / 17.0).cast(pl.Float64).alias("realized_avail"),
            pl.col("week").n_unique().cast(pl.Float64).alias("realized_games"),
        )
    )
    assert out.schema["player_id"] == realized.schema["player_id"]
    j = out.join(regulars, on="player_id", how="inner").join(realized, on="player_id", how="inner")
    assert j.height >= 300
    m = mae(j["proj_games"].to_numpy(), j["realized_games"].to_numpy())
    assert m <= 3.6, f"games MAE {m:.4f} > 3.6"
    tbl = calibration_table(j["realized_avail"].to_numpy(), j["proj_availability"].to_numpy())
    gap = float((tbl["mean_pred"] - tbl["mean_actual"]).abs().max())
    assert gap <= 0.05, f"max decile calibration gap {gap:.4f} > 0.05\n{tbl}"


def test_compose_counting_projection():
    from sportsdataverse.nfl.nfl_availability import compose_counting_projection

    rate = pl.DataFrame({"player_id": ["P1"], "proj_rate": [10.0], "proj_volume": [3.0]})
    avail = pl.DataFrame({"player_id": ["P1"], "proj_availability": [0.5]})
    out = compose_counting_projection(rate, avail)
    assert abs(out["proj_counting"][0] - 15.0) < 1e-9
