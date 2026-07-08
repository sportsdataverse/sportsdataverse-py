"""Phase-1 oracle gate: talent composite vs the 247 Team Talent snapshot (T2.2 Task 1.4).

Gate: spearman(talent_composite, talent_247) >= 0.85 on the validation season (2023),
joined on the normalized 247 team name (the recruit-feed team key and the talent-feed
team key live in DIFFERENT 247 id spaces -- name is the documented join key), plus
Bud Elliott's blue-chip -> title invariant for the realized 2023 champion (Michigan).

Rule: never lower the gate to pass -- if red, calibrate class_recency_weights /
star_points (dev/cfb_projection/fit_talent_weights.py) and re-check windowing.
Fixtures are real captures (see tests/fixtures/cfb_projection/README.md).
"""

from __future__ import annotations

import pathlib

import polars as pl
import pytest

import importlib

_mod = importlib.import_module("sportsdataverse.cfb.cfb_roster_talent")
from sportsdataverse.cfb.cfb_projection_constants import spearman_corr
from sportsdataverse.cfb.cfb_roster_talent import blue_chip_ratio, cfb_roster_talent

_FIX = pathlib.Path(__file__).parent.parent / "fixtures" / "cfb_projection"
_RECRUITS = _FIX / "recruits_2014_2023.parquet"
_TALENT = _FIX / "talent_247_2023.parquet"

pytestmark = pytest.mark.skipif(
    not (_RECRUITS.exists() and _TALENT.exists()),
    reason="cfb_projection oracle fixtures not captured",
)


def _norm_name(col: str) -> pl.Expr:
    return pl.col(col).str.to_lowercase().str.strip_chars()


@pytest.fixture()
def talent_2023(monkeypatch) -> pl.DataFrame:
    fixture = pl.read_parquet(_RECRUITS)
    monkeypatch.setattr(
        _mod,
        "load_recruit_classes",
        lambda seasons, **k: fixture.filter(
            pl.col("season").is_in([seasons] if isinstance(seasons, int) else list(seasons))
        ),
    )
    out = cfb_roster_talent(2023, division="fbs")
    assert isinstance(out, pl.DataFrame)
    return out


def test_talent_composite_oracle_gate(talent_2023: pl.DataFrame) -> None:
    """Observed spearman on 2026-07-08 capture: see assertion (floor 0.85 per plan)."""
    ora = pl.read_parquet(_TALENT)
    assert talent_2023.schema["team"] == ora.schema["team"] == pl.Utf8
    m = talent_2023.with_columns(_norm_name("team").alias("_k")).join(
        ora.with_columns(_norm_name("team").alias("_k")), on="_k", how="inner"
    )
    # the talent snapshot covers ranked FBS teams; expect the bulk of them to name-match
    assert m.height >= 100, f"name-join matched only {m.height} teams"
    rho = spearman_corr(m["talent_composite"].to_numpy(), m["talent_247"].to_numpy())
    assert rho >= 0.85, f"spearman(talent_composite, talent_247) = {rho:.4f} < 0.85"


def test_blue_chip_title_invariant() -> None:
    """2023 national champion (Michigan) must sit in the top tier of FBS blue-chip ratios.

    Bud Elliott's published invariant ("no champion since 2011 below a 50% blue-chip
    ratio") is defined on his ROSTER-based universe (signees still on the roster,
    attrition removes disproportionately many 3-stars). Our metric is the signed-class
    aggregate over the trailing 4 classes, which runs systematically lower: on the
    2026-07-08 capture the elite cluster spans 0.33-0.53 (Georgia 0.535, Clemson 0.500,
    Alabama 0.473, Ohio State 0.457, Michigan 0.337) while typical FBS programs sit
    near 0.0-0.1. The measurement-invariant form of the same claim is therefore a
    percentile gate: the champion must clear the 85th percentile of FBS-sized
    programs. Observed: Michigan 0.337 vs 85th percentile ~0.19. Do NOT replace this
    with the absolute 0.50 constant unless the metric is moved to the roster universe.
    """
    fixture = pl.read_parquet(_RECRUITS)
    bcr = blue_chip_ratio(fixture, window=4, division="fbs")
    season_2023 = bcr.filter(pl.col("season") == 2023)
    # FBS proxy: programs with a full 4-class window of signees
    fbs = season_2023.filter(pl.col("n_recruits") >= 60)
    assert fbs.height >= 100, f"FBS-sized pool unexpectedly small: {fbs.height}"
    champ = fixture.filter(_norm_name("team") == "michigan wolverines").select("team_id").unique()
    row = season_2023.filter(pl.col("team_id").is_in(champ["team_id"].implode()))
    assert row.height == 1, f"expected one Michigan team-season, got {row.height}"
    ratio = row["blue_chip_ratio"][0]
    p85 = fbs["blue_chip_ratio"].quantile(0.85)
    assert p85 is not None
    assert ratio >= p85, f"champion blue_chip_ratio {ratio:.3f} below FBS 85th pct {p85:.3f}"
