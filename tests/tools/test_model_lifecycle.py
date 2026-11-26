"""The model lifecycle checks (``tools/models/lifecycle/``): ledger fields, retire rule, coverage, sign stability.

Real fixtures: the committed ledger; BB-D25's rated-prior holdout games
(``tests/fixtures/nba_prediction/nba_rated_prior_games.parquet``), whose committed card figure the retire
rule's season record must reproduce; and the season-sim fixtures (``tests/fixtures/bb_pro_season``,
``tests/fixtures/bb_team_season_rates``: NBA 2024-25 and 2025-26 schedules and possessions), on which the
coverage check runs end to end and its silent no-op check must catch a disabled rating draw.
"""

from __future__ import annotations

import copy
import importlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType

import numpy as np
import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
LIFE = ROOT / "tools" / "models" / "lifecycle"
FIX = ROOT / "tests" / "fixtures"


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


retire = _load("lifecycle_retire", LIFE / "retire.py")
signs = _load("lifecycle_signs", LIFE / "signs.py")
cov = _load("lifecycle_coverage", LIFE / "coverage.py")


# --- (b) ledger + retire rule -------------------------------------------------------------------


def _ledger() -> dict:
    return json.loads(retire.LEDGER.read_text(encoding="utf-8"))


def test_ledger_lifecycle_fields_are_consistent() -> None:
    ledger = _ledger()
    assert ledger["schema_version"] == "1.1"
    assert retire.check_ledger(ledger) == []
    # the spec's first application has a baseline and a stated margin
    for lg in ("nba", "wnba"):
        row = retire.ledger_row(ledger, lg, "pro_season_sim")
        assert row["baseline"] and row["adoption_margin"] < 0 and row["holdout_seasons"]


@pytest.mark.parametrize(
    ("edit", "needle"),
    [
        (lambda r: r.pop("status"), "missing"),
        (lambda r: r.update(status="paused"), "not in"),
        (lambda r: r.update(status="retired"), "retired_reason"),
        (lambda r: r.update(lifecycle_note=" "), "lifecycle_note"),
        (lambda r: r.update(baseline=None, adoption_margin=None), "both set or both null"),
        (lambda r: r.update(adoption_margin=float("nan")), "finite"),
    ],
)
def test_check_ledger_catches_each_bad_row(edit, needle: str) -> None:  # type: ignore[no-untyped-def]
    ledger = copy.deepcopy(_ledger())
    row = retire.ledger_row(ledger, "nba", "pro_season_sim")
    edit(row)
    problems = retire.check_ledger(ledger)
    assert len(problems) == 1 and needle in problems[0], problems


@pytest.fixture(scope="module")
def rated_prior_diffs() -> pl.DataFrame:
    """Per-game log-loss ``rated - hfa`` on the committed holdout games, through the card's own code."""
    cards = _load("wp_model_cards", ROOT / "tools/models/nba_prediction/wp_model_cards.py")
    ll = _load("fit_preseason_prior", ROOT / "tools/fit_preseason_prior.py")._ll
    g = cards.prior_probs(pl.read_parquet(FIX / "nba_prediction" / "nba_rated_prior_games.parquet"), "nba")
    y = g["y"].to_numpy()
    d = ll(y, g["p_rated"].to_numpy()) - ll(y, g["p_hfa"].to_numpy())
    return g.select("season", "game_id").with_columns(pl.Series("d", d))


def test_season_record_reproduces_the_committed_paired_figure(rated_prior_diffs: pl.DataFrame) -> None:
    """The ledger's adoption margin for nba/rated_pregame_prior is the card's paired -0.07942 (SE 0.00554)."""
    rec = retire.season_record(2026, rated_prior_diffs["d"].to_numpy(), rated_prior_diffs["game_id"].to_numpy())
    row = retire.ledger_row(_ledger(), "nba", "rated_pregame_prior")
    assert round(rec["diff"], 5) == row["adoption_margin"] == -0.07942
    assert round(rec["se"], 5) == 0.00554  # CR1; G/(G-1) is 1.0003 at 3,961 games
    assert rec["upper"] < 0


def _records(diffs: pl.DataFrame) -> list[dict]:
    return [
        retire.season_record(s, f["d"].to_numpy(), f["game_id"].to_numpy())
        for (s,), f in diffs.group_by("season", maintain_order=True)
    ]


def test_retire_rule_on_real_season_records(rated_prior_diffs: pl.DataFrame) -> None:
    """Real per-season records, read as if 2024-2026 were past a holdout ending 2023 (illustrative).

    Every real season beats the baseline: KEEP. Flipping the sign of the gain trips both triggers.
    """
    recs = _records(rated_prior_diffs)
    assert len(recs) == 3 and all(r["upper"] < 0 for r in recs)
    assert retire.retire_status(recs, -0.07942, holdout_end=2023)["status"] == "KEEP"
    worse = [r | {"diff": -r["diff"], "upper": -r["diff"] + retire.Z * r["se"]} for r in recs]
    out = retire.retire_status(worse, -0.07942, holdout_end=2023)
    assert out["status"] == "RETIRE-candidate"
    assert sum("both fail" in x for x in out["reasons"]) == 2  # 2024-25 and 2025-26
    beyond = sum(w["diff"] > 0.07942 for w in worse)
    assert beyond >= 1 and sum("adoption margin" in x for x in out["reasons"]) == beyond

    def failing(r: dict, upper: float) -> dict:  # the bound at ``upper``, the SE kept
        return r | {"diff": upper - retire.Z * r["se"], "upper": upper}

    # the latest season fails (at the >= 0 boundary) and nothing else: WATCH, point trigger off
    one = [*recs[:2], failing(recs[2], 0.0)]
    assert retire.retire_status(one, None, holdout_end=2023) | {"reasons": []} == {
        "status": "WATCH",
        "reasons": [],
        "scored_seasons": [2024, 2025, 2026],
        "point_trigger": "off (no adoption_margin in the ledger)",
    }
    # two fails that are not consecutive: WATCH, not RETIRE
    gap = [failing(recs[0], 0.001), recs[1], failing(recs[2], 0.001)]
    assert retire.retire_status(gap, None, holdout_end=2023)["status"] == "WATCH"
    assert retire.retire_status([], -0.07942, holdout_end=2023)["status"] == "UNSCORED"
    with pytest.raises(ValueError, match="fit's window"):
        retire.retire_status(recs, -0.07942, holdout_end=2026)


@pytest.mark.parametrize(
    ("bad", "needle"),
    [
        ({"diff": float("nan"), "se": float("nan"), "upper": float("nan")}, "finite"),
        ({"diff": 0.05, "se": 0.01, "upper": -1.0}, "is not diff"),
        ({"se": 0.0}, "finite"),
        ({"clusters": 1}, "two clusters"),
    ],
)
def test_retire_rule_refuses_a_malformed_record(rated_prior_diffs: pl.DataFrame, bad: dict, needle: str) -> None:
    rec = _records(rated_prior_diffs)[0] | bad
    if bad == {"se": 0.0}:
        rec["upper"] = rec["diff"]
    with pytest.raises(ValueError, match=needle):
        retire.retire_status([rec], -0.07942, holdout_end=2023)


def test_retire_cli_uses_the_ledger_holdout(rated_prior_diffs: pl.DataFrame, tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    records = tmp_path / "seasons.json"
    records.write_text(json.dumps(_records(rated_prior_diffs)[2:]), encoding="utf-8")  # 2026
    with pytest.raises(ValueError, match="holdout ends 2026"):
        retire.main(["--league", "nba", "--model", "pro_season_sim", "--records", str(records)])
    assert retire.main(["--league", "nba", "--model", "pro_season_sim"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "UNSCORED"
    # a baseline with no holdout window (LOSO only) names no season past the fit
    assert retire.main(["--league", "cfb", "--model", "fg_model"]) == 1
    assert "no holdout_seasons" in capsys.readouterr().out


# --- (c) sign stability ---------------------------------------------------------------------------


def test_sign_classes() -> None:
    """The rule itself, one coefficient per class (estimates and SEs are illustrative, not data)."""
    folds = [
        {
            "fold_season": 2022,
            "coef": {"a": 0.5, "b": 0.05, "c": 0.3, "d": 0.2},
            "coef_se": {"a": 0.1, "b": 0.1, "c": 0.1},
        },
        {
            "fold_season": 2023,
            "coef": {"a": 0.4, "b": -0.04, "c": -0.4, "d": -0.2},
            "coef_se": {"a": 0.1, "b": 0.1, "c": 0.1},
        },
    ]
    t = signs.sign_table(folds)["coefficients"]
    assert t["a"]["class"] == "stable"
    assert t["b"]["class"] == "noise_flip"
    assert t["c"] == t["c"] | {"class": "real_flip", "real_flip_folds": [[2022, 2023]]}
    assert t["d"]["class"] == "unclassified"  # no SE: real and noise cannot be told apart
    with pytest.raises(ValueError, match="every fold"):
        signs.sign_table([folds[0], {"fold_season": 2024, "coef": {"a": 1.0}}])
    # an unusable SE is no SE
    for se in (float("nan"), 0.0):
        bad = [folds[0], folds[1] | {"coef_se": {"c": se}}]
        assert signs.sign_table(bad)["coefficients"]["c"]["class"] == "unclassified"
    with pytest.raises(ValueError, match="two folds"):
        signs.sign_table(folds[:1])


# --- (a) win-total coverage -----------------------------------------------------------------------


def test_central_interval_holds_its_level() -> None:
    pmf = np.bincount(np.random.default_rng(0).binomial(82, 0.5, 5000)) / 5000
    for lv in cov.LEVELS:
        lo, hi, mass = cov.central_interval(pmf, lv)
        assert lo <= hi and mass >= lv


def _fixture_loaders(mp: pytest.MonkeyPatch) -> None:
    tsr = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")

    def loader(kind: str):  # type: ignore[no-untyped-def]
        def load(seasons: list[int]) -> pl.DataFrame:
            (s,) = seasons
            name = f"nba_{kind}_{s + 1}.parquet"
            path = next(
                (d / name for d in (FIX / "bb_pro_season", FIX / "bb_team_season_rates") if (d / name).exists())
            )
            return pl.read_parquet(path)

        return load

    mp.setattr(tsr, "load_nba_stats_possessions", loader("possessions"))
    mp.setattr(tsr, "load_nba_stats_schedules", loader("schedule"))


def test_coverage_end_to_end_on_the_2025_26_fixture() -> None:
    with pytest.MonkeyPatch.context() as mp:
        _fixture_loaders(mp)
        rows = cov.run_season("nba", 2026, n_sims=400, seed=1)
    assert rows.height == 30 * 2 * 2
    assert sorted(rows["as_of"].unique().to_list()) == ["2025-10-21", "2025-12-01"]
    one = rows.filter((pl.col("cutoff") == "preseason") & (pl.col("arm") == "shift"))
    assert one["final_wins"].sum() == 1230  # every regular-season game has one winner
    assert (one["mass_50"] < one["mass_80"]).all() and (one["mass_80"] <= one["mass_95"]).all()
    assert (rows["mass_80"] >= 0.8).all()
    rep = cov.report(rows, "nba")
    assert rep["noop_check"]["status"] == "PASS"
    assert rep["noop_check"]["width_ratio"] > 3  # observed 3.49
    # one season is one cluster: no SE is quoted and no OK/WATCH is read
    assert rep["cutoffs"]["preseason"]["shift"]["80"]["diff_se_cr1_season"] is None
    assert rep["reading"] == "TOO_FEW_SEASONS"


def test_noop_check_catches_a_disabled_rating_draw() -> None:
    """Mutation: the shipped arm silently runs without the draw. The check must report BUG."""
    real = cov.sim._simulate_season_draws

    def no_draw(*args, **kwargs):  # type: ignore[no-untyped-def]
        return real(*args, **(kwargs | {"rating_uncertainty": False}))

    with pytest.MonkeyPatch.context() as mp:
        _fixture_loaders(mp)
        mp.setattr(cov.sim, "_simulate_season_draws", no_draw)
        rows = cov.run_season("nba", 2026, n_sims=400, seed=1)
    rep = cov.report(rows, "nba")
    assert rep["noop_check"]["status"] == "BUG" and rep["reading"] == "BUG"


def test_noop_check_catches_a_crushed_rating_draw() -> None:
    """Mutation: the draw's sd cut to a tenth. The intervals still widen a little; the floor catches it."""
    real = cov.sim._team_arrays

    def crushed(*args, **kwargs):  # type: ignore[no-untyped-def]
        net, sd = real(*args, **kwargs)
        return net, sd * 0.1

    with pytest.MonkeyPatch.context() as mp:
        _fixture_loaders(mp)
        mp.setattr(cov.sim, "_team_arrays", crushed)
        rows = cov.run_season("nba", 2026, n_sims=400, seed=1)
    check = cov.report(rows, "nba")["noop_check"]
    assert 1.0 < check["width_ratio"] < cov.WIDTH_RATIO_FLOOR and check["status"] == "BUG"


def test_draws_line_up_with_the_frame() -> None:
    """``_simulate_season_draws``: every draw has one winner per game, and the columns follow ``team_ids``."""
    with pytest.MonkeyPatch.context() as mp:
        _fixture_loaders(mp)
        cut = cov.dt.date(2025, 12, 1)
        games, model, records, season_games, _ = cov.sim._load_inputs(2026, cut, "00", cup="never")
        cols = games.select(*cov.sim._SIM_COLS).with_columns(pl.lit(2026, dtype=pl.Int64).alias("season"))
        frame, _, (teams, wins) = cov.sim._simulate_season_draws(
            cols, model, n_sims=300, seed=2, prior_records=records, as_of_date=cut, season_games=season_games
        )
    assert season_games is None and (wins.sum(1) == 1230).all()
    proj = dict(zip(frame["team_id"].to_list(), frame["proj_wins"].to_list(), strict=True))
    assert np.allclose(wins.mean(0), [proj[t] for t in teams])
    assert teams == sorted(teams)


def test_cr1_se_with_repeated_clusters() -> None:
    """Two clusters of two rows: residual sums -2 and 2, so sqrt(2 / 1 * 8) / 4 = 1."""
    assert retire.cr1_se(np.array([1.0, 2.0, 3.0, 4.0]), np.array(["a", "a", "b", "b"])) == 1.0


def test_readings_and_their_precedence() -> None:
    """8 relabelled copies of the real 2025-26 fixture season: an SE is quoted, so OK / WATCH are read."""
    with pytest.MonkeyPatch.context() as mp:
        _fixture_loaders(mp)
        one = cov.run_season("nba", 2026, n_sims=400, seed=1)
    rows = pl.concat(one.with_columns(pl.lit(2000 + k, dtype=pl.Int64).alias("season")) for k in range(8))
    rep = cov.report(rows, "nba")
    pre = rep["cutoffs"]["preseason"]
    # identical copies: no between-season spread, so the tolerance is the 0.05 floor
    assert pre["tolerance"] == cov.TOL_FLOOR
    s80 = pre["shift"]["80"]
    assert cov.reading(s80) == ("OK" if abs(s80["diff"]) <= cov.TOL_FLOOR else "WATCH")
    assert cov.reading(s80 | {"diff": 0.05}) == "OK" and cov.reading(s80 | {"diff": 0.0501}) == "WATCH"
    assert cov.reading(s80 | {"diff_se_cr1_season": 0.03, "diff": 0.06}) == "OK"  # 2 SE = 0.06 > floor
    assert cov.reading(s80 | {"diff_se_cr1_season": None}) == "TOO_FEW_SEASONS"
    readings = {rep["cutoffs"][c]["reading"] for c in ("preseason", "in_season")}
    assert rep["reading"] == ("WATCH" if "WATCH" in readings else "OK")
