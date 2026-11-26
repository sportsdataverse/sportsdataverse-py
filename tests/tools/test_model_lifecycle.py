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
        (lambda r: r.update(reserved_seasons=[2026, 2022]), "sorted and unique"),
        (lambda r: r.update(reserved_seasons="2026"), "list of integer seasons"),
        (
            lambda r: r.update(status="baseline_tied", baseline=None, primary_metric=None, adoption_margin=None),
            "baseline_tied without a baseline",
        ),
    ],
)
def test_check_ledger_catches_each_bad_row(edit, needle: str) -> None:  # type: ignore[no-untyped-def]
    ledger = copy.deepcopy(_ledger())
    row = retire.ledger_row(ledger, "nba", "pro_season_sim")
    edit(row)
    problems = retire.check_ledger(ledger)
    assert len(problems) == 1 and needle in problems[0], problems


def test_baseline_tied_is_a_ledger_status_with_the_margin_trigger_only(capsys) -> None:  # type: ignore[no-untyped-def]
    """The draft / rookie pick curves ship as ``baseline_tied``: the check accepts it and the CLI passes it on."""
    ledger = _ledger()
    tied = {(r["league"], r["model"]) for r in ledger["models"] if r["status"] == "baseline_tied"}
    assert tied >= {("nba", "draft_value"), ("nba", "rookie_projection")}
    assert retire.check_ledger(ledger) == []
    assert retire.main(["--league", "nba", "--model", "draft_value"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ledger_status"] == "baseline_tied" and out["status"] == "UNSCORED"
    assert out["margin_trigger"] == "on, margin only (baseline_tied)"


def _rec(season: int, diff: float, se: float) -> dict:
    return {"season": season, "diff": diff, "se": se, "upper": diff + retire.Z * se}


def test_baseline_tied_never_retires_on_a_tie_only_on_a_loss_beyond_the_margin() -> None:
    """A tie by construction (diff 0 every season) retires an active row after two seasons, never a tied one."""
    ties = [_rec(2020, 0.0, 0.01), _rec(2021, 0.0, 0.01)]
    assert retire.retire_status(ties, None, holdout_end=2019)["status"] == "RETIRE-candidate"
    tied = retire.retire_status(ties, None, holdout_end=2019, tied=True)
    assert tied["status"] == "KEEP" and tied["reasons"] == []
    # one season worse than the baseline at 1.96 SE (lower bound above the margin, 0 when none is stated)
    worse = [*ties, _rec(2022, 0.05, 0.01)]
    out = retire.retire_status(worse, None, holdout_end=2019, tied=True)
    assert out["status"] == "RETIRE-candidate" and len(out["reasons"]) == 1 and "2022" in out["reasons"][0]
    # inside a stated margin it stays
    assert retire.retire_status(worse, -0.04, holdout_end=2019, tied=True)["status"] == "KEEP"
    edge = [_rec(2020, retire.Z * 0.01 - 1e-12, 0.01)]  # lower bound just under 0
    assert retire.retire_status(edge, None, holdout_end=2019, tied=True)["status"] == "KEEP"
    # the pick curves rank exactly as the pick: Spearman diff 0 with SE 0 is a valid record for a tied row only
    exact = [_rec(2020, 0.0, 0.0), _rec(2021, 0.0, 0.0)]
    assert retire.retire_status(exact, None, holdout_end=2019, tied=True)["status"] == "KEEP"
    with pytest.raises(ValueError, match="se > 0"):
        retire.retire_status(exact, None, holdout_end=2019)
    with pytest.raises(ValueError, match="se > 0"):
        retire.retire_status([_rec(2020, 0.01, 0.0)], None, holdout_end=2019, tied=True)


def test_reserved_seasons_are_refused(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """WNBA 2022, 2023 and 2026 are held back by the sim-vs-market spec: the rule and coverage.py refuse them."""
    row = retire.ledger_row(_ledger(), "wnba", "pro_season_sim")
    assert row["reserved_seasons"] == [2022, 2023, 2026]
    with pytest.raises(ValueError, match="reserved"):
        retire.retire_status([_rec(2026, -0.01, 0.01)], -0.00129, holdout_end=2025, reserved=row["reserved_seasons"])
    assert (
        retire.retire_status([_rec(2027, -0.03, 0.01)], -0.00129, holdout_end=2025, reserved=[2026])["status"] == "KEEP"
    )
    records = tmp_path / "seasons.json"
    records.write_text(json.dumps([_rec(2026, -0.01, 0.01)]), encoding="utf-8")
    with pytest.raises(ValueError, match="reserved"):
        retire.main(["--league", "wnba", "--model", "pro_season_sim", "--records", str(records)])
    # coverage.py reads the same list, and it matches the seasons its own table marks reserved
    assert cov.reserved_seasons("wnba") == row["reserved_seasons"]
    assert cov.reserved_seasons("wnba") == sorted(s for s, why in cov.EXCLUDED["wnba"].items() if "reserved" in why)
    assert cov.reserved_seasons("nba") == []

    def never(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("a reserved season reached the simulation")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cov, "reserved_seasons", lambda league: [2010])
        mp.setattr(cov, "run_season", never)
        with pytest.raises(SystemExit):
            cov.main(["--league", "wnba", "--seasons", "2010"])
    assert "reserved in the ledger" in capsys.readouterr().err


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
    # the margin trigger needs the loss beyond the margin at 1.96 SE, not just at the point estimate
    beyond = sum(w["diff"] - retire.Z * w["se"] > 0.07942 for w in worse)
    assert sum(w["diff"] > 0.07942 for w in worse) > beyond
    assert sum("adoption margin" in x for x in out["reasons"]) == beyond
    sure = [w | {"diff": 0.2, "upper": 0.2 + retire.Z * w["se"]} for w in worse[:1]]
    assert "adoption margin" in retire.retire_status(sure, -0.07942, holdout_end=2023)["reasons"][0]
    d = 0.07942 + retire.Z * worse[0]["se"] - 1e-12  # just inside the bar
    edge = [worse[0] | {"diff": d, "upper": d + retire.Z * worse[0]["se"]}]
    assert retire.retire_status(edge, -0.07942, holdout_end=2023)["status"] == "WATCH"  # exactly at the bar

    def failing(r: dict, upper: float) -> dict:  # the bound at ``upper``, the SE kept
        return r | {"diff": upper - retire.Z * r["se"], "upper": upper}

    # the latest season fails (at the >= 0 boundary) and nothing else: WATCH, point trigger off
    one = [*recs[:2], failing(recs[2], 0.0)]
    assert retire.retire_status(one, None, holdout_end=2023) | {"reasons": []} == {
        "status": "WATCH",
        "reasons": [],
        "scored_seasons": [2024, 2025, 2026],
        "margin_trigger": "off (no adoption_margin in the ledger)",
    }
    # two fails that are not consecutive: WATCH, not RETIRE
    gap = [failing(recs[0], 0.001), recs[1], failing(recs[2], 0.001)]
    assert retire.retire_status(gap, None, holdout_end=2023)["status"] == "WATCH"
    # two fails with no record between them (2024, 2026): consecutive SCORED seasons, so RETIRE, as documented
    skip = [failing(recs[0], 0.001), failing(recs[2], 0.001)]
    assert [r["season"] for r in skip] == [2024, 2026]
    assert retire.retire_status(skip, None, holdout_end=2023)["status"] == "RETIRE-candidate"
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
    # a column first seen in a later fold is classified over the folds that have it
    grown = signs.sign_table([*folds, {"fold_season": 2024, "coef": {"a": 0.6, "e": 1.0}, "coef_se": {"a": 0.1}}])
    g = grown["coefficients"]
    assert g["a"]["class"] == "stable" and g["a"]["absent_folds"] == []
    assert g["b"]["absent_folds"] == [2024] and g["b"]["class"] == "noise_flip"
    assert g["e"] == g["e"] | {"class": "too_few_folds", "absent_folds": [2022, 2023]}
    assert grown["design_changes"] == {"2024": ["b", "c", "d", "e"]}
    # walk-forward folds (nested train seasons): a column may appear, never drop out
    nested = [f | {"train_seasons": list(range(2019, int(f["fold_season"])))} for f in folds]
    later = {"fold_season": 2024, "train_seasons": list(range(2019, 2024)), "coef": {"a": 0.6}, "coef_se": {"a": 0.1}}
    with pytest.raises(ValueError, match="drops out of a later nested fold"):
        signs.sign_table([*nested, later])
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


def test_scored_seasons_are_outside_every_fit_on_the_path() -> None:
    for lg in ("nba", "wnba"):
        fitted = set().union(*cov.PATH_FITS[lg].values())
        assert not set(cov.SEASONS[lg]) & fitted, lg
        assert set(cov.NEAR_HOLDOUT[lg]) <= fitted, lg  # near-holdout means inside some fit
        assert not set(cov.SEASONS[lg]) & set(cov.EXCLUDED[lg]), lg
        assert len(cov.SEASONS[lg]) >= cov.MIN_CLUSTERS, lg


def test_noop_check_with_zero_width_fixed_intervals_is_not_a_pass() -> None:
    """One sim: both arms' intervals have zero width; the ratio is undefined (None, valid JSON), not inf."""
    flat = {"width": 0.0, "coverage": 0.5}
    out = cov.noop_check(flat, flat)
    assert out["width_ratio"] is None and out["status"] == "BUG"
    json.dumps(out, allow_nan=False)


def test_report_without_an_in_season_cutoff() -> None:
    """A season scored at the preseason only (NBA 2011-12 opened Dec 25) still reports."""
    with pytest.MonkeyPatch.context() as mp:
        _fixture_loaders(mp)
        rows = cov.run_season("nba", 2026, n_sims=200, seed=1)
    rep = cov.report(rows.filter(pl.col("cutoff") == "preseason"), "nba")
    assert rep["cutoffs"]["in_season"] == {
        "reading": "TOO_FEW_SEASONS",
        "tolerance": None,
        "reason": "no in_season cutoff",
    }
    assert rep["noop_check"]["in_season_reported_only"] is None
    assert rep["noop_check"]["status"] == "PASS"  # the preseason check still runs


def test_sign_classify_rejects_non_finite_estimates() -> None:
    for bad in (float("nan"), float("inf")):
        with pytest.raises(ValueError, match="non-finite"):
            signs.classify({2022: (bad, 0.1), 2023: (-0.4, 0.1)})


def test_cfb_qbr_row_names_its_2026_holdout() -> None:
    """The row's holdout is the gate record's frozen season, after every train season."""
    row = retire.ledger_row(_ledger(), "cfb", "qbr_model")
    gate = json.loads((ROOT / "sportsdataverse/cfb/models/qbr_model.gate.json").read_text(encoding="utf-8"))
    assert row["holdout_seasons"] == [gate["holdout"]["season"]] * 2
    assert min(row["holdout_seasons"]) > max(row["trained_seasons"])
