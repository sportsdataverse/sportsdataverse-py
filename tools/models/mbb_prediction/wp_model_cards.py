"""Model cards and the holdout gate for the men's / women's college in-game WP boosters (BB-D25).

The college side of #1116's NBA / WNBA cards (``tools/models/nba_prediction/wp_model_cards.py``), whose
``card_metrics``, ``prior_probs`` and ``prior_gate`` are imported here, not re-implemented. Two steps, so
every number on a card comes from a committed artefact:

1. ``evaluate`` (network: the ESPN pbp / schedule / team-box releases). Every finished, untied game of the
   holdout season (2026) and the confirmation season (2024) goes through ``score_win_prob`` with the
   shipped booster and with the booster it replaced (``tests/fixtures/<lg>_prediction/baseline_in_game_wp``,
   the bundled model at ``train_in_game_wp.BASELINE_REF``), under both serving priors: ``hfa`` (the game
   page's: home court alone, 0.5 at a neutral site; the college releases carry no lines) and ``rated``
   (the release enrichment's weekly as-of ratings prior). Writes ``in_game_wp_<lg>_card_metrics.json``
   (the card blocks, the holdout gate, the pregame prior gate, and the sha256 of the boosters and the fit
   report) and the prior gate's per-game rows ``tests/fixtures/<lg>_prediction/<lg>_rated_prior_games.parquet``.
2. ``card`` (offline): writes ``sportsdataverse/mbb/models/<lg>_in_game_wp.card.json`` from that json, the
   fit report ``in_game_wp_<lg>_holdout.json`` and the bundled booster. ``tests/mbb/test_mbb_wp_model_cards.py``
   rebuilds the cards and fails on drift.

The holdout gate replaces the legacy ``pbp_sample_2024`` decile gate (``<= 0.03`` in every decile of every
25th play of 2024). That gate read 0.0326 on this refit and 0.0298 on the booster it replaced, with
game-clustered 90% bands that overlap (0.025-0.055 vs 0.019-0.051): on that sample it cannot tell the two
apart, and 2024 is the season the pregame constants were fit on. Provenance, plainly: the owner chose this
replacement (#1104 option (a), 2026-11-06) after the refit failed the old bar. The first draft of the rule
was unpaired (the shipped value at or under the 95th percentile of the replaced booster's own band); every
2026 cell passed it. Review showed it lets a regression of about 1.6 standard errors through, because both
boosters score the same games, so the rule is now the paired comparison below, whose interval is narrower
(the outcome noise both share cancels). A draft that also excused small regressions under fixed bounds
was withdrawn: the bounds were picked with the fit report's point estimates in view. There is no threshold
to tune:

* Season: 2026, disjoint from the training seasons (2022, 2023, 2025) and the design season (2025). The
  same gate on the confirmation season (2024) is reported, not gated.
* Rows: the live rows (the settled horn rows are scored apart as ``final``), per serving prior.
* Blocks: all live rows, each regulation period, overtime, and the final minute of regulation.
* Metrics: Brier and |calibration in the large| (mean prediction minus home win rate) in every block, and
  the worst decile gap (ten equal-width bins, as ``calibration_table``) over all live rows.
* Comparison: paired, shipped minus replaced on the same games, with a 95% interval from 2,000
  game-clustered bootstrap draws (the same draws for both boosters). The replaced booster is fixed (the
  bundled model at ``BASELINE_REF``), so the bar does not move with each refit.
* A block fails when the shipped booster is significantly worse than the replaced one: the interval's lower
  end is above 0. However small the regression, if it is significant it fails.
* Also gated, as on the NBA / WNBA cards: settled rows score Brier 0, the horn-jump share is at most
  0.005 and the final minute never moves away from the leader.

The per-game block totals of both boosters on the holdout are committed
(``tests/fixtures/<lg>_prediction/<lg>_holdout_gate_sums.parquet``), so the test recomputes the gate offline,
and the scored games are listed in ``in_game_wp_<lg>_partition.parquet`` beside this script.

Usage::

    uv run python tools/models/mbb_prediction/wp_model_cards.py evaluate --league mbb
    uv run python tools/models/mbb_prediction/wp_model_cards.py evaluate --league wbb
    uv run python tools/models/mbb_prediction/wp_model_cards.py card
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
MODELS_DIR = REPO / "sportsdataverse" / "mbb" / "models"


def _load(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


fit = _load("train_college_in_game_wp", HERE / "train_in_game_wp.py")
pro = _load("nba_wp_model_cards", REPO / "tools" / "models" / "nba_prediction" / "wp_model_cards.py")

LEAGUES = ("mbb", "wbb")
SEASONS = {"holdout": 2026, "confirmation": fit.CONFIRMATION_SEASON}
#: The pregame prior gate's seasons: disjoint from 2024, the season its constants (hfa, margin_sd, em_scale)
#: were fit on.
PRIOR_HOLDOUT = [2025, 2026]
GATE_DRAWS = 2000
GATE_RULE = (
    "2026 holdout, live rows, per serving prior: Brier and |calibration in the large| in every block (all live rows, "
    "each period, overtime, the final minute of regulation) and the worst decile gap over all live rows. Paired "
    "shipped minus replaced, 95% interval from 2,000 game-clustered bootstrap draws; a block fails when the interval "
    "is above 0 (the shipped booster is significantly worse than the fixed replaced one)"
)
CAVEATS = [
    "Inputs are the margin, the clock, possession and the pregame prior only: no fouls, timeouts, bonus or lineups.",
    "The college releases carry no lines: the game page serves home court alone (0.5 at a neutral site), while the "
    "booster trains on the weekly as-of ratings prior, so the game page's prior is the weaker of the two served.",
    "Games with no as-of rating (the season's first days, about 10% of games) are not trained on; served, they get "
    "the HFA fallback.",
    "Train and holdout seasons are whole seasons: out of sample in time, sharing rules eras.",
    "2024 (the confirmation season) is in no training season, but the pregame constants the rated prior reads were "
    "fit on it; its gate is reported, not gated.",
    "The holdout gate is a paired no-regression bar against the fixed booster this refit replaced (the card script's "
    "docstring): it is not an absolute calibration bar.",
    "The WBB 2024 release has two wrong horn rows (games 401574803 and 401601119), which the game page settles on "
    "their own score: settled Brier 0.0002 on that season.",
]
#: League-specific caveats; numbers live in the gate blocks they name, not here.
#: Gate blocks that FAIL the holdout gate and ship anyway by the owner's decision. They are recorded as exceptions,
#: never as passes; the test pins this set exactly, so any other failing block, or one of these passing, goes red.
OWNER_ACCEPTED_EXCEPTIONS = {
    "mbb": {
        "blocks": {"hfa": ["abs_citl:2"], "rated": ["abs_citl:1"]},
        "decided_by": "owner (relayed by the lane-10 coordinator)",
        "decided_at": "2026-11-06 04:19 ET",
        "decision": "ship both leagues (#1104)",
    },
}
LEAGUE_CAVEATS = {
    "mbb": [
        "2026 holdout gate (paired, the gate of record): two |calibration in the large| blocks are significantly worse "
        "than the replaced booster's, the second half under the game-page prior and the first half under the rated "
        "prior (holdout_gate.by_prior); Brier is significantly better in every block. They are owner-accepted "
        "exceptions, not passes (holdout_gate.owner_accepted_exceptions).",
        "2024 confirmation, rated prior: the worst decile gap is worse than the replaced booster's at the point "
        "estimate and its paired 95% interval only just includes 0 (confirmation_gate_reported.rated, "
        "max_decile_gap:live): a weak pass. The first-draft unpaired rule passed it only through the replaced "
        "booster's wide band.",
    ],
    "wbb": [],
}


def baseline(lg: str) -> Any:
    """The booster this refit replaced (the bundled model at ``fit.BASELINE_REF``), from its test fixture."""
    import xgboost as xgb

    b = xgb.Booster()
    path = (
        REPO
        / "tests"
        / "fixtures"
        / f"{lg}_prediction"
        / "baseline_in_game_wp"
        / f"{lg}_in_game_wp_{fit.BASELINE_REF}.ubj"
    )
    b.load_model(bytearray(path.read_bytes()))
    return b


def _blocks(scored: pl.DataFrame, reg: int) -> pl.DataFrame:
    """Per game and gate block: plays ``n``, summed squared error, summed prediction, summed outcome."""
    live = scored.filter(pro._game_end(reg) == False)  # noqa: E712
    period = pl.when(pl.col("period_number") > reg).then(pl.lit("ot")).otherwise(pl.col("period_number").cast(pl.Utf8))
    final_minute = (pl.col("period_number") == reg) & (pl.col("start_game_seconds_remaining") <= 60)
    decile = pl.lit("bin") + (pl.col("home_win_prob").clip(0.0, 0.9999) * 10).floor().cast(pl.Int64).cast(pl.Utf8)
    parts = [
        live.with_columns(pl.lit("live").alias("block")),
        live.with_columns(period.alias("block")),
        live.filter(final_minute).with_columns(pl.lit("final_minute").alias("block")),
        live.with_columns(decile.alias("block")),
    ]
    return (
        pl.concat(p.select("game_id", "block", "home_win_prob", "y") for p in parts)
        .group_by("game_id", "block")
        .agg(
            pl.len().alias("n"),
            ((pl.col("home_win_prob") - pl.col("y")) ** 2).sum().alias("se"),
            pl.col("home_win_prob").sum().alias("sp"),
            pl.col("y").sum().alias("sy"),
        )
    )


def _metric_draws(sums: pl.DataFrame, games: list[str], w: np.ndarray) -> dict[str, np.ndarray]:
    """Each gate metric under each bootstrap draw (row 0 of ``w`` is all ones: the observed value)."""
    idx = {g: i for i, g in enumerate(games)}
    tot: dict[str, dict[str, np.ndarray]] = {}
    for (block,), sub in sums.group_by("block"):
        rows = np.array([idx[g] for g in sub["game_id"].to_list()])
        tot[str(block)] = {k: w[:, rows] @ sub[k].to_numpy().astype(np.float64) for k in ("n", "se", "sp", "sy")}
    out = {}
    for block, t in tot.items():
        if not block.startswith("bin"):
            out[f"brier:{block}"] = t["se"] / t["n"]
            out[f"abs_citl:{block}"] = np.abs(t["sp"] - t["sy"]) / t["n"]
    bins = [t for b, t in tot.items() if b.startswith("bin")]
    with np.errstate(invalid="ignore", divide="ignore"):
        gaps = np.stack([np.abs(t["sp"] - t["sy"]) / t["n"] for t in bins])
    out["max_decile_gap:live"] = np.nanmax(gaps, axis=0)  # a bin empty in a draw has no gap
    return out


def gate_from_sums(a: pl.DataFrame, b: pl.DataFrame) -> dict[str, Any]:
    """The holdout gate (module docstring) from per-game block totals ``a`` (shipped) and ``b`` (replaced)."""
    games = sorted(set(a["game_id"].to_list()))
    if games != sorted(set(b["game_id"].to_list())):
        raise ValueError("both boosters must score the same games")
    w = np.random.default_rng(0).multinomial(len(games), np.full(len(games), 1 / len(games)), size=GATE_DRAWS)
    w = np.vstack([np.ones(len(games)), w])
    da, db = _metric_draws(a, games, w), _metric_draws(b, games, w)
    out: dict[str, Any] = {}
    for key in sorted(da):
        lo, hi = np.percentile(da[key][1:] - db[key][1:], [2.5, 97.5])
        shipped, replaced = float(da[key][0]), float(db[key][0])
        worse = bool(lo > 0)
        out[key] = {
            "shipped": round(shipped, 5),
            "replaced": round(replaced, 5),
            "paired_delta": round(shipped - replaced, 5),
            "ci95": [round(float(lo), 5), round(float(hi), 5)],
            "pass": not worse,
        }
    return out


def holdout_gate(new: pl.DataFrame, old: pl.DataFrame, reg: int) -> dict[str, Any]:
    """The holdout gate of the shipped booster's scored rows ``new`` against the replaced booster's ``old``."""
    return gate_from_sums(_blocks(new, reg), _blocks(old, reg))


def _labelled(lg: str, pbp: pl.DataFrame, model: Any, priors: dict[str, float] | None) -> pl.DataFrame:
    """``fit.serve`` output for the finished, untied games, with the outcome ``y``."""
    done = fit._finished(pbp).rename({"home_win": "y"})
    return fit.serve(lg, pbp.join(done.select("game_id"), on="game_id", how="semi"), model, priors).join(
        done, on="game_id"
    )


def prior_games(lg: str, season: int, pbp: pl.DataFrame, priors: dict[str, float]) -> pl.DataFrame:
    """The prior gate's rows: one per finished, untied game (``pro.GAME_COLS``; no college lines)."""
    done = fit._finished(pbp).rename({"home_win": "y"})
    site = pbp.group_by("game_id").agg(pl.col("neutral_site").first())
    return (
        done.join(site, on="game_id")
        .with_columns(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.lit(None, dtype=pl.Float64).alias("home_team_spread"),
            pl.lit(False).alias("game_spread_available"),
            pl.col("game_id").replace_strict(priors, default=None, return_dtype=pl.Float64).alias("rated_home_prob"),
        )
        .select(pro.GAME_COLS)
        .sort("game_id")
    )


def evaluate(lg: str) -> dict[str, Any]:
    import xgboost as xgb

    shipped, replaced = xgb.Booster(), baseline(lg)
    shipped.load_model(str(MODELS_DIR / fit.LEAGUES[lg]["artifact"]))
    report: dict[str, Any] = {
        "league": lg,
        "booster_holdout_season": SEASONS["holdout"],
        "confirmation_season": SEASONS["confirmation"],
        "booster_sha256": pro._sha(MODELS_DIR / fit.LEAGUES[lg]["artifact"]),
        "replaced_booster_ref": fit.BASELINE_REF,
        "fit_report_sha256": pro._sha(HERE / f"in_game_wp_{lg}_holdout.json"),
        "xgboost_version": xgb.__version__,
        "gate_rule": {"draws": GATE_DRAWS, "interval": 95, "seed": 0},
    }
    prior_rows: list[pl.DataFrame] = []
    gate_sums: list[pl.DataFrame] = []
    partition: list[pl.DataFrame] = []
    for role, season in (("confirmation", SEASONS["confirmation"]), *(("prior", s) for s in PRIOR_HOLDOUT)):
        if role == "prior" and season == SEASONS["holdout"]:
            role = "holdout"
        pbp = fit._load_pbp(lg, season)
        priors = fit._priors(lg, season)
        if season in PRIOR_HOLDOUT:
            prior_rows.append(prior_games(lg, season, pbp, priors))
        if role == "prior":
            continue
        reg = fit.game_regulation_periods(lg, season=season)
        blocks, gates, sums = {}, {}, {}
        for name, pm in (("hfa", None), ("rated", priors)):
            new = _labelled(lg, pbp, shipped, pm)
            blocks[name] = pro.card_metrics(lg, new)
            a, b = _blocks(new, reg), _blocks(_labelled(lg, pbp, replaced, pm), reg)
            gates[name] = gate_from_sums(a, b)
            if role == "holdout":
                for f, m in ((a, "shipped"), (b, "replaced")):
                    gate_sums.append(f.with_columns(pl.lit(name).alias("prior"), pl.lit(m).alias("model")))
            sums[name] = fit._game_sums(fit._scored_rows(new, reg))
            print(f"{lg} {season} {name}: {sum(not g['pass'] for g in gates[name].values())} gate blocks fail")
        split = new.select("game_id").unique()
        partition.append(split.with_columns(pl.lit(season).alias("season"), pl.lit(role).alias("split")))
        report[role] = {
            "season": season,
            "blocks": blocks,
            "gate": gates,
            "rated_minus_hfa": fit.paired_deltas(sums["rated"], sums["hfa"]),
        }
    sums_path = REPO / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_holdout_gate_sums.parquet"
    pl.concat(gate_sums).sort("prior", "model", "game_id", "block").write_parquet(sums_path, compression="zstd")
    pl.concat(partition).sort("season", "game_id").write_parquet(HERE / f"in_game_wp_{lg}_partition.parquet")
    gate_rows = pro.prior_probs(pl.concat(prior_rows), lg)
    fixture = REPO / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_rated_prior_games.parquet"
    gate_rows.select(pro.GAME_COLS).write_parquet(fixture, compression="zstd")
    report["prior_gate"] = pro.prior_gate(gate_rows)
    report["prior_gate_by_season"] = {
        str(s): pro.prior_gate(gate_rows.filter(pl.col("season") == s)) for s in PRIOR_HOLDOUT
    }
    path = HERE / f"in_game_wp_{lg}_card_metrics.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    return report


def build_card(lg: str) -> dict[str, Any]:
    """The card, from the committed fit report, card-metrics json and booster only (no network)."""
    import xgboost as xgb

    artifact = MODELS_DIR / fit.LEAGUES[lg]["artifact"]
    fitted = json.loads((HERE / f"in_game_wp_{lg}_holdout.json").read_text(encoding="utf-8"))
    metrics = json.loads((HERE / f"in_game_wp_{lg}_card_metrics.json").read_text(encoding="utf-8"))
    booster = xgb.Booster()
    booster.load_model(str(artifact))
    assert booster.attr("holdout_season") == str(fitted["holdout_season"]) == str(metrics["booster_holdout_season"])
    assert booster.feature_names == fitted["features"], "booster features differ from the fit report"
    # the metrics must have been scored with THIS booster and fit report: rerun ``evaluate`` after a refit
    assert metrics["booster_sha256"] == pro._sha(artifact), f"{lg} card metrics were scored with another booster"
    assert metrics["fit_report_sha256"] == pro._sha(HERE / f"in_game_wp_{lg}_holdout.json"), "fit report changed"
    assert not set(fitted["train_seasons"]) & {fitted["holdout_season"], fitted["confirmation_season"]}
    return {
        "model_type": f"{lg}_in_game_wp",
        "xgboost_version": metrics["xgboost_version"],
        "objective": fitted["params"]["objective"],
        "features": fitted["features"],
        "n_features": len(fitted["features"]),
        "label": "home team won (ties dropped)",
        "training_seasons": fitted["train_seasons"],
        "early_stopping": {"validation_season": fitted["validation_season"], "refit_on_all_training_seasons": True},
        "design": {
            "hfa_prior_share": fitted["hfa_prior_share"],
            "keep_unrated": fitted["keep_unrated"],
            "picked_by": f"tools/models/mbb_prediction/in_game_wp_{lg}_validation.json (2025 only)",
        },
        "holdout_seasons": [fitted["holdout_season"]],
        "confirmation_season": fitted["confirmation_season"],
        "n_training_rows": fitted["n_train_plays"],
        "hyperparameters": fitted["params"],
        "num_boost_round": fitted["rounds"],
        "source": f"sportsdataverse-data espn_{'mens' if lg == 'mbb' else 'womens'}_college_basketball_pbp releases",
        "fitting_script": booster.attr("fitting_script"),
        "card_script": "tools/models/mbb_prediction/wp_model_cards.py",
        "sha256": pro._sha(artifact),
        "fit_holdout": {k: fitted[k] for k in ("baseline_ref", "holdout", "confirmation")},
        "holdout_metrics": metrics["holdout"]["blocks"],
        "confirmation_metrics": metrics["confirmation"]["blocks"],
        "holdout_gate": {
            "rule": GATE_RULE,
            "replaced_booster_ref": metrics["replaced_booster_ref"],
            "season": metrics["holdout"]["season"],
            "by_prior": metrics["holdout"]["gate"],
            "owner_accepted_exceptions": OWNER_ACCEPTED_EXCEPTIONS.get(lg),
        },
        "confirmation_gate_reported": metrics["confirmation"]["gate"],
        # the confirmation cells where the shipped booster is significantly worse (reported, not gated)
        "confirmation_significantly_worse": sorted(
            f"{prior} {key}"
            for prior, gate in metrics["confirmation"]["gate"].items()
            for key, cell in gate.items()
            if not cell["pass"]
        ),
        "rated_minus_hfa_in_game": {
            "holdout": metrics["holdout"]["rated_minus_hfa"],
            "confirmation": metrics["confirmation"]["rated_minus_hfa"],
        },
        "pregame_prior": {
            "rated": "the release enrichment's weekly as-of ratings prior (mbb_win_prob._pregame_probs: games rated "
            "on games strictly before the Monday of their week), passed to home_prior as rated_home_prob",
            "hfa": "home_prior without it (what the game page serves today): home court, 0.5 at a neutral site",
            "holdout_seasons": PRIOR_HOLDOUT,
            "gate": metrics["prior_gate"],
            "gate_by_season": metrics["prior_gate_by_season"],
        },
        "caveats": CAVEATS + LEAGUE_CAVEATS[lg],
    }


def write_cards() -> None:
    for lg in LEAGUES:
        path = MODELS_DIR / f"{lg}_in_game_wp.card.json"
        path.write_text(json.dumps(build_card(lg), indent=2) + "\n", encoding="utf-8", newline="\n")
        print(path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("evaluate").add_argument("--league", choices=LEAGUES, required=True)
    sub.add_parser("card")
    args = ap.parse_args(argv)
    if args.cmd == "evaluate":
        report = evaluate(args.league)
        print(json.dumps({k: report[k] for k in ("prior_gate",)}, indent=2))
    else:
        write_cards()
    return 0


if __name__ == "__main__":
    sys.exit(main())
