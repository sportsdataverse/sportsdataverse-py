"""BB-D25: the men's / women's college in-game WP model cards and the college holdout gate.

* The cards (``sportsdataverse/mbb/models/<lg>_in_game_wp.card.json``) are rebuilt here from the committed
  artefacts by ``tools/models/mbb_prediction/wp_model_cards.py``; a hand edit or a stale card fails, and so does a
  card whose metrics were scored with another booster or fit report (their sha256 is pinned). Recomputing the
  holdout metrics needs the network (``wp_model_cards.py evaluate``).
* The holdout gate (the script's docstring has the rule and its derivation) replaces the retired
  ``pbp_sample_2024`` decile gate: on the 2026 holdout, by period, live and final, under both serving priors, the
  shipped booster must not be significantly worse than the fixed booster it replaced (paired, game-clustered 95%
  interval). It is recomputed here from the committed per-game block totals, and the gate function is shown to go
  red on real rows.
* The pregame prior gate is recomputed from the committed per-game rows through the shipped ``home_prior``.

Every threshold is an observed value (stated at its assert); never lower one to pass.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

xgb = pytest.importorskip("xgboost")

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "sportsdataverse" / "mbb" / "models"
LEAGUES = ("mbb", "wbb")
PRIORS = ("hfa", "rated")
# observed 2026-11-06, 2025 + 2026: (finished untied games, games with an as-of rating; the rest are the season's
# first days, which fall through to home court)
GATE_GAMES = {"mbb": (12408, 11134), "wbb": (11483, 10355)}
# the rated prior rates a season with one zero-turnover box score; the drop warning is asserted in
# tests/mbb/test_mbb_team_ratings.py
pytestmark = pytest.mark.filterwarnings(r"ignore:raw_game_efficiency\x3a dropped:UserWarning")


@pytest.fixture(scope="module")
def cards() -> Any:
    spec = importlib.util.spec_from_file_location(
        "college_wp_model_cards", ROOT / "tools/models/mbb_prediction/wp_model_cards.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _card(lg: str) -> dict[str, Any]:
    return json.loads((MODELS / f"{lg}_in_game_wp.card.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("lg", LEAGUES)
def test_card_is_built_from_the_committed_artefacts(cards: Any, lg: str) -> None:
    assert cards.build_card(lg) == _card(lg)


@pytest.mark.parametrize("lg", LEAGUES)
def test_card_pins_the_bundled_booster(lg: str) -> None:
    card = _card(lg)
    assert card["sha256"] == hashlib.sha256((MODELS / f"{lg}_in_game_wp.ubj").read_bytes()).hexdigest()
    assert not set(card["training_seasons"]) & {*card["holdout_seasons"], card["confirmation_season"]}
    # as on the NBA / WNBA cards: every pass settles at the horn, the final minute never moves away from the
    # leader, and a horn step stays rare (observed: at most 0.0003, MBB rated prior)
    for season in ("holdout_metrics", "confirmation_metrics"):
        for prior, block in card[season].items():
            # WBB 2024 settles 2 of 9,937 horn rows wrong because the release rows are wrong: game 401574803's
            # overtime "End Period" carries 77-78 after 81-78, and 401601119 repeats "End Game" with the scores
            # swapped (58-97); the game page settles a row on its own score
            allowed = 0.0002 if (lg, season) == ("wbb", "confirmation_metrics") else 0.0
            assert block["final"]["brier"] <= allowed, (season, prior, block["final"])
            assert block["late_game"]["horn_jump_share"] <= 0.005, (season, prior)
            assert block["late_game"]["final_minute_away_share"] == 0.0, (season, prior)


# observed 2026-11-06 on the 2026 holdout: games per league, the same under both priors
HOLDOUT_GAMES = {"mbb": 6275, "wbb": 6011}
# The gate's verdict, NOT tuned away: the MBB blocks where the shipped booster is significantly worse than the
# replaced one, shipped as owner-accepted exceptions (2026-11-06, "ship both"; card holdout_gate.owner_accepted_exceptions). Both are |calibration in the large|: second half, game-page
# prior, 0.0093 vs 0.0086 (paired +0.0007, 95% [+0.0003, +0.0011]); first half, rated prior, 0.0117 vs 0.0075
# (+0.0042, [+0.0014, +0.0055]). Brier is significantly better in every MBB block. Any other failing block, or one
# of these passing, fails the test: change this only with the owner's call.
KNOWN_FAILURES = {("mbb", "hfa"): {"abs_citl:2"}, ("mbb", "rated"): {"abs_citl:1"}}


@pytest.mark.parametrize("lg", LEAGUES)
@pytest.mark.parametrize("prior", PRIORS)
def test_holdout_gate(cards: Any, lg: str, prior: str) -> None:
    """Recomputed from the committed per-game block totals, the 2026 holdout gate equals the card's verdict."""
    sums = pl.read_parquet(ROOT / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_holdout_gate_sums.parquet")
    mine = sums.filter(pl.col("prior") == prior)
    gate = cards.gate_from_sums(mine.filter(pl.col("model") == "shipped"), mine.filter(pl.col("model") == "replaced"))
    card = _card(lg)
    assert json.loads(json.dumps(gate)) == card["holdout_gate"]["by_prior"][prior]
    live = mine.filter((pl.col("model") == "shipped") & (pl.col("block") == "live"))
    # a partial evaluate (fewer games) would widen every interval: pin what was scored
    assert live["game_id"].n_unique() == HOLDOUT_GAMES[lg] == card["holdout_metrics"][prior]["games"]
    assert int(live["n"].sum()) == card["holdout_metrics"][prior]["live"]["n"]
    periods = ("1", "2") if lg == "mbb" else ("1", "2", "3", "4")
    blocks = ("live", *periods, "ot", "final_minute")
    assert set(gate) == {f"{m}:{b}" for m in ("brier", "abs_citl") for b in blocks} | {"max_decile_gap:live"}
    for key, cell in gate.items():
        assert cell["pass"] == (cell["ci95"][0] <= 0), key
    failed = {k for k, v in gate.items() if not v["pass"]}
    assert failed == KNOWN_FAILURES.get((lg, prior), set()), {k: gate[k] for k in failed}
    accepted = card["holdout_gate"]["owner_accepted_exceptions"]
    assert set((accepted or {}).get("blocks", {}).get(prior, [])) == failed  # every failure is an accepted exception
    for key in failed:
        assert not gate[key]["pass"], key  # recorded as a failure, never flipped to a pass


def test_decile_blocks_match_calibration_table(cards: Any) -> None:
    """The gate's decile bins are ``calibration_table``'s (same clip, same floor), on real holdout rows."""
    from sportsdataverse._common.metrics import calibration_table

    lg = "mbb"
    rows = pl.read_parquet(ROOT / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_late_game_2026.parquet")
    rows = rows.filter(pl.col("game_id").is_in(rows["game_id"].unique().sort()[:300].implode()))
    scored = cards._labelled(lg, rows, cards.baseline(lg), None)
    live = scored.filter(cards.pro._game_end(2) == False)  # noqa: E712
    bins = cards._blocks(scored, 2).filter(pl.col("block").str.starts_with("bin"))
    mine = bins.group_by("block").agg(pl.col("n", "sp", "sy").sum()).sort("block")
    want = calibration_table(live["y"].to_numpy(), live["home_win_prob"].to_numpy(), n_bins=10)
    assert mine["n"].to_list() == want["n"].to_list()
    assert [round(a / n, 9) for a, n in zip(mine["sp"], mine["n"])] == [round(v, 9) for v in want["mean_pred"]]


def test_holdout_gate_goes_red_on_a_worse_model(cards: Any) -> None:
    """On real holdout rows (the late-game fixture), a model pulled halfway to 0.5 fails the Brier blocks.

    (The decile gap is too noisy to separate the two on these 400 games; the full holdout has about 6,000.)
    """
    lg = "wbb"
    rows = pl.read_parquet(ROOT / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_late_game_2026.parquet")
    rows = rows.filter(pl.col("game_id").is_in(rows["game_id"].unique().sort()[:400].implode()))
    old = cards._labelled(lg, rows, cards.baseline(lg), None)
    same = cards.holdout_gate(old, old, 4)
    assert all(c["pass"] for c in same.values())  # a model never fails against itself
    worse = old.with_columns(((pl.col("home_win_prob") + 0.5) / 2).alias("home_win_prob"))
    gate = cards.holdout_gate(worse, old, 4)
    assert not gate["brier:live"]["pass"] and not gate["brier:final_minute"]["pass"], gate


@pytest.mark.parametrize("lg", LEAGUES)
def test_pregame_prior_gate_is_recomputed(cards: Any, lg: str) -> None:
    """The card's rated-vs-home-court prior gate, recomputed through the shipped ``home_prior``."""
    games = pl.read_parquet(ROOT / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_rated_prior_games.parquet")
    card = json.loads(json.dumps(_card(lg)["pregame_prior"]))
    assert cards.pro.prior_gate(cards.pro.prior_probs(games, lg)) == card["gate"]
    by_season = {
        str(s): cards.pro.prior_gate(cards.pro.prior_probs(games.filter(pl.col("season") == s), lg))
        for s in card["holdout_seasons"]
    }
    assert by_season == card["gate_by_season"]
    games_n, rated_n = GATE_GAMES[lg]
    assert card["gate"]["games"] >= games_n and card["gate"]["games_rated"] >= rated_n  # a broken join drops ratings
    rated = card["gate"]["paired_log_loss"]["rated - hfa"]
    # observed (2026-11-06): MBB -0.0784 +- 0.0056 over 12,408 games, WBB -0.1649 +- 0.0062 over 11,483
    assert rated["diff"] + 2 * rated["se"] < 0
    assert all(card["gate"]["pass"].values())
