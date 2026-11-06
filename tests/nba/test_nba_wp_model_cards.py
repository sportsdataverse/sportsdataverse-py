"""BB-D25: the NBA / WNBA in-game WP model cards and the rated pregame prior.

* The cards (``sportsdataverse/nba/models/<lg>_in_game_wp.card.json``) are rebuilt here from the committed
  artefacts by ``tools/models/nba_prediction/wp_model_cards.py``; any hand edit or stale card fails, and so
  does a card whose metrics were scored with another booster or fit report (their sha256 is pinned). The
  holdout metrics themselves need the network to recompute (``wp_model_cards.py evaluate``).
* The prior gate is recomputed from the committed per-game holdout rows
  (``tests/fixtures/nba_prediction/<lg>_rated_prior_games.parquet``) through the shipped ``home_prior``:
  the rated prior must beat the home-court fallback by more than two game-clustered standard errors of
  log loss, so a ``home_prior`` that ignored ``rated_home_prob`` fails it.
* The as-of boundary is checked on real WNBA 2025 possessions: a game is rated from the games dated
  strictly before it, so the same-day possessions must not move its rating, and the preseason prior reads
  the season before.

Every threshold is an observed value (stated at its assert) with a margin; never lower one to pass.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

pytest.importorskip("xgboost")

from sportsdataverse.nba.nba_game_page import home_prior, score_win_prob  # noqa: E402
from sportsdataverse.errors import SeasonNotFoundError  # noqa: E402
from sportsdataverse.nba import nba_rated_prior  # noqa: E402
from sportsdataverse.nba.nba_rated_prior import nba_rated_priors, rated_home_probs  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests" / "fixtures" / "nba_prediction"
MODELS = ROOT / "sportsdataverse" / "nba" / "models"
LEAGUES = ("nba", "wnba")
# observed 2026-11-05: (finished games, games rated); unrated: an All-Star game, the WNBA All-Star and Commissioner's Cup finals
GATE_GAMES = {"nba": (3961, 3960), "wnba": (576, 572)}


@pytest.fixture(scope="module")
def cards() -> Any:
    spec = importlib.util.spec_from_file_location(
        "wp_model_cards", ROOT / "tools/models/nba_prediction/wp_model_cards.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _card(lg: str) -> dict[str, Any]:
    return json.loads((MODELS / f"{lg}_in_game_wp.card.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("lg", LEAGUES)
def test_card_is_built_from_the_committed_artefacts(cards: Any, lg: str) -> None:
    """Rebuilding the card from the fit report, the card metrics and the booster reproduces the file."""
    assert cards.build_card(lg) == _card(lg)


@pytest.mark.parametrize("lg", LEAGUES)
def test_card_pins_the_bundled_booster(lg: str) -> None:
    card = _card(lg)
    assert card["sha256"] == hashlib.sha256((MODELS / f"{lg}_in_game_wp.ubj").read_bytes()).hexdigest()
    assert not set(card["training_seasons"]) & set(card["holdout_seasons"])
    # every pass settles at the horn, the final minute never moves away from the leader, and a 20-point horn
    # step stays rare: observed at most 0.00303 (NBA with the line withheld, 4 of 1,321 games)
    for name, block in card["holdout_metrics"].items():
        assert block["final"]["brier"] == 0.0, name
        assert block["late_game"]["horn_jump_share"] <= 0.005, name
        assert block["late_game"]["final_minute_away_share"] == 0.0, name


@pytest.mark.parametrize("lg", LEAGUES)
def test_rated_prior_beats_the_home_court_fallback(cards: Any, lg: str) -> None:
    """Recomputed through the shipped ``home_prior`` on the committed holdout games; equals the card's gate."""
    games = pl.read_parquet(FIX / f"{lg}_rated_prior_games.parquet")
    gate = cards.prior_gate(cards.prior_probs(games, lg))
    card = json.loads(json.dumps(_card(lg)["pregame_prior"]))
    assert gate == card["gate"]
    by_season = {
        str(s): cards.prior_gate(cards.prior_probs(games.filter(pl.col("season") == s), lg))
        for s in card["holdout_seasons"]
    }
    assert by_season == card["gate_by_season"]
    games_n, rated_n = GATE_GAMES[lg]
    assert gate["games"] >= games_n and gate["games_rated"] >= rated_n  # a broken name join drops ratings
    assert card["constants"] == cards.rated_prior_constants(cards.LEAGUE_IDS[lg])  # scored with today's constants
    rated = gate["paired_log_loss"]["rated - hfa"]
    # observed (2026-11-05): NBA -0.0756 +- 0.0057 over 3,961 games, WNBA -0.0639 +- 0.0165 over 576
    assert rated["diff"] + 2 * rated["se"] < 0
    assert all(gate["pass"].values())
    if lg == "wnba":  # no line in the release: passing the rating changes every game
        served = gate["paired_log_loss"]["line_or_rated - line_or_hfa"]
        assert served["diff"] + 2 * served["se"] < 0


def test_home_prior_order_line_then_rating_then_home_court() -> None:
    assert home_prior(-4.0, True, rated_home_prob=0.9) == home_prior(-4.0, True)
    assert home_prior(None, False, rated_home_prob=0.71) == (0.71, "rated")
    for bad in (float("nan"), float("inf"), 1.5, -0.1):  # not a probability: home court
        assert home_prior(2.5, False, rated_home_prob=bad)[1] == "hfa"
    assert home_prior(None, False, league="wnba", neutral_site=True, rated_home_prob=0.6) == (0.6, "rated")


def test_score_win_prob_serves_the_rated_prior() -> None:
    one = pl.read_parquet(FIX / "wnba_late_game_2025.parquet")
    one = one.filter(pl.col("game_id") == one["game_id"][0])
    hfa = score_win_prob(one, league="wnba")
    rated = score_win_prob(one, league="wnba", rated_home_prob=0.8)
    assert hfa["prior_source"][0] == "hfa" and rated["prior_source"][0] == "rated"
    assert rated["pregame_home_prob"][0] == 0.8
    live = ~((rated["type_text"] == "End Game") | (rated["type_text"] == "End Period"))
    assert (rated.filter(live)["home_win_prob"] >= hfa.filter(live)["home_win_prob"]).all()
    assert (rated.filter(live)["home_win_prob"] > hfa.filter(live)["home_win_prob"]).any()


def test_rating_uses_only_games_before_the_tip() -> None:
    """Same-day possessions must not move a game's rating; earlier ones must."""
    rows = pl.read_parquet(FIX / "wnba_rated_prior_asof_2025.parquet")
    games = pl.read_parquet(FIX / "wnba_rated_prior_asof_games_2025.parquet").with_columns(
        pl.lit(False).alias("neutral_site")
    )
    last = games["game_date"].max()
    day = games.filter(pl.col("game_date") == last)
    assert rows.filter(pl.col("game_date") == last).height > 0  # the same-day possessions are in the fixture
    full = rated_home_probs(rows, day, league_id="10")
    before = rated_home_probs(rows.filter(pl.col("game_date") < last), day, league_id="10")
    assert full["rated_home_prob"].to_list() == before["rated_home_prob"].to_list()
    earlier = rated_home_probs(rows.filter(pl.col("game_date") < games["game_date"].min()), day, league_id="10")
    assert earlier["rated_home_prob"].to_list() != full["rated_home_prob"].to_list()


def test_nba_rated_priors_wiring_is_as_of(monkeypatch: pytest.MonkeyPatch) -> None:
    """End to end on the fixture: the prior reads the season before, and a game ignores its own day's possessions."""
    rows = pl.read_parquet(FIX / "wnba_rated_prior_asof_2025.parquet")
    games = pl.read_parquet(FIX / "wnba_rated_prior_asof_games_2025.parquet")
    espn = games.select(
        pl.lit(2025).alias("season"),
        pl.int_range(pl.len()).alias("game_id"),
        "game_date",
        pl.int_range(pl.len()).alias("home_id"),
        pl.int_range(pl.len()).alias("away_id"),
        pl.lit(False).alias("neutral_site"),
        pl.col("_h").alias("home_display_name"),
        pl.col("_a").alias("away_display_name"),
    )
    from sportsdataverse.wnba import wnba_loaders

    monkeypatch.setattr(wnba_loaders, "load_wnba_schedule", lambda seasons: espn)
    last = games["game_date"].max()

    def run(possessions: pl.DataFrame) -> tuple[list[int], pl.DataFrame]:
        calls: list[int] = []

        def fake(league_id: str, season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
            calls.append(season)
            if season != 2025:
                raise SeasonNotFoundError(f"no {season} in the fixture")
            return possessions, games

        monkeypatch.setattr(nba_rated_prior, "_possession_rows", fake)
        return calls, nba_rated_priors(2025, league_id="10")

    calls, full = run(rows)
    assert calls == [2025, 2024]
    assert full["rated_home_prob"].null_count() == 0 and full.height == games.height
    _, cut = run(rows.filter(pl.col("game_date") < last))
    on_last = pl.col("game_date") == last
    assert full.filter(on_last)["rated_home_prob"].to_list() == cut.filter(on_last)["rated_home_prob"].to_list()
