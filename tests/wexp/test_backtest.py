"""Walk-forward backtest driver tests.

The driver owns the leakage boundary: engines see only completed prior
games plus an outcome-stripped slate. The Elo parity gate proves the
week loop reconstructs the exact chronological walk on the real NFL
fixture (821 games); the postseason test proves the CFB week-reset
cannot reorder the walk. Never lower a gate to make it pass — debug the
driver instead.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.backtest import elo_predictor, run_backtest
from sportsdataverse.wexp.elo import EloConfig, elo_ratings
from sportsdataverse.wexp.oracle_market import nfl_market_oracle_from_schedule
from sportsdataverse.wexp.variants import VariantConfig, variant_hash

FIXDIR = Path(__file__).resolve().parents[1] / "fixtures" / "wexp"

ELO_VARIANT = VariantConfig(
    core="elo_margin",
    response="raw",
    opponent_adjust="none",
    prior="carryover",
    wp_map="elo_logistic",
    hfa="fixed",
)


@pytest.fixture(scope="module")
def nfl_oracle() -> pl.DataFrame:
    return nfl_market_oracle_from_schedule(pl.read_parquet(FIXDIR / "nfl_schedule_sample.parquet"))


def test_slate_never_carries_outcomes(nfl_oracle):
    """Engines must be structurally unable to read the slate's results."""

    def spy(history, slate, store):
        assert "home_win" not in slate.columns
        assert "home_margin" not in slate.columns
        assert history.filter(pl.col("home_margin").is_null()).height == 0
        if history.height:
            last = history.select(pl.col("season").max()).item()
            assert last <= slate["season"][0]
        return [0.5] * slate.height

    probs, rows = run_backtest(nfl_oracle, spy, model_id="spy", variant=ELO_VARIANT)
    assert probs.height == nfl_oracle.height == 821  # observed fixture size
    assert probs["p_home"].null_count() == 0


def test_elo_backtest_matches_direct_walk(nfl_oracle):
    """Per-week refits from history reconstruct the exact chronological walk."""
    config = EloConfig()
    probs, _ = run_backtest(nfl_oracle, elo_predictor(config), model_id="elo", variant=ELO_VARIANT)
    direct = elo_ratings(
        nfl_oracle.select("game_id", "season", "week", "home_team", "away_team", "neutral_site", "home_margin"),
        config,
    )
    joined = probs.select("game_id", bt=pl.col("p_home")).join(
        direct.select("game_id", direct=pl.col("p_home")), on="game_id", how="inner"
    )
    assert joined.height == 821  # min-size guard: every game rated on both paths
    assert (joined["bt"] - joined["direct"]).abs().max() < 1e-12


def test_result_rows_keyed_by_variant(nfl_oracle, tmp_path):
    path = tmp_path / "lb.parquet"
    _, rows = run_backtest(nfl_oracle, elo_predictor(), model_id="elo", variant=ELO_VARIANT, path=path)
    assert rows["model_id"].unique().to_list() == ["elo"]
    assert rows["variant_hash"].unique().to_list() == [variant_hash(ELO_VARIANT)]
    assert -1 in rows["season"].to_list()  # pooled row present
    assert set(rows["metric"].unique()) == {"brier", "log_loss", "winner_accuracy", "ece"}
    assert pl.read_parquet(path).height == rows.height


def test_lined_slice_rows(nfl_oracle):
    """Partial market coverage emits like-for-like week_slice='lined' rows.

    The NFL fixture has FULL close coverage -> no lined rows (they would
    duplicate 'all'). The CFB fixture has 16 line-less games -> lined rows
    appear with n == the covered count (observed 767 of 783 scorable).
    """
    from sportsdataverse.wexp.oracle_market import cfb_market_oracle_from_lines

    _, nfl_rows = run_backtest(nfl_oracle, lambda h, s, st: [0.5] * s.height, model_id="spy")
    assert nfl_rows.filter(pl.col("week_slice") == "lined").height == 0

    cfb = cfb_market_oracle_from_lines(
        pl.read_parquet(FIXDIR / "cfb_line_odds_sample.parquet"),
        pl.read_parquet(FIXDIR / "cfb_schedule_sample.parquet"),
    )
    _, cfb_rows = run_backtest(cfb, lambda h, s, st: [0.5] * s.height, model_id="spy")
    lined = cfb_rows.filter((pl.col("week_slice") == "lined") & (pl.col("season") == -1))
    full = cfb_rows.filter((pl.col("week_slice") == "all") & (pl.col("season") == -1))
    assert lined.height > 0
    assert lined["n"][0] < full["n"][0]  # strictly the covered subset
    assert lined["n"][0] >= 760  # observed 767


def test_prob_validation(nfl_oracle):
    with pytest.raises(ValueError, match="length"):
        run_backtest(nfl_oracle, lambda h, s, st: [0.5], model_id="bad")
    with pytest.raises(ValueError, match="probabilit"):
        run_backtest(nfl_oracle, lambda h, s, st: [1.5] * s.height, model_id="bad")


def test_postseason_walked_after_regular():
    """CFB postseason week numbers reset to 1; the walk must not reorder."""
    games = pl.DataFrame(
        {
            "league": ["cfb"] * 3,
            "game_id": ["g1", "g2", "g3"],
            "season": [2024] * 3,
            "week": [1, 2, 1],
            "season_type": ["regular", "regular", "postseason"],
            "home_win": [1, 0, 1],
            "home_margin": [7.0, -3.0, 10.0],
        }
    ).with_columns(pl.col("home_win").cast(pl.Int8), pl.col("season").cast(pl.Int32), pl.col("week").cast(pl.Int32))
    seen: list[list[str]] = []

    def spy(history, slate, store):
        seen.append(sorted(history["game_id"].to_list()))
        return [0.5] * slate.height

    run_backtest(games, spy, model_id="spy")
    # the postseason game is walked LAST, with both regular games in history
    assert seen == [[], ["g1"], ["g1", "g2"]]


def test_duplicate_game_id_refused():
    games = pl.DataFrame(
        {
            "league": ["nfl", "nfl"],
            "game_id": ["g1", "g1"],
            "season": pl.Series([2024, 2024], dtype=pl.Int32),
            "week": pl.Series([1, 1], dtype=pl.Int32),
            "season_type": ["REG", "REG"],
            "home_win": pl.Series([1, 0], dtype=pl.Int8),
            "home_margin": [7.0, -7.0],
        }
    )
    with pytest.raises(ValueError, match="duplicate"):
        run_backtest(games, lambda h, s, st: [0.5] * s.height, model_id="spy")
