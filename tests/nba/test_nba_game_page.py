"""nba_game_page: the game-page derivations shared by the GOP processor and the sdv-db stage."""

from __future__ import annotations

from pathlib import Path


from sportsdataverse.nba import nba_game_predict

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "espn"


def test_in_game_win_prob_is_not_memoised_on_the_frame() -> None:
    # lru_cache keyed by DataFrame identity would pin every scored game in memory for the
    # life of the Flask processor; only the artifact loader may cache.
    assert not hasattr(nba_game_predict.nba_in_game_win_prob, "cache_info")
    assert hasattr(nba_game_predict._load_in_game_artifact, "cache_info")
