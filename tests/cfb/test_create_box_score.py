from sportsdataverse.cfb import CFBPlayProcess
from tests.conftest import skip_if_no_live


@skip_if_no_live
def test_create_box_score_adds_defensive_players_and_specialists():
    # 0.0.53: create_box_score now emits per-player defensive (havoc) and specialist
    # (kicking/punting/return) sections in addition to the existing 8.
    proc = CFBPlayProcess(gameId=401628455)
    proc.espn_cfb_pbp()
    res = proc.run_processing_pipeline()
    abx = res["advBoxScore"]

    assert "defensive_players" in abx
    assert "specialists" in abx
    assert isinstance(abx["defensive_players"], list)
    assert isinstance(abx["specialists"], list)

    # this game (Ohio St vs Akron, 2024) has a sack and several punts
    dp = abx["defensive_players"]
    sp = abx["specialists"]
    assert len(dp) > 0 and len(sp) > 0
    assert all("player_name" in r and ("def_pos_team" in r) for r in dp)
    assert all("player_name" in r and ("pos_team" in r) for r in sp)
    assert any(r.get("sacks", 0) for r in dp), "expected at least one sack attributed"
    assert any(r.get("punts", 0) for r in sp), "expected at least one punt attributed"
