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


@skip_if_no_live
def test_passer_box_survives_the_participants_join():
    """Passers must appear when join_participants=True (the production config).

    Regression: athlete_name was copied during QBR feature setup, i.e. BEFORE
    the participants join rewrites passer_player_name / rusher_player_name with
    cleaned names. It therefore kept the raw participant text
    ("No Huddle-Shotgun #2 E.Buehler") while qbs_list held "Eddie Buehler", the
    is_in() matched nothing, the QBR frame came back empty, and the INNER join
    onto it deleted every passer. advBoxScore["pass"] was [] on every game while
    rush and receiver looked fine -- there cannot be receptions without passes,
    which is what made it obvious the aggregation, not the data, was at fault.
    """
    proc = CFBPlayProcess(gameId=401628455)
    proc.join_participants = True
    proc.espn_cfb_pbp()
    abx = proc.run_processing_pipeline()["advBoxScore"]

    passers = abx["pass"]
    receivers = abx["receiver"]
    assert receivers, "fixture game should have receiving rows"
    assert passers, "passers vanished: receptions exist, so passes must too"

    # names come from the cleaned column, never the raw participant text
    for row in passers:
        name = row["passer_player_name"]
        assert name and "#" not in name, f"raw participant text leaked into box score: {name!r}"
        assert row["Att"] >= 1

    # QBR is an enrichment; its absence must not delete rows (left join)
    assert set(passers[0]) >= {"Comp", "Att", "Yds", "Pass_TD", "EPA"}
