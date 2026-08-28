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


def test_formation_prefix_never_becomes_a_player_name():
    """Play-text formation tags must not survive into an extracted name.

    ESPN prefixes play text with the formation ("No Huddle-Shotgun #1 C.Parker
    pass complete..."). The name captures are windowed -- (.{0,30} )pass -- so a
    prefix short enough to fit is swallowed whole, and a longer one is captured
    TRUNCATED, starting mid-token ("dle-Shotgun #5 R.Marshall"). Either way it
    reaches the box score as a phantom player: game 401896383 listed
    "No Huddle-Shotgun #1 C.Parker" as a third quarterback alongside that same
    player's real line.

    Only the regex FALLBACK is affected; where ESPN supplies a participant the
    join overwrites the name. It bites where ESPN does not -- that game's
    participants feed had a null passer on 96 of its 210 plays.
    """
    import polars as pl

    from sportsdataverse.cfb.cfb_pbp import _strip_presentational_tokens

    cases = {
        # intact prefix, fits inside the capture window
        "No Huddle-Shotgun #1 C.Parker": "C.Parker",
        "No Huddle #3 D.Batch": "D.Batch",
        "Shotgun #7 T.Kenan": "T.Kenan",
        # truncated by the window, starting mid-token
        "dle-Shotgun #5 R.Marshall": "R.Marshall",
        "le-Shotgun #20 N.Laughlin": "N.Laughlin",
        "ddle-Shotgun #3 K.Battles": "K.Battles",
        # jersey without a formation, and names that must pass through untouched
        "#9 J.Triplett": "J.Triplett",
        "Carson Parker": "Carson Parker",
        "Beau Brungard": "Beau Brungard",
    }
    got = (
        pl.DataFrame({"raw": list(cases)})
        .select(_strip_presentational_tokens(pl.col("raw")).alias("name"))["name"]
        .to_list()
    )
    assert got == list(cases.values())
    for name in got:
        assert "#" not in name
        assert "huddle" not in name.lower() and "shotgun" not in name.lower()


def test_every_player_name_column_is_cleaned():
    """The cleanup runs where names are finalized, not only in one extractor.

    _extract_player_name is not the only path: receiver_player comes from a
    direct `to (.+)` extract and the Passing Touchdown passer from
    `pass from(.+)`, so cleaning inside that helper alone would leave "#9"
    prefixes on those. Applying it at the assignment block covers every column.
    """
    import inspect

    from sportsdataverse.cfb import cfb_pbp

    src = inspect.getsource(cfb_pbp)
    block = src[src.index("## Extract player names") : src.index("## Extract player names") + 4000]
    assigned = [ln for ln in block.splitlines() if "_player_name=" in ln]
    assert len(assigned) >= 15, f"expected the full name block, saw {len(assigned)} columns"
    for line in assigned:
        assert "_strip_presentational_tokens(" in line, f"uncleaned name column: {line.strip()}"
