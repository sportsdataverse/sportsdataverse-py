"""The shared abbreviated-name pattern against the name shapes ESPN actually writes.

Every text below is quoted verbatim from a stored ESPN summary -- nfl-raw
``nfl/espn/raw/{season}/{event_id}.json.gz`` or cfbfastR-cfb-raw
``cfb/json/raw/{event_id}.json`` -- and the case id ends with that event id. The
templates are the anchors the NFL and CFB processors read a name through: the
verb after it ("pass", "kicks", "up the middle") or the token before it ("to",
"(", "INTERCEPTED by", "RECOVERED by CAR-", "fair catch by #N").
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.football import espn_text
from sportsdataverse.football.espn_text import ABBREVIATED_NAME


def _read(template: str, text: str) -> str | None:
    pattern = template.replace("NAME", "(" + ABBREVIATED_NAME + ")")
    return pl.DataFrame({"text": [text]}).select(pl.col("text").str.extract(pattern, 1)).item()


# (template, real play text, the name the template must read)
ACCEPTS = {
    "multi-word surname 401547409": (
        r"\(NAME",
        "S.Barkley left guard to NYG 25 for no gain (L.Vander Esch).",
        "L.Vander Esch",
    ),
    "two-letter last word 281228025": (
        r"fair catch by NAME",
        "A.Lee punts 50 yards to WAS 11, Center-B.Jennings, fair catch by A.Randle El.",
        "A.Randle El",
    ),
    "two short words 291004025": (r"\(NAME", "S.Hill sacked at SL 25 for -2 yards (C.Ah You).", "C.Ah You"),
    "sack parenthetical 321230025": (
        r"sacked[^()]*\(NAME",
        "B.Hoyer sacked at ARZ 17 for -9 yards (R.Jean Francois).",
        "R.Jean Francois",
    ),
    "lower-case particles 401873289": (
        r"\(NAME",
        " G.Brightwell left end to CIN 41 for no gain (J.van den Berg).",
        "J.van den Berg",
    ),
    "III not cut to II 401220158": (
        r"INTERCEPTED by NAME",
        "(5:16) R.Tannehill pass short middle intended for A.Brown INTERCEPTED by J.Bates III at CIN -4. Touchback.",
        "J.Bates III",
    ),
    "suffix V 400951751": (r"to NAME", "(14:24) T.Savage pass incomplete deep left to W.Fuller V.", "W.Fuller V"),
    "roman numeral leaves the sentence period 401030690": (
        r"to NAME",
        "(1:25) (Shotgun) J.Flacco pass incomplete short left to W.Snead IV.",
        "W.Snead IV",
    ),
    "two-capital initials 401128111": (
        r"NAME right end",
        "(15:00) DK.Metcalf right end to SEA 29 for 4 yards (J.Schobert; T.Carrie).",
        "DK.Metcalf",
    ),
    "two-capital initials in a tackle 401030808": (
        r"\(NAME",
        "(1:30) R.Woods right end to SEA 18 for 56 yards (SL.Griffin).",
        "SL.Griffin",
    ),
    "first name spelled out 311030014": (
        r"NAME kicks",
        "Josh.Brown kicks 50 yards from 50 to end zone, Touchback.",
        "Josh.Brown",
    ),
    "four-letter initial group 401547435": (
        r"\(NAME",
        "C.Stroud pass incomplete deep right to R.Woods (Dari.Williams).",
        "Dari.Williams",
    ),
    "initial group ending in a capital 321118008": (
        r"\(NAME",
        "J.Starks left end to DET 20 for no gain (SamL.Hill).",
        "SamL.Hill",
    ),
    "double initial without a space 291221028": (
        r"\(NAME",
        "L.Tynes kicks 63 yards from NYG 30 to WAS 7. D.Thomas to WAS 14 for 7 yards (D.J.Johnson).",
        "D.J.Johnson",
    ),
    "double initial after a jersey 400951628": (
        r"deflected by \d+-NAME",
        "(11:43) B.Wing punts 16 yards to 50, Center-Z.DeOssie, downed by NYG-Z.DeOssie. Ball deflected by 58-D.J.Alexander.",
        "D.J.Alexander",
    ),
    "double initial 400554216": (
        r"\(NAME",
        "(7:20) (No Huddle) A.Luck pass incomplete deep left to H.Nicks (A.J. Bouye).",
        "A.J. Bouye",
    ),
    "HTML-escaped apostrophe 281123015": (
        r"\(NAME",
        "R.Williams right end to NE 26 for 13 yards (D.O&apos;Neal).",
        "D.O&apos;Neal",
    ),
    "accented letter 401866409": (
        r"#\d{1,3} NAME pass",
        "No Huddle-Shotgun #12 K.Colón pass incomplete short right thrown to BUF40",
        "K.Colón",
    ),
    "comma before a numeral 401858202": (
        r"\(#\d{1,3} NAME",
        "(14:59) Shotgun #3 N.Vaughn rush left for 5 yards gain to the UVA32 (#2 R.Royal, III)",
        "R.Royal, III",
    ),
    "St. particle 401671492": (r"to NAME", "J.Goff pass incomplete short left to A.St. Brown.", "A.St. Brown"),
    "Jr. keeps its period 400874585": (
        r"\(NAME",
        "(3:31) C.West left tackle to DEN 1 for 5 yards (C.Harris Jr.; T.Davis).",
        "C.Harris Jr.",
    ),
    "hyphenated surname 401437919": (
        r"RECOVERED by [A-Z]{2,3}-NAME",
        "(14:20) J.Goff FUMBLES (Aborted) at CAR 9, RECOVERED by CAR-Y.Gross-Matos at CAR 9.",
        "Y.Gross-Matos",
    ),
}

# The name must stop where the name stops: before the verb, a team code, the next
# tackler, an upper-case verb, or a sentence glued on without a space.
STOPS = {
    "before the verb 401437765": (
        r"NAME pass",
        "(2:42) (Shotgun) T.Lawrence pass short right to M.Jones to JAX 28 for 6 yards (D.King).",
        "T.Lawrence",
    ),
    "before a team code 401437765": (
        r"to NAME",
        "(2:42) (Shotgun) T.Lawrence pass short right to M.Jones to JAX 28 for 6 yards (D.King).",
        "M.Jones",
    ),
    "before a rush direction 401772828": (
        r"NAME up the middle",
        "B.Tuten up the middle to CAR 9 for 3 yards (B.Brown).",
        "B.Tuten",
    ),
    "at the tackler separator 401872922": (
        r"\(NAME",
        "D.Watson pass short middle to B.Whiteheart to JAX 46 for 14 yards (V.Miller; D.Gardeck).",
        "V.Miller",
    ),
    "a comma before the next initial is no suffix 300110017": (
        r"\(NAME",
        "(No Huddle) J.Flacco up the middle to NE 15 for 1 yard (J.Seau, V.Wilfork).",
        "J.Seau",
    ),
    "before an upper-case verb 311023016": (
        r"Center-C\.Loeffler\. NAME",
        "C.Kluwe punts 36 yards to MIN 47, Center-C.Loeffler. R.Cobb MUFFS catch, RECOVERED by MIN-T.Gerhart at 50.",
        "R.Cobb",
    ),
    "before a glued sentence 401872661": (
        r"\(NAME",
        "D.Swift right tackle to CHI 37 for 17 yards (Ja.Horn).PENALTY on CHI-R.Odunze, Offensive Holding, 10 yards, "
        "enforced at CHI 29.",
        "Ja.Horn",
    ),
    "double initial with a space, then the verb 320930030": (
        r"to NAME for",
        "A. Dalton pass to A.J. Green for 18 yards, TOUCHDOWN",
        "A.J. Green",
    ),
    "a sentence period is no double initial 340126035": (
        r"to NAME",
        "Cam Newton pass incomplete to A.J. Green.",
        "A.J. Green",
    ),
    "a sentence period before a single name 301017021": (
        r"to NAME",
        "K.Kolb pass incomplete to B.Celek.",
        "B.Celek",
    ),
    "no double initial starts inside a glued word 401547408": (
        r"NAME extra point is",
        "C.Akers left end for 1 yard, TOUCHDOWN.B.Maher extra point is GOOD, Center-A.Ward, Holder-E.Evans.",
        "B.Maher",
    ),
    "no initial group starts inside a glued word 401220355": (
        r"NAME extra point is",
        "(5:57) (Shotgun) C.Wentz pass deep left to T.Fulgham for 42 yards, TOUCHDOWN. caught at SF8, 8 yd "
        "YACJ.Elliott extra point is GOOD, Center-R.Lovato, Holder-C.Johnston.",
        "J.Elliott",
    ),
}


@pytest.mark.parametrize(("template", "text", "expected"), list(ACCEPTS.values()), ids=list(ACCEPTS))
def test_reads_real_name_shapes(template, text, expected):
    assert _read(template, text) == expected


@pytest.mark.parametrize(("template", "text", "expected"), list(STOPS.values()), ids=list(STOPS))
def test_name_stops_at_its_boundary(template, text, expected):
    assert _read(template, text) == expected


# The 2025+ college vendor template, read through the public jersey_* helpers.
JERSEY = {
    "fg kicker, two surnames 401858224": (
        espn_text.jersey_fg_kicker,
        "(00:04) #92 J.Echeverria Lozano field goal attempt from 22 yards GOOD (H: #42 D.Drennan, LS: #46 L.Raab), "
        "clock 00:02",
        "J.Echeverria Lozano",
    ),
    "kicker, particle surname 401866410": (
        espn_text.jersey_kicker,
        "(09:44) #81 A.De La Poza kickoff 40 yards to the BGSU25 fair catch by #44 L.Kemp at BGSU25",
        "A.De La Poza",
    ),
    "punter, two words 401778319": (
        espn_text.jersey_punter,
        "(08:15) #35 S.Vander Haar punt 40 yards to the GT09",
        "S.Vander Haar",
    ),
    "punter, comma suffix 401760398": (
        espn_text.jersey_punter,
        "#18 B.Edmiston, Jr. punt 36 yards to the CSU04",
        "B.Edmiston, Jr.",
    ),
    "kicker, comma suffix 401864506": (
        espn_text.jersey_kicker,
        "#18 B.Edmiston, Jr. kickoff 65 yards to the UNC00, Touchback",
        "B.Edmiston, Jr.",
    ),
    "kicker, suffix V 401856635": (
        espn_text.jersey_kicker,
        "(15:00) #37 T.Schwartz V kickoff 65 yards to the ARK00, Touchback",
        "T.Schwartz V",
    ),
    "fair catch, curly apostrophe 401760399": (
        espn_text.jersey_returner,
        "#93 H.Green punt 50 yards to the FST13 fair catch by #10 J.Malau’ulu at FST13",
        "J.Malau’ulu",
    ),
    "fair catch, III 401752789": (
        espn_text.jersey_returner,
        "#90 G.Chadwick punt 49 yards to the OU16 fair catch by #5 I.Sategna III at OU16",
        "I.Sategna III",
    ),
    "fair catch, hyphen and two words 401858427": (
        espn_text.jersey_returner,
        "#37 B.Ramirez punt 36 yards to the Terps06 fair catch by #1 N.Abdul-Rahim Gladding at Terps06",
        "N.Abdul-Rahim Gladding",
    ),
    "returner, lower-case-L numeral 401858439": (
        espn_text.jersey_returner,
        "(07:53) #93 Q.Warren kickoff 62 yards to the HOW03 #21 J.Washington lll return 21 yards to the HOW24 "
        "(#34 J.Utzinger)",
        "J.Washington lll",
    ),
    "returner, comma suffix 401866415": (
        espn_text.jersey_returner,
        "(00:36) #95 L.Vogeler kickoff 65 yards to the CMU00 #9 J.Ruffin, Jr. return 21 yards to the CMU21 (#40 J.Davis)",
        "J.Ruffin, Jr.",
    ),
}


@pytest.mark.parametrize(("expr", "text", "expected"), list(JERSEY.values()), ids=list(JERSEY))
def test_jersey_helpers_read_real_name_shapes(expr, text, expected):
    assert pl.DataFrame({"text": [text]}).select(expr()).item() == expected


# The clause patterns: the same special-teams clauses with a name in a shape the
# abbreviated grammar does not cover -- surname-first (stats.ncaa.org) and, in
# 2005-2014 text, spelled out. One name expression, two anchors; the CFB processor
# reads them through jersey_name().
CLAUSE = {
    "returner, surname-first punt 401752723": (
        espn_text.CLAUSE_RETURNER_RE,
        "(12:31) Gerrand,Curtis punt 49 yards to the TEX22 Niblett,Ryan return 6 yards to the TEX28 "
        "(Bailey,Tyler; Adams,Railyn) PENALTY SAM Illegal Formation (Wallace,Eli) 5 yards from SAM29 to SAM24. "
        "NO PLAY.",
        "Niblett,Ryan",
    ),
    "returner, spelled out punt 252950041": (
        espn_text.CLAUSE_RETURNER_RE,
        "Joe Radigan punt 48 yards to the UConn17, Brandon McLean return 32 yards to the UConn49, clock 13:50.",
        "Brandon McLean",
    ),
    "returner, upper-case comma-space kickoff 272442305": (
        espn_text.CLAUSE_RETURNER_RE,
        "Webb, Scott kickoff 68 yards to the CM17, BROWN, Antonio return 17 yards to the CM34 "
        "(McAnderson, Bra;Schermer, Jake), PENALTY KU holding declined",
        "BROWN, Antonio",
    ),
    "returner, jersey kickoff 401858439": (
        espn_text.CLAUSE_RETURNER_RE,
        "(07:53) #93 Q.Warren kickoff 62 yards to the HOW03 #21 J.Washington lll return 21 yards to the HOW24 "
        "(#34 J.Utzinger)",
        "J.Washington lll",
    ),
    "returner, the spot token is not a name 401858439": (
        espn_text.CLAUSE_RETURNER_RE,
        "#93 Q.Warren kickoff 62 yards to the Bryant14  return 14 yards to the HOW24",
        None,
    ),
    "fg kicker, surname-first after the clock 401752795": (
        espn_text.CLAUSE_FG_KICKER_RE,
        "David Olano 28 yd FG BLOCKED blocked by DJ Taylor (10:18) Olano,David field goal attempt from 28 yards "
        "NO GOOD blocked by Taylor,DJ (H: Crimmins,Keelan, LS: Mahoney III,Patrick), clock 10:13 recovered by "
        "ILL Olano,David at WIU18, End Of Play.",
        "Olano,David",
    ),
    "fg kicker, spelled out at the text start 252440154": (
        espn_text.CLAUSE_FG_KICKER_RE,
        "Bryan Hahnfeldt field goal attempt from 34 GOOD, clock 08:41.",
        "Bryan Hahnfeldt",
    ),
}


@pytest.mark.parametrize(("pattern", "text", "expected"), list(CLAUSE.values()), ids=list(CLAUSE))
def test_clause_patterns_read_any_name_shape(pattern, text, expected):
    assert pl.DataFrame({"text": [text]}).select(espn_text.jersey_name(pattern)).item() == expected


# Jersey-style yardage: a distance of one is singular and a return behind its catch
# point reads "return for loss of N yards" -- both are signed yardages.
YARDS = {
    "punt return 1 yard 401862693": (
        espn_text.jersey_return_yards,
        "#26 M.Choules punt 44 yards to the MEM23 #17 A.Brown return 1 yard to the MEM24 (#13 J.Allen)",
        1,
    ),
    "kickoff return 1 yard, out of bounds 401856669": (
        espn_text.jersey_return_yards,
        "(01:33) #12 H.DiBoyan kickoff 42 yards to the VU23 #32 M.Carter return 1 yard to the VU24, out of bounds at VU24",
        1,
    ),
    "return for loss of 1 yard 401757282": (
        espn_text.jersey_return_yards,
        "(08:13) #32 M.Dean punt 44 yards to the LU22 #6 R.Smith return for loss of 1 yard to the LU21, End Of Play",
        -1,
    ),
    "kickoff return for loss of 5 yards 401752785": (
        espn_text.jersey_return_yards,
        "(14:59)  kickoff 27 yards to the FSU38 #83 T.Gelsey return for loss of 5 yards to the FSU33, End Of Play",
        -5,
    ),
    "the first return clause, a loss, not the fumble return after it 401752910": (
        espn_text.jersey_return_yards,
        "(11:27) #49 W.Karoll punt 50 yards to the WASH17 #81 D.Roebuck return for loss of 4 yards to the WASH13 "
        "fumbled by #81 D.Roebuck at WASH13 forced by #1 K.Clark recovered by UCLA #12 J.Benjamin at WASH13 "
        "#12 J.Benjamin return 13 yards to the WASH00 TOUCHDOWN, clock 11:19 #94 M.Bhaghani kick attempt good "
        "(H: #35 C.Peterman, LS: #50 S.Abdul-Wahab)",
        -4,
    ),
    "a loss is a return clause 401778329": (
        espn_text.has_jersey_return,
        "#39 C.Salas kickoff 6 yards to the DUKE14 #16 J.Hamilton return for loss of 27 yards to the DUKE41 "
        "(#39 C.Salas), out of bounds",
        True,
    ),
    "plural punt beside a loss return 401752910": (
        espn_text.jersey_punt_yards,
        "(11:27) #49 W.Karoll punt 50 yards to the WASH17 #81 D.Roebuck return for loss of 4 yards to the WASH13",
        50,
    ),
    "punt 1 yard 401754372": (
        espn_text.jersey_punt_yards,
        "(04:45) #95 N.Veltsistas punt 1 yard to the VT 21 blocked by #82 C.Boscia recovered by VT  #57 L.Austin at "
        "VT 21, End Of Play",
        1,
    ),
    "no jersey, no jersey return 401757280": (
        espn_text.jersey_return_yards,
        "Huiet, Joshua punt 41 yards to the LATECH18, Latulas, Dedrick return -2 yards to the LATECH16, PENALTY KENN "
        "illegal formation 5 yards to the KENN36, NO PLAY.",
        None,
    ),
}


@pytest.mark.parametrize(("expr", "text", "expected"), list(YARDS.values()), ids=list(YARDS))
def test_jersey_yardage_reads_singular_and_loss(expr, text, expected):
    assert pl.DataFrame({"text": [text]}).select(expr()).item() == expected


# --- O3: the offense's gain ends at the turnover -------------------------------------------------

#: ``(text, text before the turnover, returned for a touchdown)``. Verbatim ESPN text.
TURNOVER = {
    "cfb pick-six labelled a Passing Touchdown 252600087": (
        "Brady Quinn pass intercepted by Darean Adams at the NDame 31, returned for 31 yards for a TOUCHDOWN.",
        "Brady Quinn pass ",
        True,
    ),
    "cfb catch, fumble, 82-yard opponent return 302540025": (
        "Tyler Hansen pass complete to Ryan Deehan, fumbled, recovered by Cal Darian Hagan at the Cal 18, "
        "Darian Hagan for 82 yards, to the Colo 0 for a TOUCHDOWN.",
        "Tyler Hansen pass complete to Ryan Deehan, ",
        True,
    ),
    "cfb rush stated before the fumble 253020077": (
        "Tyrell Sutton rush for 83 yards, fumbled at the Mich 17, forced by Leon Hall, recovered by Leon Hall, "
        "returned by Leon Hall for 83 yards for a TOUCHDOWN.",
        "Tyrell Sutton rush for 83 yards, ",
        True,
    ),
    "cfb rushing TD with a trailing interception note 400985386": (
        "David Pindell run for 10 yds for a TD, (David Pindell intercepted )",
        "David Pindell run for 10 yds for a TD, (David Pindell ",
        False,
    ),
    "nfl pick-six labelled an Interception Return 400791590": (
        "(8:17) (Shotgun) T.Romo pass short left intended for D.Street INTERCEPTED by T.McBride (B.Meriweather) "
        "at DAL 20. T.McBride for 20 yards, TOUCHDOWN.",
        "(8:17) (Shotgun) T.Romo pass short left intended for D.Street ",
        True,
    ),
    "nfl run, fumble, 36-yard opponent return 401438030": (
        "(9:48) (Shotgun) J.Hurts up the middle to PHI 45 for -4 yards. FUMBLES, touched at PHI 44, "
        "RECOVERED by KC-N.Bolton at PHI 36. N.Bolton for 36 yards, TOUCHDOWN.",
        "(9:48) (Shotgun) J.Hurts up the middle to PHI 45 for -4 yards. ",
        True,
    ),
    "nfl the try, not the play, was intercepted 401671799": (
        "(Shotgun) K.Hunt right guard for 2 yards, TOUCHDOWN. TWO-POINT CONVERSION ATTEMPT. "
        "P.Mahomes pass to T.Kelce is intercepted. ATTEMPT FAILS.",
        "(Shotgun) K.Hunt right guard for 2 yards, TOUCHDOWN. TWO-POINT CONVERSION ATTEMPT. P.Mahomes pass to "
        "T.Kelce is ",
        False,
    ),
    "nfl a clean completion is untouched": (
        "J.Goff pass short left to A.St. Brown to DET 27 for 5 yards",
        "J.Goff pass short left to A.St. Brown to DET 27 for 5 yards",
        False,
    ),
}


@pytest.mark.parametrize(("text", "expected", "_td"), list(TURNOVER.values()), ids=list(TURNOVER))
def test_before_turnover_cuts_the_defence_s_return(text, expected, _td):
    assert pl.DataFrame({"text": [text]}).select(espn_text.before_turnover()).item() == expected


@pytest.mark.parametrize(("text", "_before", "expected"), list(TURNOVER.values()), ids=list(TURNOVER))
def test_returned_for_touchdown_needs_the_score_after_the_turnover(text, _before, expected):
    assert pl.DataFrame({"text": [text]}).select(espn_text.returned_for_touchdown()).item() is expected


def test_turnover_helpers_are_null_safe():
    frame = pl.DataFrame({"text": [None]}, schema={"text": pl.Utf8})
    assert frame.select(espn_text.before_turnover()).item() is None
    assert frame.select(espn_text.returned_for_touchdown()).item() is False
