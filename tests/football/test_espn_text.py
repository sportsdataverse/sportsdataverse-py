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
