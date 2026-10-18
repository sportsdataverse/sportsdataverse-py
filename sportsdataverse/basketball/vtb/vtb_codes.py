"""Code tables of the Infobasket play-by-play and line-up rows (``sdv-internal-refs/vtb/vtb-returns.md``).

Labels are the site widget's English ``getPlayDescription`` strings; the gloss is the refs documentation's English
reading. Codes the table lacks (new play types) parse to a null label rather than failing.
"""

from __future__ import annotations

from typing import Dict, Tuple

#: ``OnlinePlays.PlayTypeID`` -> (widget label, gloss).
PLAY_TYPES: Dict[int, Tuple[str, str]] = {
    0: ("-", "Placeholder (the row-0 header play)"),
    1: ("1 point", "Free throw made"),
    2: ("2 points", "2-point field goal made"),
    3: ("3 points", "3-point field goal made"),
    4: ("Miss 1 point", "Free throw missed"),
    5: ("Miss 2 points", "2-point field goal missed"),
    6: ("Miss 3 points", "3-point field goal missed"),
    7: ("Substitution", "Substitution (parent of an In and an Out)"),
    8: ("In", "Player in (child of a substitution or period start)"),
    9: ("Out", "Player out (child of a substitution or period end)"),
    10: ("Turnover", "Turnover"),
    11: ("Pass-Turnover", "Bad-pass turnover"),
    12: ("Traveling", "Travelling"),
    13: ("Step-out", "Stepped out of bounds"),
    14: ("Backcourt", "Backcourt violation"),
    15: ("Double drible", "Double dribble"),
    16: ("3 seconds", "3-second violation"),
    17: ("5 seconds", "5-second violation"),
    18: ("8 seconds", "8-second violation"),
    19: ("24 seconds", "Shot-clock violation"),
    20: ("Ball-Turnover", "Ball-handling turnover (lost ball)"),
    21: ("Start", "Period start"),
    22: ("Finish", "Period end"),
    23: ("Timeout", "Timeout"),
    24: ("Possession", "Possession change (child of a scoring play)"),
    25: ("Assist", "Assist (child of a made shot)"),
    26: ("Steal", "Steal"),
    27: ("Blockshot", "Block"),
    28: ("Rebound", "Rebound (child of a miss)"),
    29: ("Fast-break", "Fast break (shot qualifier)"),
    30: ("Dunk", "Dunk (shot qualifier)"),
    31: ("Jump shot", "Jump shot (shot qualifier)"),
    32: ("Layup", "Layup (shot qualifier)"),
    33: ("Tip-in", "Tip-in (shot qualifier)"),
    34: ("Alley-oop", "Alley-oop (shot qualifier)"),
    35: ("Hook", "Hook shot (shot qualifier)"),
    36: ("Free throw", "Free throw (qualifier)"),
    40: ("Foul P", "Personal foul"),
    41: ("Foul U", "Unsportsmanlike foul"),
    42: ("Foul T", "Technical foul"),
    43: ("Foul D", "Disqualifying foul"),
    44: ("Foul C", "Technical foul on the coach (own conduct)"),
    45: ("Foul B", "Technical foul on the coach (bench)"),
    46: ("Foul F", "Fighting foul"),
    47: ("in offence", "Offensive (qualifier of a foul)"),
    50: ("Throw-in", "Throw-in awarded (child of a foul)"),
    51: ("1 FT", "1 free throw awarded (child of a foul)"),
    52: ("2 FT", "2 free throws awarded (child of a foul)"),
    53: ("3 FT", "3 free throws awarded (child of a foul)"),
    54: ("Compencation", "Compensation (cancelled penalties)"),
    59: ("(none)", "No label in the widget; seen once in game 921391, before tip-off (meaning unverified)."),
    60: ("Warmup", "Team warm-up"),
    61: ("Presentation", "Team presentation"),
    62: ("3 Minutes To Start", "Less than 3 minutes to tip-off"),
    63: ("Ready To Start", "Game about to start"),
    64: ("Delay", "Delay for technical reasons"),
    65: ("Game Interrupted", "Game interrupted"),
    66: ("Game Resumed", "Game resumed"),
    69: ("Correction", "Correction"),
    70: ("Timer Stopped", "Clock stopped"),
    71: ("Timer Started", "Clock started"),
    72: ("Review Video", "Video review"),
    73: ("Video Started", "Video stream started"),
    74: ("Video Stoped", "Video stream ended"),
    100: ("Final", "Game over (Final)"),
    141: ("Foul FL", "Flagrant foul (FL)"),
    142: ("Foul T (1 cat.)", "Technical foul, category 1"),
    143: ("Foul GD", "Game disqualifying foul (GD, unverified)"),
    145: ("Foul BD", "Bench disqualification (BD, unverified)"),
    241: ("Foul DI", "Disqualifying foul (DI, 'destructive' in Russian)"),
    242: ("Foul T (2 cat.)", "Technical foul, category 2"),
}

#: ``OnlineStarts.StartType``: 0 team row, 1 player, 3 coach, 4 official, 5 placeholder (StartID 0, PersonID 0).
START_TYPES: Dict[int, str] = {0: "team", 1: "player", 3: "coach", 4: "official", 5: "placeholder"}


def staff_role(start_type: object, pos_id: object) -> str:
    """The English role of a coach or official line-up row from its ``StartType`` and ``PosID``.

    Args:
        start_type: ``OnlineStarts.StartType`` (3 coach, 4 official).
        pos_id: ``OnlineStarts.PosID`` (coach 1000 head / 1001 assistant; official 101-103 referees, 104 commissioner,
            111-123 table officials).

    Returns:
        ``"head coach"``, ``"assistant coach"``, ``"referee"``, ``"commissioner"``, ``"table official"`` or the generic
        ``"coach"`` / ``"official"`` for a code the table lacks.
    """
    pos = pos_id if isinstance(pos_id, int) and not isinstance(pos_id, bool) else -1
    if start_type == 3:
        return {1000: "head coach", 1001: "assistant coach"}.get(pos, "coach")
    if 101 <= pos <= 103:
        return "referee"
    if pos == 104:
        return "commissioner"
    if 111 <= pos <= 123:
        return "table official"
    return "official"
