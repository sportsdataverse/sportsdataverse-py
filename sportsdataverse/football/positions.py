"""ESPN football position ids -> abbreviation and position group.

Vendored from ``sports.core.api.espn.com/v2/sports/football/leagues/{nfl,
college-football}/positions`` (74 entries, identical across the two leagues,
captured 2026-09-15). The 67 player positions are mapped; the seven non-player
ids ESPN also lists -- the placeholders ``0`` and ``99`` (Unknown), ``50``
(Athlete), the side groups ``70`` (Offense), ``71`` (Defense) and ``72``
(Special Teams), and ``218`` (Setter, a volleyball leak) -- deliberately
resolve to ``None``. Play participants carry a position ``$ref`` whose
trailing id is what :func:`position_group` maps; the groups are the ones the
usage box splits tackles and first downs by: ``QB``, ``RB``, ``WR``, ``TE``,
``OL``, ``DL``, ``LB``, ``DB``, ``ST``.

Example:
    Quick start::

        from sportsdataverse.football.positions import position_abbr, position_group
        position_abbr(264), position_group(264)   # ('EDGE', 'DL')
"""

from __future__ import annotations

from typing import Optional

__all__ = ["POSITIONS", "position_abbr", "position_group"]

#: ESPN position id -> (abbreviation, position group)
POSITIONS: dict[int, tuple[str, str]] = {
    1: ("WR", "WR"),
    2: ("LT", "OL"),
    3: ("LG", "OL"),
    4: ("C", "OL"),
    5: ("RG", "OL"),
    6: ("RT", "OL"),
    7: ("TE", "TE"),
    8: ("QB", "QB"),
    9: ("RB", "RB"),
    10: ("FB", "RB"),
    11: ("LDE", "DL"),
    12: ("NT", "DL"),
    13: ("RDE", "DL"),
    14: ("LOLB", "LB"),
    15: ("LILB", "LB"),
    16: ("RILB", "LB"),
    17: ("ROLB", "LB"),
    18: ("LCB", "DB"),
    19: ("RCB", "DB"),
    20: ("SS", "DB"),
    21: ("FS", "DB"),
    22: ("PK", "ST"),
    23: ("P", "ST"),
    24: ("LDT", "DL"),
    25: ("RDT", "DL"),
    26: ("WLB", "LB"),
    27: ("MLB", "LB"),
    28: ("SLB", "LB"),
    29: ("CB", "DB"),
    30: ("LB", "LB"),
    31: ("DE", "DL"),
    32: ("DT", "DL"),
    33: ("UT", "OL"),
    34: ("NB", "DB"),
    35: ("DB", "DB"),
    36: ("S", "DB"),
    37: ("DL", "DL"),
    39: ("LS", "ST"),
    45: ("OL", "OL"),
    46: ("OT", "OL"),
    47: ("OG", "OL"),
    73: ("G", "OL"),
    74: ("T", "OL"),
    75: ("NG", "DL"),
    76: ("PR", "ST"),
    77: ("KR", "ST"),
    78: ("LS", "ST"),
    79: ("H", "ST"),
    80: ("PK", "ST"),
    90: ("ILB", "LB"),
    91: ("C", "OL"),
    94: ("P", "ST"),
    96: ("LS", "ST"),
    100: ("FL", "WR"),
    101: ("HB", "RB"),
    102: ("TB", "RB"),
    103: ("LHB", "RB"),
    104: ("RHB", "RB"),
    105: ("LLB", "LB"),
    106: ("RLB", "LB"),
    107: ("OLB", "LB"),
    108: ("LSF", "DB"),
    109: ("RSF", "DB"),
    110: ("MG", "OL"),
    111: ("SE", "WR"),
    219: ("B", "RB"),
    264: ("EDGE", "DL"),
}


def _coerce(position_id: object) -> Optional[int]:
    if isinstance(position_id, bool) or position_id is None:
        return None
    if isinstance(position_id, (int, float, str)):
        try:
            return int(position_id)
        except (TypeError, ValueError):
            return None
    return None


def position_abbr(position_id: object) -> Optional[str]:
    """The ESPN abbreviation for a position id (``None`` when unknown).

    Args:
        position_id: ESPN position id (int or numeric string).

    Returns:
        Abbreviation such as ``"EDGE"``, or ``None``.

    Example:
        Quick start::

            from sportsdataverse.football.positions import position_abbr
            position_abbr("8")   # 'QB'
    """
    pid = _coerce(position_id)
    return POSITIONS[pid][0] if pid in POSITIONS else None


def position_group(position_id: object) -> Optional[str]:
    """The position group (``QB RB WR TE OL DL LB DB ST``) for a position id.

    Args:
        position_id: ESPN position id (int or numeric string).

    Returns:
        The group, or ``None`` for an unknown / non-player id.

    Example:
        Quick start::

            from sportsdataverse.football.positions import position_group
            position_group(29)   # 'DB'
    """
    pid = _coerce(position_id)
    return POSITIONS[pid][1] if pid in POSITIONS else None
