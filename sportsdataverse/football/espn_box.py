"""ESPN official team box (``summary['boxscore']['teams']``) parsing shared by the
CFB and NFL play-by-play processors.

The per-team dict this returns is the authoritative source for countable team
totals (turnovers, first downs, rushing / passing yards, penalties, ...). Both
processors surface it verbatim under ``advBoxScore["espn_team"]`` so consumers
can validate the play-by-play derivation against ESPN's own numbers.
"""

from __future__ import annotations

__all__ = ["espn_num", "parse_espn_player_box", "parse_espn_team_box"]


def espn_num(value):
    """Best-effort numeric cast of an ESPN ``displayValue``.

    Returns ``int`` for whole numbers, ``float`` for decimals, and the original value
    unchanged when it is not purely numeric (e.g. ``'8-37'``, ``'31:24'``, ``'16/32'``).
    """
    if not isinstance(value, str):
        return value
    v = value.strip()
    if v.lstrip("-").isdigit():
        return int(v)
    try:
        f = float(v)
    except ValueError:
        return value
    # Reject non-finite floats ("inf"/"nan") so the box stays valid JSON.
    return f if (f == f and f not in (float("inf"), float("-inf"))) else value


def parse_espn_team_box(boxscore):
    """Parse ESPN's official team box statistics into a per-team dict keyed by team id.

    ESPN's team box (``summary['boxscore']['teams']``) is the authoritative source for
    countable team totals -- turnovers, fumbles lost, interceptions, total/passing/rushing
    yards, penalties, first downs, possession time, etc. It is surfaced verbatim (numbers
    cast where clean) so downstream totals can come straight from ESPN rather than the
    lossy play-by-play derivation. Hyphenated combos are also split into integer fields:
    ``totalPenaltiesYards`` ('8-37') -> ``penalties``/``penalty_yards``; ``completionAttempts``
    ('16-32') -> ``completions``/``pass_attempts``.
    """
    out = {}
    for t in (boxscore or {}).get("teams", []) or []:
        team = t.get("team", {}) or {}
        tid = team.get("id")
        if tid is None:
            continue
        rec = {
            "team_id": int(tid),
            "abbreviation": team.get("abbreviation"),
            "display_name": team.get("displayName"),
            "home_away": t.get("homeAway"),
        }
        for st in t.get("statistics", []) or []:
            name = st.get("name")
            dv = st.get("displayValue")
            if not name:
                continue
            # ESPN joins the combo stats with "-" for college ("16-32") and "/"
            # for the NFL ("16/22"); split on whichever is present.
            sep = "-" if isinstance(dv, str) and "-" in dv else "/" if isinstance(dv, str) and "/" in dv else None
            if name == "totalPenaltiesYards" and sep:
                p, _, y = dv.partition(sep)
                rec["penalties"] = espn_num(p)
                rec["penalty_yards"] = espn_num(y)
            elif name == "completionAttempts" and sep:
                c, _, a = dv.partition(sep)
                rec["completions"] = espn_num(c)
                rec["pass_attempts"] = espn_num(a)
            rec[name] = espn_num(dv)
        out[int(tid)] = rec
    return out


def parse_espn_player_box(boxscore):
    """Parse ESPN's official per-player box into a flat list of rows.

    Each row carries ``team_id`` / ``team_abbreviation`` / ``category`` / ``athlete_id`` /
    ``athlete`` plus the category's stat keys mapped to the athlete's values (e.g. passing
    ``completions/passingAttempts``, ``passingYards``, ``interceptions``; defensive
    ``sacks``, ``tacklesForLoss``, ``passesDefended``; ``fumbles`` / ``fumblesLost`` /
    ``fumblesRecovered``; ``puntReturns`` / ``kickReturns`` ...). ESPN's authoritative
    player stats with clean display names.
    """
    rows = []
    for pg in (boxscore or {}).get("players", []) or []:
        team = pg.get("team", {}) or {}
        tid = team.get("id")
        tab = team.get("abbreviation")
        for cat in pg.get("statistics", []) or []:
            cname = cat.get("name")
            keys = cat.get("keys") or []
            for a in cat.get("athletes", []) or []:
                ath = a.get("athlete", {}) or {}
                stats = a.get("stats") or []
                row = {
                    "team_id": int(tid) if tid is not None else None,
                    "team_abbreviation": tab,
                    "category": cname,
                    "athlete_id": ath.get("id"),
                    "athlete": ath.get("displayName"),
                }
                for k, v in zip(keys, stats):
                    row[k] = espn_num(v)
                rows.append(row)
    return rows
