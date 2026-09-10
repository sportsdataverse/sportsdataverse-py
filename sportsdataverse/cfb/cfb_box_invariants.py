"""Sanity checks over the drive-summary and situational-stats aggregates.

The two builders slice one plays frame many ways, and every slice must agree
with every other about the counts it shares -- a fumble can be lost only if it
happened, a red-zone trip is also a scoring-opportunity trip, the unit split
of a penalty count must sum back to the count. These are the identities a
reader would notice if they broke, so they are checked as data, not assumed.

``check_box_invariants`` returns the violations as strings; an empty list is
a clean bill. It never raises, so a validation harness can report every
failure in one pass rather than the first.
"""

from __future__ import annotations

from collections.abc import Iterable

__all__ = ["check_box_invariants"]

_RATES = ("success_rate", "explosive_rate")
_ST_PHASES = ("kickoff", "kickoff_return", "punt", "punt_return", "fg_xp")


def _grp_ok(name: str, g: dict, out: list[str]) -> None:
    """A ``_grp``-shaped node: counts non-negative, rates inside [0, 1], null only when empty."""
    if not isinstance(g, dict) or "plays" not in g:
        return
    plays = g["plays"]
    if plays < 0:
        out.append(f"{name}: plays {plays} < 0")
    for r in _RATES:
        v = g.get(r)
        if plays == 0 and v is not None:
            out.append(f"{name}: {r} is {v} on an empty slice")
        if v is not None and not (0.0 <= v <= 1.0):
            out.append(f"{name}: {r} {v} outside [0, 1]")
    if plays and g.get("epa_play") is not None and g.get("epa_total") is not None:
        # total = mean * n, to rounding
        if abs(g["epa_total"] - g["epa_play"] * plays) > 0.05 * plays + 0.05:
            out.append(f"{name}: epa_total {g['epa_total']} != epa_play {g['epa_play']} x {plays}")


def _walk_grps(prefix: str, node, out: list[str]) -> None:
    if isinstance(node, dict):
        _grp_ok(prefix, node, out)
        for k, v in node.items():
            _walk_grps(f"{prefix}.{k}", v, out)


def _unit_sums(name: str, node: dict, key: str, total: int | None, out: list[str]) -> None:
    """offense + defense + special_teams == total, and the phases sum to special_teams."""
    st = node.get("special_teams") or {}
    parts = [node.get("offense", {}).get(key, 0), node.get("defense", {}).get(key, 0), st.get(key, 0)]
    if total is not None and sum(parts) != total:
        out.append(f"{name}.by_unit: offense+defense+special_teams {key} = {sum(parts)} != {total}")
    phases = sum((st.get(ph) or {}).get(key, 0) for ph in _ST_PHASES)
    if phases != st.get(key, 0):
        out.append(f"{name}.by_unit.special_teams: phases sum {phases} != {st.get(key, 0)}")


def check_box_invariants(drive_summary: dict | None = None, situational: dict | None = None) -> list[str]:
    """Every identity the two aggregates must satisfy; violations as strings.

    Args:
        drive_summary: the dict from :func:`create_drive_summary`, or None.
        situational: the dict from :func:`create_situational_stats`, or None.

    Returns:
        list[str]: one line per violation, ``[]`` when everything holds.

    Example:
        Assert a processed game is internally consistent::

            assert check_box_invariants(summary, stats) == []
    """
    out: list[str] = []

    for tid, t in ((drive_summary or {}).get("teams") or {}).items():
        n = t.get("total_drives", 0)
        for k in (
            "scoring_drives",
            "td_drives",
            "long_drives_70yds",
            "long_drives_10plays",
            "drives_over_5min",
            "drives_under_1min",
            "drives_under_2min",
            "three_and_outs",
        ):
            if t.get(k, 0) > n:
                out.append(f"drives[{tid}]: {k} {t[k]} > total_drives {n}")
        if t.get("td_drives", 0) > t.get("scoring_drives", 0):
            out.append(f"drives[{tid}]: td_drives > scoring_drives")
        if t.get("drives_under_1min", 0) > t.get("drives_under_2min", 0):
            out.append(f"drives[{tid}]: drives_under_1min > drives_under_2min")
        if t.get("scoring_drives_under_2min", 0) > min(t.get("drives_under_2min", 0), t.get("scoring_drives", 0)):
            out.append(f"drives[{tid}]: scoring_drives_under_2min exceeds its parents")
        for k in ("third_downs", "fourth_downs"):
            c = t.get(k) or {}
            if c.get("made", 0) > c.get("att", 0):
                out.append(f"drives[{tid}]: {k} made > att")

    for tid, t in ((situational or {}).get("teams") or {}).items():
        _walk_grps(f"situational[{tid}]", t, out)

        tos = t.get("turnovers") or {}
        if tos.get("fumbles_lost", 0) > tos.get("fumbles", 0):
            out.append(f"situational[{tid}].turnovers: fumbles_lost > fumbles")
        if "by_unit" in tos:
            _unit_sums(f"situational[{tid}].turnovers", tos["by_unit"], "n", None, out)
            for unit in ("offense", "defense"):
                leaf = tos["by_unit"].get(unit) or {}
                if leaf.get("interceptions", 0) + leaf.get("fumbles_lost", 0) > leaf.get("n", 0):
                    out.append(f"situational[{tid}].turnovers.by_unit.{unit}: int + fumbles_lost > n")

        pens = t.get("penalties_situational") or {}
        if "by_unit" in pens:
            _unit_sums(f"situational[{tid}].penalties", pens["by_unit"], "n", pens.get("accepted"), out)
            for unit, leaf in _leaves(pens["by_unit"]):
                if leaf.get("auto_first", 0) > leaf.get("n", 0):
                    out.append(f"situational[{tid}].penalties.by_unit.{unit}: auto_first > n")

        rz = t.get("red_zone") or {}
        if rz.get("td_trips", 0) + rz.get("fg_trips", 0) > rz.get("trips", 0):
            out.append(f"situational[{tid}].red_zone: td_trips + fg_trips > trips")
        fin = t.get("finishing_drives") or {}
        if rz.get("trips", 0) > fin.get("trips", 0):
            out.append(
                f"situational[{tid}]: red_zone.trips {rz.get('trips')} > finishing_drives.trips {fin.get('trips')}"
            )

        for dn, d in (t.get("downs") or {}).items():
            conv = d.get("conversions")
            if conv and conv.get("made", 0) > conv.get("att", 0):
                out.append(f"situational[{tid}].downs.{dn}: conversions made > att")
            bd = d.get("by_distance")
            if conv and bd and sum(b.get("att", 0) for b in bd.values()) != conv.get("att", 0):
                out.append(f"situational[{tid}].downs.{dn}: by_distance att does not sum to conversions att")
            if d.get("pass_rate") is not None and not (0.0 <= d["pass_rate"] <= 1.0):
                out.append(f"situational[{tid}].downs.{dn}: pass_rate outside [0, 1]")

        rq = t.get("rushing_quality") or {}
        if rq.get("plays") is not None and rq.get("attempts") is not None and rq["plays"] != rq["attempts"]:
            out.append(f"situational[{tid}].rushing_quality: plays {rq['plays']} != attempts {rq['attempts']}")

    return out


def _leaves(by_unit: dict) -> Iterable[tuple[str, dict]]:
    yield "offense", by_unit.get("offense") or {}
    yield "defense", by_unit.get("defense") or {}
    st = by_unit.get("special_teams") or {}
    yield "special_teams", st
    for ph in _ST_PHASES:
        yield f"special_teams.{ph}", st.get(ph) or {}
