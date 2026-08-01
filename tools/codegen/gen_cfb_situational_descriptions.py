"""Descriptions for the ESPN advBoxScore situational block (adv_situational).

Another raw-ESPN block, so the same discipline as gen_cfb_adv_descriptions.py:
the SHAPE of each column (count vs rate vs EPA) is verified empirically against
the published 2024 asset, and situation definitions are described without
asserting ESPN's internal down/distance thresholds.

Verified empirically before writing:
  * EPA_success* WITHOUT a _rate suffix are integer play COUNTS (e.g. 7..56),
    not EPA totals, despite the EPA_ prefix.
  * *_rate columns are true rates in [0,1].
  * EPA_{situation} and *_per_play are signed EPA floats.
Naming alone would have mislabelled every EPA_success* column.
"""

from __future__ import annotations

import pathlib
import re
import sys

sys.path.insert(0, ".")

# situation -> phrase. Thresholds are ESPN's; the text does not invent them.
SIT = {
    "early_down": "early downs",
    "late_down": "late downs",
    "standard_down": "standard downs (the team ahead of schedule for the series)",
    "passing_down": "passing downs (the team behind schedule for the series)",
    "middle_8": "the middle eight -- the closing minutes of the first half and opening minutes of the second",
    "rz": "the red zone",
    "third": "third down",
}
PLAY = {"pass": " on pass plays", "rush": " on rush plays"}


def _sit_phrase(key: str) -> str | None:
    return SIT.get(key)


def describe(col: str) -> str | None:
    # --- play-mix counts / rates: {situation}_{pass|rush}[_rate], {situation}s
    m = re.fullmatch(r"(early_down|late_down|middle_8)_(pass|rush)(_rate)?", col)
    if m:
        sit, play, rate = m.groups()
        if rate:
            return f"Share of the team's plays on {SIT[sit]} that were {play} plays."
        return f"Number of {play} plays the team ran on {SIT[sit]}."
    if col in ("early_downs", "late_downs", "standard_downs", "passing_downs"):
        return f"Number of plays the team ran on {SIT[col[:-1]]}."
    if col == "middle_8":
        # verified Int64 1..27 on the published 2024 asset -- a play count, not an EPA figure
        return f"Number of plays the team ran in {SIT['middle_8']}."
    if col == "pos_team":
        return "Display name of the team on offense (e.g. 'Ohio State Buckeyes')."
    if col == "early_down_first_down":
        return "Number of early-down plays that produced a first down."
    if col == "early_down_first_down_rate":
        return "Share of early-down plays that produced a first down."

    # --- success COUNTS and RATES: EPA_success[_{sit}][_{pass|rush}][_rate]
    m = re.fullmatch(
        r"EPA_success(?:_(early_down|late_down|standard_down|passing_down|middle_8|rz|third))?"
        r"(?:_(pass|rush))?(_rate)?",
        col,
    )
    if m:
        sit, play, rate = m.groups()
        where = f" on {SIT[sit]}" if sit else ""
        pl_ = PLAY.get(play or "", "")
        if rate:
            return f"Success rate{where}{pl_} -- the share of those plays ESPN scored as successful."
        return f"Count of successful plays{where}{pl_}. Despite the EPA_ prefix this is a play COUNT, not an EPA total."
    # ESPN also emits the SITUATION-FIRST order for middle-eight success:
    # EPA_middle_8_success[_{pass|rush}][_rate], not EPA_success_middle_8_*.
    m = re.fullmatch(r"EPA_(middle_8)_success(?:_(pass|rush))?(_rate)?", col)
    if m:
        sit, play, rate = m.groups()
        pl_ = PLAY.get(play or "", "")
        if rate:
            return f"Success rate on {SIT[sit]}{pl_} -- the share of those plays ESPN scored as successful."
        return (
            f"Count of successful plays on {SIT[sit]}{pl_}. Despite the EPA_ prefix this is a play "
            "COUNT, not an EPA total."
        )

    # EPA_success_rate_rz / EPA_success_rate_third (suffix order flipped by ESPN)
    m = re.fullmatch(r"EPA_success_rate_(rz|third)", col)
    if m:
        return f"Success rate on {SIT[m.group(1)]} -- the share of those plays ESPN scored as successful."

    # --- EPA totals / per-play: EPA_{sit}[_{pass|rush}][_per_play]
    m = re.fullmatch(
        r"EPA_(early_down|late_down|standard_down|passing_down|middle_8)"
        r"(?:_(pass|rush))?(_per_play)?",
        col,
    )
    if m:
        sit, play, per = m.groups()
        pl_ = PLAY.get(play or "", "")
        if per:
            return f"EPA per play on {SIT[sit]}{pl_}."
        return f"Total EPA the team generated on {SIT[sit]}{pl_}."

    if col == "pos_team_id":
        return "ESPN team id of the team on offense. Present for every season 2004+."
    return None


def main() -> None:
    import yaml

    target = "load_cfb_adv_situational"
    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    got, unparsed = {}, []
    for c in schemas[target]:
        col = c["name"]
        d = describe(col)
        if d is None:
            unparsed.append(col)
        else:
            got[col] = d
    print(f"composed {len(got)}; unparsed (left blank, never invented) {len(unparsed)}")
    for u in unparsed:
        print(f"   {u}")
    with open("_sit_descs.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump({target: dict(sorted(got.items()))}, fh, sort_keys=False, allow_unicode=True, width=120)
    print("wrote _sit_descs.yaml")


if __name__ == "__main__":
    main()
