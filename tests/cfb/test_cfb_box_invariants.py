import copy
import sys

from sportsdataverse.cfb import cfb_box_invariants as inv
from sportsdataverse.cfb import cfb_drive_summary as drive_summary
from sportsdataverse.cfb import cfb_situational_stats as situational_stats

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from test_cfb_drive_summary import _clock_drives, _clock_frame, _drives  # noqa: E402
from test_cfb_drive_summary import _frame as _drive_frame  # noqa: E402
from test_cfb_situational_stats import _frame as _sit_frame  # noqa: E402
from test_cfb_situational_stats import _st_frame  # noqa: E402

HOME, AWAY = "10", "20"


def test_fixtures_satisfy_every_invariant():
    """The shipped builders must agree with themselves on every fixture we have."""
    cases = [
        (drive_summary.create_drive_summary(_drives(), _drive_frame(), HOME, AWAY), None),
        (drive_summary.create_drive_summary(_clock_drives(), _clock_frame(), HOME, AWAY), None),
        (None, situational_stats.create_situational_stats(_sit_frame(), HOME, AWAY)),
        (None, situational_stats.create_situational_stats(_st_frame(), HOME, AWAY)),
    ]
    for ds, sit in cases:
        assert inv.check_box_invariants(ds, sit) == []
    # and per window, where the slices get thin
    for periods in ({1}, {2}, {3, 4}, "ot"):
        ds = drive_summary.create_drive_summary(_clock_drives(), _clock_frame(), HOME, AWAY, periods=periods)
        if ds is not None:
            assert inv.check_box_invariants(ds, None) == []


def test_checker_names_what_it_catches():
    """A broken aggregate produces a specific line, never a silent pass."""
    sit = situational_stats.create_situational_stats(_sit_frame(), HOME, AWAY)
    bad = copy.deepcopy(sit)
    t = bad["teams"][AWAY]
    t["turnovers"]["fumbles_lost"] = t["turnovers"]["fumbles"] + 1  # lost more than happened
    t["red_zone"]["td_trips"] = t["red_zone"]["trips"] + 5  # scored on more trips than taken
    t["penalties_situational"]["by_unit"]["offense"]["n"] += 1  # units no longer sum to accepted
    t["downs"]["down_3"]["explosive_rate"] = 1.7  # a rate outside [0, 1]
    found = inv.check_box_invariants(None, bad)
    for needle in (
        "fumbles_lost > fumbles",
        "td_trips + fg_trips > trips",
        "offense+defense+special_teams",
        "explosive_rate 1.7",
    ):
        assert any(needle in line for line in found), (needle, found)

    ds = drive_summary.create_drive_summary(_drives(), _drive_frame(), HOME, AWAY)
    bad = copy.deepcopy(ds)
    bad["teams"][HOME]["td_drives"] = bad["teams"][HOME]["scoring_drives"] + 1
    bad["teams"][HOME]["drives_under_1min"] = bad["teams"][HOME]["drives_under_2min"] + 1
    found = inv.check_box_invariants(bad, None)
    assert any("td_drives > scoring_drives" in x for x in found)
    assert any("drives_under_1min > drives_under_2min" in x for x in found)


def test_checker_is_quiet_on_nothing():
    assert inv.check_box_invariants(None, None) == []
    assert inv.check_box_invariants({}, {}) == []
