"""stats.ncaa.org college-softball (NCAA sport code ``WSB``) play-by-play parser.

Thin re-export of the college-baseball parser **by reference** -- stats.ncaa.org
softball play-by-play uses the identical per-inning ``<table class="table">``
layout and the same play-text grammar as baseball, so one parser serves both
(verified against a real WSB capture). This is ORIGINAL sdv-py code; see
:mod:`sportsdataverse.baseball.college_baseball.college_baseball_ncaa_pbp` for the
full provenance note, schema, and grammar details.

Example:
    Quick start::

        from sportsdataverse.baseball.college_softball import parse_college_softball_ncaa_pbp
        df = parse_college_softball_ncaa_pbp(open("contest.html").read(), contest_id=...)
        print(df.shape)
"""

from __future__ import annotations

from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_pbp import (
    PBP_SCHEMA,
)
from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_pbp import (
    parse_college_baseball_ncaa_pbp as parse_college_softball_ncaa_pbp,
)

__all__ = ["PBP_SCHEMA", "parse_college_softball_ncaa_pbp"]
