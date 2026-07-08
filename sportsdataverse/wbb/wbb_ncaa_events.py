"""Women's college basketball NCAA PBP-line event extractors.

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_events` -- the faithful port
of hoop-explorer's ``cbb-explorer``
(`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_)
``EventUtils.scala``. The extractors operate on raw NCAA play-by-play line
strings and are entirely league-agnostic -- the on-the-wire format is
identical for the men's and women's college basketball index. This module
re-exports the mbb core functions **by reference** (not a copy) so
``sportsdataverse.wbb`` callers get the identical implementation the mbb side
uses, with no duplicated logic to drift out of sync.

``EventUtils.scala`` is upstream-licensed under Apache License, Version 2.0;
see the full attribution (copyright notice, upstream URL, what was derived)
in the ``sportsdataverse.mbb.mbb_ncaa_events`` module docstring and in
``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_events import parse_two_pointer_made

        print(parse_two_pointer_made("08:44,20-23,WATKINS,MIKE made Layup"))

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_events import (
    is_gen2,
    parse_any_play,
    parse_assist,
    parse_deadball_rebound,
    parse_defensive_action_event,
    parse_defensive_event,
    parse_defensive_info_event,
    parse_defensive_rebound,
    parse_flagrant_foul,
    parse_foul_info,
    parse_free_throw_attempt,
    parse_free_throw_event,
    parse_free_throw_event_attempt_gen2,
    parse_free_throw_made,
    parse_free_throw_missed,
    parse_game_time,
    parse_jumpball_won,
    parse_jumpball_won_or_lost,
    parse_live_offensive_rebound,
    parse_offensive_deadball_rebound,
    parse_offensive_event,
    parse_offensive_foul,
    parse_offensive_rebound,
    parse_personal_foul,
    parse_rebound,
    parse_rim_made,
    parse_rim_missed,
    parse_shot_blocked,
    parse_shot_made,
    parse_shot_missed,
    parse_stolen,
    parse_team_sub_in,
    parse_team_sub_out,
    parse_technical_foul,
    parse_three_pointer_made,
    parse_three_pointer_missed,
    parse_timeout,
    parse_turnover,
    parse_two_pointer_made,
    parse_two_pointer_missed,
)

__all__ = [
    "is_gen2",
    "parse_game_time",
    "parse_team_sub_in",
    "parse_team_sub_out",
    "parse_any_play",
    "parse_jumpball_won_or_lost",
    "parse_jumpball_won",
    "parse_timeout",
    "parse_rim_made",
    "parse_rim_missed",
    "parse_two_pointer_made",
    "parse_two_pointer_missed",
    "parse_three_pointer_made",
    "parse_three_pointer_missed",
    "parse_shot_made",
    "parse_shot_missed",
    "parse_shot_blocked",
    "parse_rebound",
    "parse_offensive_rebound",
    "parse_defensive_rebound",
    "parse_deadball_rebound",
    "parse_offensive_deadball_rebound",
    "parse_live_offensive_rebound",
    "parse_free_throw_made",
    "parse_free_throw_missed",
    "parse_free_throw_attempt",
    "parse_free_throw_event",
    "parse_free_throw_event_attempt_gen2",
    "parse_turnover",
    "parse_stolen",
    "parse_assist",
    "parse_personal_foul",
    "parse_technical_foul",
    "parse_flagrant_foul",
    "parse_offensive_foul",
    "parse_foul_info",
    "parse_offensive_event",
    "parse_defensive_action_event",
    "parse_defensive_info_event",
    "parse_defensive_event",
]
