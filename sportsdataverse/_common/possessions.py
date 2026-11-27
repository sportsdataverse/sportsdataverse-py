"""The free-throw weight of the box possession estimate ``FGA - ORB + TOV + w * FTA``, per league.

A free-throw attempt ends a possession less often than a shot: and-ones, the first of two, and
technicals do not. The pros use ``w = 0.44``. College uses ``w = 0.475``: the one-and-one and the
college foul structure send more trips to the line that are one or three attempts, so each attempt
stands for more of a possession. Measured on the men's 2025-26 season (12,040 team-games), the
counted play-by-play possessions run +1.05 per team-game above the 0.44 estimate and +0.35 above
0.475; the weight that zeroes the gap is 0.491, so 0.475 is the conservative college value (and the
one the stats.ncaa.org lineup ports already use).

The box-score possession counts, pace, offensive / defensive ratings, turnover rates (turnovers per
possession-ending play) and usage of the game page, the official season profiles and the four-factor
point accounting take their weight from :func:`ft_possession_weight`. True shooting keeps 0.44 in every
league (:data:`TS_FT_WEIGHT`): it is a shooting convention, not a possession count. The college rank
sample gates (``rate_gates.TEAM_MIN_N``) are fit on the 0.475 estimate.

Three college inputs to fitted models keep the pros' weight. Each was refit at 0.475 on 2026-11-26
against a pre-registered no-regression gate (new no worse than the shipped model by more than 2
clustered SEs):

* the opponent-adjusted ratings engine (``mbb_team_ratings``) and the pregame constants fit on it passed:
  the 2025-2026 pregame Brier, margin MAE and calibration slope are within 2 SEs in both leagues (Brier
  moves by less than 5e-6). It stays at 0.44 by the owner's decision: the models fit downstream of the
  engine at 0.44 (the player WAR constants, the recruiting and transfer models) would all need refitting
  (on the 0.475 engine their pinned gates fail; refit commit ``51a86ae3e46``), for a negligible forecast gain;
* the player-value features (``usage``, ``tov_pct``): box-BPM passes for men and fails for women
  against the 2025 external BPM, and one weight serves both;
* the expected player box usage: points improve, assists get worse by more than 2 SEs in both leagues.

The numbers sit beside each constant.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

__all__ = ["COLLEGE_FT_POSSESSION_WEIGHT", "PRO_FT_POSSESSION_WEIGHT", "TS_FT_WEIGHT", "ft_possession_weight"]

#: NBA, WNBA and G League.
PRO_FT_POSSESSION_WEIGHT: float = 0.44
#: Men's and women's college.
COLLEGE_FT_POSSESSION_WEIGHT: float = 0.475
#: The free-throw weight of true-shooting attempts ``FGA + 0.44 FTA``, in every league.
TS_FT_WEIGHT: float = 0.44

_WEIGHTS: dict[str, float] = {
    **dict.fromkeys(("nba", "wnba", "gleague", "nbagl"), PRO_FT_POSSESSION_WEIGHT),
    **dict.fromkeys(("mbb", "wbb", "mens", "womens"), COLLEGE_FT_POSSESSION_WEIGHT),
}


def ft_possession_weight(league: str) -> float:
    """The free-throw weight of the box possession estimate for ``league``.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"gleague"`` / ``"nbagl"`` (0.44), or ``"mbb"`` / ``"wbb"`` /
            ``"mens"`` / ``"womens"`` (0.475).

    Returns:
        The weight ``w`` in ``FGA - ORB + TOV + w * FTA``.

    Raises:
        ValueError: If ``league`` is none of those.

    Example:
        Quick start::

            from sportsdataverse._common.possessions import ft_possession_weight
            ft_possession_weight("mbb")  # 0.475
    """
    try:
        return _WEIGHTS[league]
    except KeyError:
        raise ValueError(
            f"no free-throw possession weight for league {league!r}; use one of {sorted(_WEIGHTS)}"
        ) from None
