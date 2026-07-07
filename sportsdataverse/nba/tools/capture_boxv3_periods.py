"""One-off: capture per-period ``boxscoretraditionalv3`` range payloads.

Writes a single JSON fixture mapping period-number-as-string -> the verbatim
``boxscoretraditionalv3`` envelope fetched for that period's opening
``StartRange``/``EndRange`` window (``RangeType=2``, the pbpstats
convention -- verified against a live capture, see Task 1 of the NBA
quarter-box lineups plan).

Run from a residential IP (``stats.nba.com`` TLS/JA3-blocks datacenter IPs)
or with a working ``proxy_url``. This module is NOT imported by the package
at runtime -- it is a capture tool only, invoked via::

    SDV_PY_NBA_STATS_LIVE=1 uv run python -m sportsdataverse.nba.tools.capture_boxv3_periods \\
        <game_id> <n_periods> <out_path> [proxy_url]
"""

from __future__ import annotations

import json
import pathlib
import sys
import time
from typing import Dict, Optional

from sportsdataverse.nba.nba_stats import nba_stats_boxscoretraditionalv3

#: RangeType for period-opening on-court range-boxscores (pbpstats convention).
_RANGE_TYPE: str = "2"

#: Politeness delay (seconds) between successive live stats.nba.com calls.
_THROTTLE_SECONDS: float = 2.0


def _period_start_tenths(period: int) -> int:
    """Elapsed game time (tenths of a second) at *period*'s opening tick.

    Args:
        period: 1-indexed period number (5+ = overtime, 5 minutes each).

    Returns:
        Elapsed time in tenths of a second since game start.
    """
    elapsed_s = (period - 1) * 720 if period <= 4 else 2880 + (period - 5) * 300
    return elapsed_s * 10


#: Window width added to StartRange to form EndRange (verified against pbpstats'
#: own ``StartOfPeriod._get_period_boxscore_request_params`` "rt2_start_window"
#: mode: ``EndRange = period_start_tenths + 10``, i.e. a 1-second opening window).
_WINDOW_WIDTH_TENTHS: int = 10


def capture(
    game_id: str,
    n_periods: int,
    out: pathlib.Path,
    proxy_url: Optional[str] = None,
) -> None:
    """Capture one ``boxscoretraditionalv3`` payload per period and write JSON.

    Args:
        game_id: Ten-character NBA ``GameID``.
        n_periods: Number of periods to capture (4 for a regulation-only game;
            include OT periods if the game went beyond regulation).
        out: Destination path for the combined JSON fixture. Parent
            directories are created if missing.
        proxy_url: Optional proxy URL forwarded to the injectable
            ``nba_stats_runtime._get`` transport.

    Example:
        Quick start::

            import pathlib
            from sportsdataverse.nba.tools.capture_boxv3_periods import capture
            capture("0022300001", 4, pathlib.Path("out/boxv3_periods.json"))
    """
    out.parent.mkdir(parents=True, exist_ok=True)
    payloads: Dict[str, dict] = {}
    for period in range(1, n_periods + 1):
        t = _period_start_tenths(period)
        raw = nba_stats_boxscoretraditionalv3(
            game_id=game_id,
            start_range=str(t),
            end_range=str(t + _WINDOW_WIDTH_TENTHS),
            range_type=_RANGE_TYPE,
            return_parsed=False,
            proxy_url=proxy_url,
        )
        payloads[str(period)] = raw
        if period < n_periods:
            time.sleep(_THROTTLE_SECONDS)
    out.write_text(json.dumps(payloads))
    print(f"wrote {out} ({len(payloads)} periods)")


if __name__ == "__main__":
    _game_id = sys.argv[1]
    _n_periods = int(sys.argv[2])
    _out = pathlib.Path(sys.argv[3])
    _proxy_url = sys.argv[4] if len(sys.argv) > 4 else None
    capture(_game_id, _n_periods, _out, proxy_url=_proxy_url)
