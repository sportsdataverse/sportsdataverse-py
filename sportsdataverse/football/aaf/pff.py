"""PFF Premium Stats -- AAF league shim over :mod:`sportsdataverse.nfl.pff_core`.

Exposes the generated ``pff_*`` wrappers with ``league="aaf"`` pre-bound. AAF data is
2019-only and some reports are empty; the parsers return zero-row frames for those. See
the generated ``pff`` reference (under the NFL docs) for the full endpoint surface.
"""

from sportsdataverse.nfl.pff_league import make_pff_league_module

__all__ = make_pff_league_module(globals(), "aaf")
