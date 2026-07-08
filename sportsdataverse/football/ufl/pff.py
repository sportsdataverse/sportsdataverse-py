"""PFF Premium Stats -- UFL league shim over :mod:`sportsdataverse.nfl.pff_core`.

Exposes the generated ``pff_*`` wrappers with ``league="ufl"`` pre-bound. See the
generated ``pff`` reference (under the NFL docs) for the full endpoint surface.
"""

from sportsdataverse.nfl.pff_league import make_pff_league_module

__all__ = make_pff_league_module(globals(), "ufl")
