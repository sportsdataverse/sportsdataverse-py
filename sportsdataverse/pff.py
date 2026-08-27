"""League-prefixed PFF wrappers for the top-level ``sportsdataverse`` namespace.

The per-league shims all install the same 46 bare names bound to different
leagues, so they cannot be star-imported into one namespace without colliding.
This module mints ``pff_<slug>_<rest>`` aliases for every league instead, which
is what makes PFF reachable from ``sportsdataverse`` alongside every other
source family.

Example:
    Same report, three leagues::

        import sportsdataverse as sdv

        sdv.pff_nfl_facet_blocking_summary()
        sdv.pff_ncaa_facet_blocking_summary()
        sdv.pff_ufl_facet_blocking_summary()

    The module-scoped bare names are unchanged::

        from sportsdataverse.nfl import pff
        pff.pff_facet_blocking_summary()
"""

from __future__ import annotations

from sportsdataverse.nfl.pff_league import make_pff_alias_module

__all__ = make_pff_alias_module(globals())
