"""sportsdataverse.ohl -- live OHL HockeyTech wrappers (core set + analytics)."""

from __future__ import annotations

from sportsdataverse.hockeytech._family import build_family

_family = build_family("ohl")
globals().update(_family)
__all__ = list(_family)
