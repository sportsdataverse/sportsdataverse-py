"""sportsdataverse._common_espn — shared ESPN host constants + legacy helper re-export.

**History:** this module used to host a runtime *factory* (``make_league_module`` +
``_bind`` + the ``_UNIVERSAL_WRAPPERS``/``_NCAA_WRAPPERS``/``_FOOTBALL_WRAPPERS``/
``_MLB_WRAPPERS`` tables + ~127 ``_site_v2_*`` / ``_espn_*`` / ``_core_v2_*`` core
functions) that mass-generated each league's ``espn_<league>_*`` wrappers at import
time. That magic has been **retired** in favour of a declarative codegen pipeline:
the per-league wrappers are now concrete, fully-documented modules generated from
``tools/codegen/endpoints/*.yaml`` into ``sportsdataverse/<league>/<league>_espn_ext.py``.

What remains here:

* the four ESPN host base URLs (still the canonical reference for the codegen host
  table in ``tools/codegen/endpoints/leagues.yaml`` and the build-time extractor), and
* a re-export of ``_get`` / ``_csv`` from :mod:`sportsdataverse._codegen_runtime` for
  any legacy importer that still reaches for ``_common_espn._get``.

The endpoint↦parser registry lives on in :mod:`sportsdataverse._common_espn_parsers`
(``ENDPOINT_PARSERS`` / ``parser_for``); the generated wrappers import their parsers
from there directly.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Hosts (canonical ESPN base URLs; mirrored in tools/codegen/endpoints/leagues.yaml)
# ---------------------------------------------------------------------------

_SITE_V2 = "https://site.api.espn.com/apis/site/v2/sports"
_SITE_V2_ALT = "https://site.api.espn.com/apis/v2/sports"
_WEB_V3 = "https://site.web.api.espn.com/apis/common/v3/sports"
_CORE_V2 = "https://sports.core.api.espn.com/v2/sports"
_CORE_V3 = "https://sports.core.api.espn.com/v3/sports"


# ---------------------------------------------------------------------------
# Internal helpers (single source of truth lives in _codegen_runtime)
# ---------------------------------------------------------------------------

from sportsdataverse._codegen_runtime import _csv, _get  # noqa: E402,F401
