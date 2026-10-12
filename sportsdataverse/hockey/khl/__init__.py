"""KHL wrappers over www.khl.ru/rest + khl.api.webcaster.pro (codegen flat-API family ``khl_rest`` + ``khl_webcaster``).

Unofficial use of a public source: payloads are not redistributed as release assets. Owner flag
(``sdv-internal-refs/khl/README.md``): the KHL and its webcaster platform are run by Russian operators,
some clubs by sanctioned state companies; any published data release, hosted mirror or redistribution
needs owner (sanctions / legal) sign-off first.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES). The generated modules (khl_rest,
# khl_webcaster) are re-exported here by codegen once they exist.
from sportsdataverse.hockey.khl.khl_rest_parsers import *  # noqa: F401,F403
from sportsdataverse.hockey.khl.khl_webcaster_parsers import *  # noqa: F401,F403
