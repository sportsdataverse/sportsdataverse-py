"""Keep the offline network guard from leaking out of this directory.

Several tests here replace ``<processor module>.download`` with a function that
raises, to prove the fixture path never touches the network. Three of them
assign the attribute directly rather than through ``monkeypatch``, so the guard
survived the test and every later live test in the same worker failed with
"network call on the offline path". Snapshot before each module (module scope, so it
runs ahead of the module-scoped fixtures that call the processor) and restore after.
"""

import pytest


@pytest.fixture(autouse=True, scope="module")
def _restore_processor_download():
    import sportsdataverse.cfb.cfb_pbp as cfb
    import sportsdataverse.nfl.nfl_pbp as nfl

    saved = (cfb.download, nfl.download)
    yield
    cfb.download, nfl.download = saved
