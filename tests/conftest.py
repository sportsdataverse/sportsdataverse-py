"""Shared pytest fixtures and skip helpers for the sportsdataverse test suite.

Live-API gating
---------------
Tests that hit external services (ESPN, NHL api-web, MLB Stats API,
Baseball Savant) are gated behind the ``SDV_PY_LIVE_TESTS`` environment
variable so CI doesn't flake on upstream downtime and so contributors
don't accidentally hit live endpoints during local development.

Set ``SDV_PY_LIVE_TESTS=1`` to enable them::

    SDV_PY_LIVE_TESTS=1 pytest tests/test_espn_live.py -v
    SDV_PY_LIVE_TESTS=1 pytest tests/wbb/

The gating is OPT-IN — without the env var, gated tests are skipped, not
failed.

Two usage patterns
~~~~~~~~~~~~~~~~~~

**Per-test decorator**: apply ``@skip_if_no_live`` to a single test::

    from tests.conftest import skip_if_no_live

    @skip_if_no_live
    def test_my_thing():
        ...

**Module-level marker**: apply to every test in a file via
``pytestmark`` (used by :mod:`tests.test_espn_live` and any future
``test_*_live.py``)::

    from tests.conftest import skip_if_no_live
    pytestmark = skip_if_no_live

Both forms read the env var at collection time so unsetting it inside a
test won't suddenly unskip the rest of the module.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from urllib.error import HTTPError, URLError

import pytest

LIVE: bool = os.environ.get("SDV_PY_LIVE_TESTS") == "1"

_LIVE_REASON = "Set SDV_PY_LIVE_TESTS=1 to run tests that hit live external APIs"
skip_if_no_live = pytest.mark.skipif(not LIVE, reason=_LIVE_REASON)


def skip_on_transient_network_error(exc: BaseException) -> None:
    """Skip (never fail) a live test hit by a transient upstream problem.

    CI runs the OS matrix (macos/ubuntu/windows) in parallel, so a shared
    upstream (e.g. raw.githubusercontent.com for the DynastyProcess CSV/parquet
    loaders) can rate-limit concurrent requests with HTTP 429 even though the
    endpoint is otherwise healthy. That's an upstream-availability flake, not a
    regression in this repo's code, so it should ``skip`` the same way the
    ESPN "incomplete data" gap does above -- never silently pass with fake
    data, and never assert on it either. Anything else re-raises.
    """
    if _is_transient(exc):
        pytest.skip(f"Live upstream fetch failed transiently: {exc}")
    raise exc


def _is_transient(exc: BaseException | None) -> bool:
    """A timeout, dropped connection or 429/5xx anywhere in ``exc``'s cause chain.

    Covers requests' own ``Timeout`` / ``ConnectionError``, which are not the builtin classes.
    """
    import requests

    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, HTTPError):  # a URLError subclass: decided by status, so a 404 still fails
            return exc.code in (429, 502, 503, 504)
        if isinstance(exc, (URLError, ConnectionError, TimeoutError, requests.Timeout, requests.ConnectionError)):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item):
    """Every ``@skip_if_no_live`` test skips, rather than fails, on a transient upstream problem.

    The live job runs on main only, where nobody can act on a third-party outage; a skip
    still shows in the summary, and the next run retries.
    """
    try:
        return (yield)
    except Exception as exc:
        live = any(m.kwargs.get("reason") == _LIVE_REASON for m in item.iter_markers("skipif"))
        if live and _is_transient(exc):
            pytest.skip(f"Live upstream fetch failed transiently: {exc!r}")
        raise


# stats.nba.com / stats.wnba.com hang on datacenter / cloud IPs: the TLS/JA3
# fingerprint block compounds with IP reputation, so even with curl_cffi browser
# impersonation the request silently stalls (not a fast failure) from CI runners.
# These live tests therefore need a SEPARATE opt-in that NO CI workflow sets
# (tests.yml + live-tests-cron.yml only ever set SDV_PY_LIVE_TESTS), so they run
# only when a contributor explicitly enables them from a residential IP.
NBA_STATS_LIVE: bool = os.environ.get("SDV_PY_NBA_STATS_LIVE") == "1"

skip_if_no_nba_stats_live = pytest.mark.skipif(
    not NBA_STATS_LIVE,
    reason=(
        "stats.nba.com/stats.wnba.com hang on datacenter/cloud IPs; set "
        "SDV_PY_NBA_STATS_LIVE=1 to run these live tests from a residential IP"
    ),
)

# premium.pff.com is paywalled (a PFF+ session cookie) and best exercised from a
# residential IP; like the nba-stats gate, NO CI workflow sets this — it runs
# only when a contributor explicitly enables it.
PFF_LIVE: bool = os.environ.get("SDV_PY_PFF_LIVE") == "1"

skip_if_no_pff_live = pytest.mark.skipif(
    not PFF_LIVE,
    reason=(
        "PFF Premium is paywalled (needs a PFF+ session); set SDV_PY_PFF_LIVE=1 "
        "to run these live tests from a residential IP"
    ),
)

# ipa/www.247sports.com sit behind a Fastly edge that may hang (not fail fast) on
# datacenter/CI IPs the way stats.nba.com does; both 247 tracks (RDB + site-pages)
# share this ONE gate, which NO CI workflow sets.
SPORTS247_LIVE: bool = os.environ.get("SDV_PY_247_LIVE") == "1"

skip_if_no_247_live = pytest.mark.skipif(
    not SPORTS247_LIVE,
    reason=(
        "ipa/www.247sports.com may hang on datacenter/CI IPs; set SDV_PY_247_LIVE=1 "
        "to run these live tests from a residential IP"
    ),
)

# Concurrent-validity tests correlate a computed metric (e.g. nba_la_rapm,
# nba_decay_rapm) against a real published oracle (Ryan Davis RAPM CSVs). The
# oracle files aren't bundled with the repo, so these tests skip cleanly
# unless a contributor points SDV_PY_NBA_ORACLE_DIR at a local checkout.
skip_if_no_nba_oracle = pytest.mark.skipif(
    not os.environ.get("SDV_PY_NBA_ORACLE_DIR"),
    reason="SDV_PY_NBA_ORACLE_DIR not set (Ryan Davis oracle CSVs unavailable)",
)


def _rscript_available() -> bool:
    try:
        from tools.validation.lint.leakage_r import rscript_path
    except ImportError:
        return False
    return rscript_path() is not None


skip_if_no_rscript = pytest.mark.skipif(
    not _rscript_available(),
    reason="Rscript not found — install R or set SDV_RSCRIPT to run the R-lint live tests",
)


# ---------------------------------------------------------------------------
# Captured-fixture loader
# ---------------------------------------------------------------------------

FIXTURES_ROOT = Path(__file__).parent / "fixtures"


def patch_espn_fetch(monkeypatch, payload):
    """Point every codegen-emitted ESPN wrapper at ``payload`` instead of the network.

    The generated ``*_espn_ext`` modules import ``_get`` from
    :mod:`sportsdataverse._codegen_runtime`, so rebinding
    ``sportsdataverse._common_espn._get`` -- a re-export those modules never look at
    -- patches nothing and silently leaves the real HTTP call in place. Five tests
    did exactly that and reached live ESPN from inside the "offline" CI job. Patch
    the sink ``download`` instead, which is what every generated wrapper bottoms out
    in.

    Args:
        monkeypatch: the test's ``monkeypatch`` fixture.
        payload: the JSON body to serve, or a ``url -> body`` callable for tests that
            need to vary the response per endpoint.
    """
    import sportsdataverse._codegen_runtime as rt

    resolve = payload if callable(payload) else (lambda url: payload)

    class _Resp:
        def __init__(self, body: object) -> None:
            self._body = body

        def json(self) -> object:
            return self._body

    monkeypatch.setattr(rt, "download", lambda url, params=None, **kwargs: _Resp(resolve(url)))


def load_fixture(category: str, stem: str) -> dict:
    """Load a JSON fixture from ``tests/fixtures/{category}/{stem}.json``.

    Single shared helper used by every ``test_*_parsers.py`` file so the
    five parser test modules don't each carry their own copy of the
    same ``json.loads((FIXTURE_DIR / f'{stem}.json').read_text(...))``
    boilerplate.

    Args:
        category: Fixture subdirectory under ``tests/fixtures/``
            (``"espn"``, ``"mlb_api"``, ``"nhl_api_web"``, ``"nhl_edge"``,
            ``"nhl_stats_rest"``, ``"nhl_records"``).
        stem: Filename without the ``.json`` extension
            (e.g. ``"summary_nba"``, ``"team_roster_nfl"``).

    Returns:
        Parsed JSON payload as a Python ``dict`` (or list — whatever
        the fixture contains at the top level).

    Raises:
        FileNotFoundError: If the fixture doesn't exist. The error
            message points at the expected path so missing-fixture bugs
            are easy to locate.
    """
    path = FIXTURES_ROOT / category / f"{stem}.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Fixture not found: {path}. Expected category={category!r}, stem={stem!r}.",
        )
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_pbp_or_skip(proc):
    """Run a CFB/NFL ``*PlayProcess`` live ESPN fetch, gating + skipping cleanly.

    This is the single live-fetch chokepoint for the PBP test modules, so it owns
    the live gate: it ``skip``s unless ``SDV_PY_LIVE_TESTS=1`` (same contract as
    :data:`skip_if_no_live`) -- meaning every fixture / test that fetches through
    it is gated, with no per-test decorator to forget.

    When live, ESPN's summary endpoint intermittently returns a body with no
    ``header.competitions`` (offseason / a game not yet ingested). The PBP
    processors raise :class:`~sportsdataverse.errors.NoESPNDataError` for that case
    instead of a bare ``KeyError``; we treat it as a transient gap and ``skip``
    rather than fail. The guard fires inside ``run_processing_pipeline()`` (not the
    fetch), so this helper runs **both** the fetch and the pipeline under one
    ``try`` -- ``run_processing_pipeline`` is idempotent, so a test/fixture calling
    it again afterward just gets the cached result. ``proc`` is an ``NFLPlayProcess``
    / ``CFBPlayProcess`` (the league ``espn_*_pbp`` method is auto-detected).
    Returns ``proc`` for chaining.
    """
    if not LIVE:
        pytest.skip("Live ESPN PBP fetch — set SDV_PY_LIVE_TESTS=1 to run.")

    from sportsdataverse.errors import NoESPNDataError

    fetch = getattr(proc, "espn_cfb_pbp", None) or getattr(proc, "espn_nfl_pbp", None)
    try:
        fetch()
        proc.run_processing_pipeline()
    except NoESPNDataError as exc:
        pytest.skip(f"ESPN returned incomplete data for game {getattr(proc, 'gameId', '?')}: {exc}")
    return proc


# ---------------------------------------------------------------------------
# Committed-tree guard: no test may write into the repo.
# ---------------------------------------------------------------------------
# A test that rewrites a committed file -- even one that restores the bytes
# afterwards -- leaves it truncated or missing for a moment, and under ``-n auto``
# a parallel reader lands in that moment (codegen generator tests that wiped
# ``schemas/native/<stem>/`` produced FileNotFoundError in schema readers). Write
# to ``tmp_path``; generators take a monkeypatched ``ROOT`` / output path.
#
# ponytail: the fingerprint is (mtime_ns, size), not a content hash -- a same-bytes
# rewrite is still the race, and stat is ~5x cheaper than hashing the ~300 MB tree.
# It runs in the xdist controller only (workers carry ``workerinput``); the
# controller's sessionfinish fires after every worker has exited. git is asked
# for the file list once, at start; the end re-stats that same list, so a git
# hiccup at the end cannot read as "every file changed".
_GUARDED_DIRS = ("tools/codegen", "docs/docs", "sportsdataverse", "tests/fixtures")
_REPO_ROOT = Path(__file__).resolve().parents[1]
_tree_at_start: dict[str, tuple[int, int]] | None = None


def _tracked_files() -> list[str] | None:
    """Tracked files under ``_GUARDED_DIRS``; None outside a git checkout (an sdist)."""
    try:
        out = subprocess.run(
            ["git", "ls-files", "-z", "--", *_GUARDED_DIRS],
            cwd=_REPO_ROOT,
            capture_output=True,
            check=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    return [rel for rel in out.decode("utf-8").split("\0") if rel]


def _fingerprint(files: list[str]) -> dict[str, tuple[int, int]]:
    """(mtime_ns, size) per file; (-1, -1) for a missing one."""
    fingerprint = {}
    for rel in files:
        try:
            st = (_REPO_ROOT / rel).stat()
            fingerprint[rel] = (st.st_mtime_ns, st.st_size)
        except FileNotFoundError:
            fingerprint[rel] = (-1, -1)
    return fingerprint


def pytest_sessionstart(session: pytest.Session) -> None:
    global _tree_at_start
    if not hasattr(session.config, "workerinput"):
        files = _tracked_files()
        _tree_at_start = None if files is None else _fingerprint(files)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if _tree_at_start is None:
        return
    after = _fingerprint(list(_tree_at_start))
    changed = sorted(rel for rel, fp in _tree_at_start.items() if after[rel] != fp)
    if not changed:
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    write = reporter.write_line if reporter else print
    write("")
    write(f"ERROR: the test session wrote {len(changed)} committed file(s); tests must write to tmp_path:")
    for rel in changed:
        write(f"  {rel}")
    if session.exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
