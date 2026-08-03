"""Write guard: never let a provider error body into the archive.

The raw tree is the scrape checkpoint -- a captured file is never re-fetched.
So a persisted error body is permanent: it silently yields an empty dataset for
that key on every rebuild, forever, with nothing failing anywhere. Seven such
files were found in ``wbb/team_stats/json/`` when the sportsdataverse payload
schemas were first run against the committed tree, spanning 2007-2023.

Refusing the write is deliberately the whole fix. A refused key simply looks
un-scraped, so the next run retries it -- the archive self-heals instead of
accumulating a quarantine directory nobody reads.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Key sets that only ever appear in a provider ERROR body, never in a data
# payload. Matched as subsets so extra keys in the error body still match.
_ERROR_SIGNATURES: tuple[frozenset[str], ...] = (
    # Spring-style error, e.g. {"error","message","path","status","timestamp"}
    # where `status` is an int HTTP code. Seen at wbb/team_stats/json/2007/2948.
    frozenset({"error", "message", "status"}),
    # ESPN API error, e.g. {"code": 404, "detail": "no data"}.
    frozenset({"code", "detail"}),
)


def is_error_payload(payload: Any) -> bool:
    """True when a payload is empty or is a provider error body.

    A structurally valid but empty *collection* (``{"count": 0, "items": []}``)
    is NOT an error -- it is a real answer meaning "nothing here", and treating
    it as a failure would re-scrape that key on every run forever.
    """
    if not payload or not isinstance(payload, dict):
        return True
    keys = set(payload)
    return any(signature <= keys for signature in _ERROR_SIGNATURES)


def write_payload(path: Path | str, payload: dict, *, indent: int | None = None) -> bool:
    """Persist a payload unless it is empty or an error body.

    Args:
        path: Destination file. Parent directories are created as needed.
        payload: The parsed provider response.
        indent: ``json.dump`` indent. Defaults to None (compact), which is what
            ``wehoop-wbb-raw`` has always written. The hoopR/wehoop ESPN
            archives were written with ``indent=0`` and pass that through, so
            adopting this guard does not silently reformat a tree that is
            committed to git -- a format flip would churn the diff of every
            file any later run happens to rewrite.

    Returns:
        True when written, False when refused. Refusing leaves any existing
        capture at ``path`` untouched -- a later error must never truncate an
        earlier success, which is the failure that turns a working season into
        a silent gap.
    """
    if is_error_payload(payload):
        return False
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=indent), encoding="utf-8")
    return True


def scan_for_error_payloads(root: Path | str, pattern: str) -> list[Path]:
    """Find committed captures that are error bodies, empty, or unreadable.

    Args:
        root: Directory to scan from.
        pattern: Glob relative to ``root``, e.g. ``"wbb/team_stats/json/*/*.json"``.

    Returns:
        Sorted paths of every file that should not be in the archive.
    """
    root = Path(root)
    bad: list[Path] = []
    for path in sorted(root.glob(pattern)):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            bad.append(path)
            continue
        if is_error_payload(payload):
            bad.append(path)
    return bad
