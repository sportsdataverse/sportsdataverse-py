"""Release-management helpers for ``sportsdataverse/sportsdataverse-data``.

Python port of the ``sportsdataversedata`` R package (v0.0.11): save tidy
frames to rds / csv / csv.gz / parquet and upload them (plus timestamp /
package-function sidecar files) to a GitHub release via the ``gh`` CLI, and
inspect existing releases. ``rds`` files are written natively (pure-Python
serializer in ``sportsdataverse._rds``, byte-validated against R's own
``saveRDS`` output) including the ``sportsdataverse_type`` /
``sportsdataverse_timestamp`` frame attributes R attaches.

Deliberate divergences from the R package (documented in
``tests/release/test_release_parity.py``):

* uploads run one ``gh release upload`` invocation per file — the multi-file
  form silently drops large assets;
* the ``qs`` file type is R-only and raises ``ValueError``;
* ``size_string`` values are not right-justified across the frame (R's
  vector ``format()`` artifact);
* there is no ``.token`` argument — authentication is whatever the ``gh``
  CLI resolves, with ``GH_TOKEN`` falling back to ``GITHUB_PAT`` per the R
  package's ``.onLoad`` behavior.

Retry behavior mirrors ``zzz.R``: the whole upload is retried with
exponential backoff (``pause_base * 2^k`` after the k-th failure, floored at
``pause_min``, capped at purrr's default 60s), configured by the same env
vars the R package reads (``SPORTSDATAVERSE.UPLOAD.INSIST`` /
``.PAUSE_BASE`` / ``.PAUSE_MIN`` / ``.MAX_TIMES``). Unlike
``purrr::rate_backoff`` the pauses are not jittered.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Callable, Iterable
from typing import Any

import polars as pl

from sportsdataverse._rds import write_rds

DEFAULT_REPO = "sportsdataverse/sportsdataverse-data"

_VALID_FILE_TYPES = ("rds", "csv", "csv.gz", "parquet")
_R_ONLY_FILE_TYPES = ("qs",)

# regex ported verbatim from gh_cli.R .cli_parse_json (crayon::strip_style)
_ANSI_RE = re.compile("(?:(?:\x1b\\[)|\x9b)(?:(?:[0-9]{1,3})?(?:(?:;[0-9]{0,3})*)?[A-M|f-m])|\x1b[A-M]")

_ASSETS_SCHEMA: dict[str, type[pl.DataType]] = {
    "name": pl.Utf8,
    "size": pl.Int64,
    "downloads": pl.Int64,
    "last_update": pl.Utf8,
    "url": pl.Utf8,
    "size_string": pl.Utf8,
}

__all__ = [
    "DEFAULT_REPO",
    "gh_cli_available",
    "gh_cli_rate_limits",
    "gh_cli_release_assets",
    "gh_cli_release_tags",
    "gh_cli_release_upload",
    "sportsdataverse_save",
    "sportsdataverse_upload",
    "upload_release_sidecars",
    "write_release_sidecars",
]


def _gh_env() -> dict[str, str]:
    """Subprocess env for gh; GH_TOKEN falls back to GITHUB_PAT (zzz.R L31-33)."""
    env = dict(os.environ)
    if not env.get("GH_TOKEN") and env.get("GITHUB_PAT"):
        env["GH_TOKEN"] = env["GITHUB_PAT"]
    return env


# generous ceiling: multi-hundred-MB release assets legitimately upload for
# minutes; the timeout only guards against an indefinite network stall
_GH_TIMEOUT_SECONDS = 600.0


def _invoke_gh(args: list[str]) -> str:
    """Run ``gh <args>`` and return stdout; raise on any failure.

    The single subprocess chokepoint — tests monkeypatch this to run offline.
    """
    gh_cli_available()
    try:
        proc = subprocess.run(
            ["gh", *args],
            capture_output=True,
            text=True,
            timeout=_GH_TIMEOUT_SECONDS,
            env=_gh_env(),
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"The GitHub CLI timed out after {_GH_TIMEOUT_SECONDS:.0f}s for 'gh {' '.join(args)}'"
        ) from exc
    if proc.returncode != 0:
        raise RuntimeError(
            f"The GitHub CLI errored (exit {proc.returncode}) for 'gh {' '.join(args)}': {proc.stderr.strip()}"
        )
    return proc.stdout


def _parse_gh_json(output: str) -> Any:
    """ANSI-strip and JSON-parse gh output (gh_cli.R .cli_parse_json L172-178)."""
    return json.loads(_ANSI_RE.sub("", output))


def gh_cli_available() -> bool:
    """Check that the GitHub CLI is on the PATH.

    Returns:
        True when ``gh`` is available.

    Raises:
        RuntimeError: If the ``gh`` executable cannot be found.

    Example:
        Quick start::

            from sportsdataverse.release import gh_cli_available
            gh_cli_available()
    """
    if shutil.which("gh") is None:
        raise RuntimeError(
            "The GitHub Command Line Interface is not available on your machine! "
            "Please visit https://github.com/cli/cli#installation for install "
            "instructions."
        )
    return True


def _size_string(size: int) -> str:
    """Human-readable size matching ``as.character(rlang::as_bytes(size))``.

    Decimal (1000-based) units; two fixed decimals unless the scaled value is
    whole (``"1 kB"``, ``"1.50 kB"``, ``"38.39 kB"``). Oracle fixture:
    ``tests/fixtures/release/sizes_expected.csv``.
    """
    units = ("B", "kB", "MB", "GB", "TB", "PB", "EB")
    scaled = float(size)
    idx = 0
    while scaled >= 1000 and idx < len(units) - 1:
        scaled /= 1000
        idx += 1
    # rlang promotes to the next unit when 2-decimal rounding crosses 1000
    # (999999 -> "1.00 MB", not "1000.00 kB")
    if round(scaled, 2) >= 1000 and idx < len(units) - 1:
        scaled /= 1000
        idx += 1
    number = f"{scaled:g}" if scaled == int(scaled) else f"{scaled:.2f}"
    return f"{number} {units[idx]}"


def gh_cli_release_upload(
    files: Iterable[str | Path],
    tag: str,
    *,
    repo: str = DEFAULT_REPO,
    overwrite: bool = True,
) -> bool:
    """Upload files to a GitHub release (gh_cli.R L21-73).

    Missing files are skipped with a warning; when nothing is left to upload
    the function returns ``False`` without invoking ``gh``. Files are uploaded
    one per ``gh release upload`` invocation (the multi-file form silently
    drops large assets).

    Args:
        files: File paths to upload.
        tag: Release tag to upload to.
        repo: Target repository. Defaults to ``sportsdataverse/sportsdataverse-data``.
        overwrite: Pass ``--clobber`` so existing assets are replaced.

    Returns:
        True when at least one file was uploaded, False when none existed.

    Raises:
        RuntimeError: If the ``gh`` CLI is unavailable or an upload fails.

    Example:
        Quick start::

            from sportsdataverse.release import gh_cli_release_upload
            gh_cli_release_upload(["pbp_2024.parquet"], tag="espn_cfb_pbp")
    """
    paths = [Path(f) for f in files]
    missing = [p for p in paths if not p.is_file()]
    if missing:
        warnings.warn(
            f"The following files are missing: {[str(p) for p in missing]}",
            stacklevel=2,
        )
    paths = [p for p in paths if p.is_file()]
    if not paths:
        warnings.warn("There's nothing left to upload. Exiting!", stacklevel=2)
        return False

    for path in paths:
        args = ["release", "upload", tag, str(path), "-R", repo]
        if overwrite:
            args.append("--clobber")
        _invoke_gh(args)
    return True


def gh_cli_release_tags(repo: str = DEFAULT_REPO) -> list[str]:
    """List release tags of a repository (gh_cli.R L76-93).

    Args:
        repo: Repository to list. Defaults to ``sportsdataverse/sportsdataverse-data``.

    Returns:
        Release tag names, newest first (gh CLI ordering).

    Example:
        Quick start::

            from sportsdataverse.release import gh_cli_release_tags
            tags = gh_cli_release_tags()
    """
    payload = _parse_gh_json(_invoke_gh(["release", "list", "-R", repo, "--json", "tagName"]))
    return [entry["tagName"] for entry in payload]


def gh_cli_release_assets(
    tag: str,
    *,
    repo: str = DEFAULT_REPO,
    return_as_pandas: bool = False,
) -> Any:
    """List the assets of a release as a tidy frame (gh_cli.R L97-128).

    Timestamp sidecar assets are filtered out, matching the R behavior.

    Args:
        tag: Release tag to inspect.
        repo: Repository. Defaults to ``sportsdataverse/sportsdataverse-data``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Frame with columns ``name``, ``size``, ``downloads``, ``last_update``,
        ``url``, ``size_string``; zero rows when the release has no assets.

    Example:
        Quick start::

            from sportsdataverse.release import gh_cli_release_assets
            assets = gh_cli_release_assets("nfl_espn_qbr")
    """
    payload = _parse_gh_json(_invoke_gh(["release", "view", tag, "-R", repo, "--json", "assets"]))
    rows = [
        {
            "name": asset["name"],
            "size": asset["size"],
            "downloads": asset["downloadCount"],
            "last_update": asset["updatedAt"],
            "url": asset["url"],
            "size_string": _size_string(asset["size"]),
        }
        for asset in payload.get("assets", [])
        if "timestamp" not in asset["name"]
    ]
    df = pl.DataFrame(rows, schema=_ASSETS_SCHEMA)
    return df.to_pandas() if return_as_pandas else df


def gh_cli_rate_limits(verbose: bool = True) -> dict[str, Any]:
    """Return GitHub API rate limits (gh_cli.R L131-153).

    Args:
        verbose: Print the core rate block to stdout.

    Returns:
        The full ``gh api rate_limit`` payload; ``rate`` gains a
        ``reset_parsed`` UTC string.

    Example:
        Quick start::

            from sportsdataverse.release import gh_cli_rate_limits
            limits = gh_cli_rate_limits(verbose=False)
    """
    all_rates = _parse_gh_json(_invoke_gh(["api", "rate_limit"]))
    rate = all_rates["rate"]
    rate["reset_parsed"] = datetime.fromtimestamp(rate["reset"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if verbose:
        for key, value in rate.items():
            print(f"{key} : {value}")
    return all_rates


def _timestamp_now() -> str:
    """Timestamp string matching R ``format(Sys.time(), tz="America/Toronto", usetz=TRUE)``."""
    try:
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo("America/Toronto"))
    except Exception:  # ponytail: no tzdata on this box -> local time is fine
        now = datetime.now().astimezone()
    return now.strftime("%Y-%m-%d %H:%M:%S %Z")


def _create_timestamp_file(temp_dir: Path) -> list[Path]:
    """timestamp.txt + timestamp.json sidecars (upload.R L46-59)."""
    update_time = _timestamp_now()
    txt = temp_dir / "timestamp.txt"
    txt.write_text(update_time + "\n")
    js = temp_dir / "timestamp.json"
    js.write_text(json.dumps({"last_updated": update_time}, separators=(",", ":")) + "\n")
    return [txt, js]


def _create_package_function(temp_dir: Path, pkg_function: str) -> list[Path]:
    """package_function.txt + .json sidecars (upload.R L62-80)."""
    txt = temp_dir / "package_function.txt"
    txt.write_text(pkg_function + "\n")
    js = temp_dir / "package_function.json"
    js.write_text(json.dumps({"package_function": pkg_function}, separators=(",", ":")) + "\n")
    return [txt, js]


def write_release_sidecars(dest_dir: str | Path, pkg_function: str | None = None) -> list[Path]:
    """Write the release metadata sidecars R attaches to every published tag.

    Always writes ``timestamp.txt`` + ``timestamp.json``; adds
    ``package_function.txt`` + ``package_function.json`` when ``pkg_function``
    is given. This is the writer half of :func:`sportsdataverse_upload`, split
    out so a producer that owns its own ``gh`` transport -- most of the
    ``-data`` repos upload one file per invocation with a longer timeout, and
    inject the runner to keep their tests offline -- can still emit the same
    four files rather than re-deriving them.

    Args:
        dest_dir: Directory to write into. Must already exist.
        pkg_function: Related package function name, e.g.
            ``"hoopR::load_nba_pbp()"``. When None the package_function pair
            is skipped.

    Returns:
        The written paths, timestamp pair first.

    Example:
        Quick start::

            import tempfile
            from pathlib import Path
            from sportsdataverse.release import write_release_sidecars

            with tempfile.TemporaryDirectory() as td:
                paths = write_release_sidecars(Path(td), "hoopR::load_nba_pbp()")
                print([p.name for p in paths])

        Upload them through a producer's own runner::

            for sidecar in write_release_sidecars(Path(td), pkg_fn):
                run(["release", "upload", tag, str(sidecar), "--repo", repo, "--clobber"])

        See Also:
            * `hoopR`_ -- the R package whose ``sportsdataverse_save()`` this mirrors

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    dest = Path(dest_dir)
    sidecars = _create_timestamp_file(dest)
    if pkg_function is not None:
        sidecars += _create_package_function(dest, pkg_function)
    return sidecars


def upload_release_sidecars(
    tag: str,
    *,
    runner: Callable[[list[str]], Any],
    pkg_function: str | None = None,
    repo: str = DEFAULT_REPO,
) -> list[str]:
    """Write the release sidecars and push them through a caller's ``gh`` runner.

    The producer repos each own their upload transport -- one ``gh release
    upload`` per file, a longer subprocess timeout for 100MB+ assets, and an
    injected ``runner`` so their tests never touch the network. This lets them
    stamp a tag with the same four files :func:`sportsdataverse_upload` writes
    without giving up any of that: the sidecars are written to a temp dir and
    handed to ``runner`` one at a time, exactly like their own data assets.

    Call it once per tag AFTER the data assets land, so the timestamp reflects
    the finished upload rather than the start of it.

    Args:
        tag: Release tag to stamp.
        runner: Callable taking a ``gh`` argument list, e.g. the producer's
            ``_gh``. Injectable so tests stay offline.
        pkg_function: Related package function name. When None only the
            timestamp pair is uploaded.
        repo: Target repository. Defaults to ``sportsdataverse/sportsdataverse-data``.

    Returns:
        The uploaded sidecar file names, in upload order.

    Example:
        Quick start::

            from sportsdataverse.release import upload_release_sidecars
            upload_release_sidecars(
                "espn_nba_pbp",
                runner=_gh,
                pkg_function="hoopR::load_nba_pbp()",
                repo="sportsdataverse/sportsdataverse-data",
            )

        Hermetic test::

            calls = []
            upload_release_sidecars("t", runner=calls.append)

        See Also:
            * `hoopR`_ -- the R package whose ``sportsdataverse_save()`` this mirrors

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    uploaded: list[str] = []
    temp_dir = Path(tempfile.mkdtemp(prefix="sdv_sidecars_"))
    try:
        for sidecar in write_release_sidecars(temp_dir, pkg_function):
            runner(["release", "upload", tag, str(sidecar), "--repo", repo, "--clobber"])
            uploaded.append(sidecar.name)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return uploaded


def sportsdataverse_upload(
    files: Iterable[str | Path],
    tag: str,
    pkg_function: str | None = None,
    *,
    repo: str = DEFAULT_REPO,
    overwrite: bool = True,
) -> bool:
    """Upload files plus timestamp sidecars to a sportsdataverse release.

    Port of ``sportsdataverse_upload`` (upload.R L11-44) including the
    ``zzz.R`` retry wrapping: the whole upload (sidecar creation included) is
    retried with exponential backoff, configured by the same env vars the R
    package reads — ``SPORTSDATAVERSE.UPLOAD.INSIST`` (default ``"true"``),
    ``.PAUSE_BASE`` (0.05), ``.PAUSE_MIN`` (1), ``.MAX_TIMES`` (20).

    Args:
        files: Data files to upload.
        tag: Release tag to upload to.
        pkg_function: Related package function name, uploaded as
            ``package_function.txt`` / ``.json`` sidecars when given.
        repo: Target repository. Defaults to ``sportsdataverse/sportsdataverse-data``.
        overwrite: Pass ``--clobber`` so existing assets are replaced. Retry
            attempts always clobber regardless — a failed attempt may have
            landed some files already, and re-uploading them must succeed.

    Returns:
        True when files were uploaded, False when none existed.

    Raises:
        RuntimeError: When every retry attempt failed.

    Example:
        Quick start::

            from sportsdataverse.release import sportsdataverse_upload
            sportsdataverse_upload(
                ["qbr_week_level.parquet"],
                tag="nfl_espn_qbr",
                pkg_function="sportsdataverse.nfl.load_nfl_espn_qbr()",
            )
    """
    files = list(files)  # a generator would be exhausted on retry attempt 2
    insist = os.environ.get("SPORTSDATAVERSE.UPLOAD.INSIST", "true") == "true"
    pause_base = float(os.environ.get("SPORTSDATAVERSE.UPLOAD.PAUSE_BASE", "0.05"))
    pause_min = float(os.environ.get("SPORTSDATAVERSE.UPLOAD.PAUSE_MIN", "1"))
    max_times = int(float(os.environ.get("SPORTSDATAVERSE.UPLOAD.MAX_TIMES", "20"))) if insist else 1

    def _upload_once(force_clobber: bool) -> bool:
        temp_dir = Path(tempfile.mkdtemp(prefix="sdv_release_"))
        try:
            sidecars = write_release_sidecars(temp_dir, pkg_function)
            return gh_cli_release_upload(
                [*files, *sidecars],
                tag=tag,
                repo=repo,
                # retries re-upload files a failed earlier attempt may have
                # already landed; without --clobber those would fail forever
                overwrite=overwrite or force_clobber,
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    for attempt in range(1, max_times + 1):
        try:
            return _upload_once(force_clobber=attempt > 1)
        except Exception:
            if attempt == max_times:
                raise
            # purrr::rate_backoff: pause after the k-th failure is
            # pause_base * 2^k, floored at pause_min, capped at 60s.
            # ponytail: no jitter — deterministic is fine here
            time.sleep(min(60.0, max(pause_min, pause_base * 2**attempt)))
    return False  # pragma: no cover - loop always returns or raises


def sportsdataverse_save(
    data_frame: Any,
    file_name: str,
    sportsdataverse_type: str,
    release_tag: str,
    pkg_function: str,
    *,
    file_types: Iterable[str] = ("rds", "csv", "parquet"),
    repo: str = DEFAULT_REPO,
) -> list[Path]:
    """Save a frame in release formats and upload it (upload.R L100-188).

    Coerces ``season`` / ``week`` columns to integer (Int32, matching R
    ``as.integer``), stamps ``sportsdataverse_type`` and
    ``sportsdataverse_timestamp`` into the parquet file metadata and as R
    attributes on the rds frame, writes the requested formats to a temp
    directory, and uploads them together with the timestamp /
    package-function sidecars.

    Args:
        data_frame: polars (or pandas) DataFrame to save.
        file_name: Asset file name, without extension.
        sportsdataverse_type: Dataset description stored in parquet metadata
            and as an rds frame attribute.
        release_tag: Release tag to upload to.
        pkg_function: Related package function name (sidecar metadata).
        file_types: Subset of ``("rds", "csv", "csv.gz", "parquet")``; the
            default matches the R package. The R-only ``qs`` serialization
            raises ``ValueError`` — write that from R via
            ``sportsdataversedata``.
        repo: Target repository. Defaults to ``sportsdataverse/sportsdataverse-data``.

    Returns:
        Paths of the data files written (sidecars excluded). They live in a
        fresh temp directory; the caller owns any cleanup (R leaves them in
        the session ``tempdir()`` the same way).

    Raises:
        ValueError: On unknown or R-only file types.
        RuntimeError: When the upload fails after all retries.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.release import sportsdataverse_save
            df = pl.DataFrame({"season": [2024], "week": [1], "epa": [0.12]})
            sportsdataverse_save(
                df,
                file_name="example",
                sportsdataverse_type="Example dataset",
                release_tag="example-tag",
                pkg_function="sportsdataverse.example.load_example()",
            )
    """
    requested = list(file_types)
    unknown = [ft for ft in requested if ft not in _VALID_FILE_TYPES]
    if unknown:
        r_only = [ft for ft in unknown if ft in _R_ONLY_FILE_TYPES]
        if r_only:
            raise ValueError(
                f"file_types {r_only} are R-only serialization formats — "
                f"use the sportsdataversedata R package for those. "
                f"Valid values: {_VALID_FILE_TYPES}"
            )
        raise ValueError(f"Unknown file_types {unknown}; valid: {_VALID_FILE_TYPES}")

    df = data_frame if isinstance(data_frame, pl.DataFrame) else pl.from_pandas(data_frame)

    # R: as.integer() on season/week when present (upload.R L128-133).
    # Strings go through Float64 first: R parses character via double then
    # truncates, so "2024.0" / " 2024" / "2023.7" must coerce, not null out.
    for col in ("season", "week"):
        if col in df.columns:
            expr = pl.col(col)
            if df.schema[col] == pl.Utf8:
                expr = expr.str.strip_chars().cast(pl.Float64, strict=False)
            nulls_before = df[col].null_count()
            df = df.with_columns(expr.cast(pl.Int32, strict=False))
            if df[col].null_count() > nulls_before:
                warnings.warn(
                    f"{col}: non-numeric values coerced to null (R: 'NAs introduced by coercion')",
                    stacklevel=2,
                )

    stamped_at = datetime.now()
    metadata = {
        "sportsdataverse_type": sportsdataverse_type,
        "sportsdataverse_timestamp": str(stamped_at),
    }

    temp_dir = Path(tempfile.mkdtemp(prefix="sdv_release_"))
    written: list[Path] = []
    if "rds" in requested:
        path = temp_dir / f"{file_name}.rds"
        # attribute parity with R: attr(df, "sportsdataverse_type"/"..._timestamp")
        write_rds(
            df,
            path,
            attributes={
                "sportsdataverse_type": sportsdataverse_type,
                "sportsdataverse_timestamp": stamped_at,
            },
        )
        written.append(path)
    if "csv" in requested:
        path = temp_dir / f"{file_name}.csv"
        df.write_csv(path)
        written.append(path)
    if "csv.gz" in requested:
        path = temp_dir / f"{file_name}.csv.gz"
        df.write_csv(path, compression="gzip")
        written.append(path)
    if "parquet" in requested:
        path = temp_dir / f"{file_name}.parquet"
        df.write_parquet(path, metadata=metadata)
        written.append(path)

    sportsdataverse_upload(written, tag=release_tag, pkg_function=pkg_function, repo=repo)
    return written
