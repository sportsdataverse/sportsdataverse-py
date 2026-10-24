"""tools/upstream_baseline.sh: only a real sync PR gets the upstream baseline, and every error fails closed.

A tiny origin holds the topic chain (main -> devel-program-setup -> devel-test-harness -> devel-cfb)
plus GitHub-style ``refs/pull/N/merge`` refs. Each case clones it ``--depth 1`` and checks out the
merge ref detached, as actions/checkout does, then runs the script with ``bash``. Stubbed ``git``
wrappers force the error paths (a history that stays shallow, a failed fetch, merge-base exit 128).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "upstream_baseline.sh"


def _bash() -> str | None:
    if os.name != "nt":
        return shutil.which("bash")
    # Git for Windows' bash, never C:\Windows\System32\bash.exe (WSL, which cannot see this git)
    git = shutil.which("git")
    for parent in Path(git).parents if git else []:
        for cand in (parent / "bin" / "bash.exe", parent / "usr" / "bin" / "bash.exe"):
            if cand.is_file():
                return str(cand)
    return None


BASH = _bash()
pytestmark = pytest.mark.skipif(
    BASH is None or shutil.which("git") is None, reason="needs bash and git (Git for Windows' bash on Windows)"
)


@pytest.fixture(scope="module")
def env(tmp_path_factory) -> dict[str, str]:
    empty = tmp_path_factory.mktemp("gitcfg") / "gitconfig"
    empty.write_text("", encoding="utf-8")
    return {
        **os.environ,
        "GIT_CONFIG_GLOBAL": str(empty),  # no user hooks, signing or autocrlf leak into the scratch repos
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@x",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@x",
    }


def _git(cwd: Path, env: dict[str, str], *args: str) -> str:
    out = subprocess.run(["git", *args], cwd=cwd, env=env, capture_output=True, text=True, check=True)
    return out.stdout.strip()


@pytest.fixture(scope="module")
def origin(tmp_path_factory, env) -> str:
    work = tmp_path_factory.mktemp("origin") / "work"
    work.mkdir()
    g = lambda *a: _git(work, env, *a)  # noqa: E731

    def commit(name: str, baseline: str | None = None) -> None:
        (work / f"f_{name}").write_text(name, encoding="utf-8")
        if baseline:
            (work / "baseline").mkdir(exist_ok=True)
            (work / "baseline" / "scoreboard-devel.json").write_text(f'{{"who": "{baseline}"}}\n', encoding="utf-8")
        g("add", "-A")
        g("commit", "-qm", name)

    def merge(rev: str) -> None:
        g("merge", "-q", "--no-edit", "-X", "theirs", rev)

    g("init", "-q", "-b", "main")
    commit("m1")
    g("checkout", "-qb", "devel-program-setup")
    commit("ps1", "ps-1")
    g("checkout", "-qb", "devel-test-harness")
    commit("th1", "th-1")
    old_th = g("rev-parse", "HEAD")
    g("checkout", "-qb", "devel-cfb")
    commit("cfb1", "cfb-1")
    g("checkout", "-q", "devel-test-harness")
    commit("th2", "th-2")
    th2 = g("rev-parse", "HEAD")
    g("checkout", "-q", "devel-cfb")
    commit("cfb2")
    g("checkout", "-qb", "task", "devel-cfb")
    commit("task1")
    g("checkout", "-qb", "stale-old", "devel-cfb")
    merge(old_th)  # an upstream commit devel-cfb already has
    commit("stale")
    g("checkout", "-q", "devel-test-harness")
    commit("th3", "th-3")  # the current upstream tip
    g("checkout", "-qb", "partial", "devel-cfb")
    merge(th2)  # upstream commits the base lacks, but not the tip
    g("checkout", "-qb", "devel-test-harness-spoof", "devel-cfb")
    commit("spoof")
    g("checkout", "-qb", "sync-current", "devel-cfb")
    merge("devel-test-harness")
    g("checkout", "-qb", "sync-plus-task", "sync-current")
    commit("sneaky")
    g("checkout", "-qb", "task-th", "devel-test-harness")  # devel-test-harness already has the ps tip
    commit("task-th")
    g("checkout", "-q", "main")

    bare = work.parent / "origin.git"
    _git(work.parent, env, "clone", "-q", "--bare", str(work), str(bare))
    prs = {
        2: ("devel-cfb", "task"),
        3: ("devel-cfb", "stale-old"),
        4: ("devel-cfb", "partial"),
        5: ("devel-cfb", "devel-test-harness-spoof"),
        6: ("devel-cfb", "sync-current"),
        7: ("devel-cfb", "sync-plus-task"),
        8: ("devel-test-harness", "task-th"),
    }
    for n, (base, head) in prs.items():  # GitHub's refs/pull/N/merge: the base merged with the head
        g("checkout", "-q", "--detach", base)
        merge(head)
        g("push", "-q", str(bare), f"HEAD:refs/pull/{n}/merge")
    g("checkout", "-q", "main")
    return bare.as_uri()


STUBS = {
    "shallow": 'if [ "$1" = rev-parse ] && [ "$2" = --is-shallow-repository ]; then echo true; exit 0; fi',
    "fetchfail": 'if [ "$1" = fetch ]; then exit 1; fi',
    "mb128": 'if [ "$1" = merge-base ]; then exit 128; fi',
}


def run(tmp_path: Path, env: dict, origin: str, pr: int, base_ref: str, stub: str | None = None):
    """(allowance JSON or None, stdout) of the script on PR ``pr``'s merge ref in a depth-1 clone."""
    ck = tmp_path / "ck"
    _git(tmp_path, env, "clone", "-q", "--depth", "1", "--no-tags", origin, str(ck))
    _git(ck, env, "fetch", "-q", "--depth", "1", "origin", f"+refs/pull/{pr}/merge:refs/remotes/pull/{pr}/merge")
    _git(ck, env, "checkout", "-q", "--detach", f"refs/remotes/pull/{pr}/merge")
    out, summary = tmp_path / "out.json", tmp_path / "summary.md"
    run_env = {**env, "BASE_REF": base_ref, "GITHUB_STEP_SUMMARY": str(summary), "STUB": ""}
    if stub:
        real = subprocess.run([BASH, "-c", "command -v git"], env=env, capture_output=True, text=True).stdout.strip()
        stub_dir = tmp_path / "stub"
        stub_dir.mkdir()
        (stub_dir / "git").write_text(f'#!/usr/bin/env bash\n{STUBS[stub]}\nexec "{real}" "$@"\n', encoding="utf-8")
        (stub_dir / "git").chmod(0o755)
        run_env["STUB"] = str(stub_dir)
    cmd = 'if [ -n "$STUB" ]; then PATH="$(cygpath -u "$STUB" 2>/dev/null || printf %s "$STUB"):$PATH"; fi; exec bash "$@"'
    proc = subprocess.run(
        [BASH, "-c", cmd, "bash", SCRIPT.as_posix(), "scoreboard-devel.json", out.as_posix()],
        cwd=ck,
        env=run_env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    if summary.exists():  # the decision is always written to the step summary
        assert "Upstream baseline:" in summary.read_text(encoding="utf-8")
    return (out.read_text(encoding="utf-8") if out.exists() else None), proc.stdout


def test_a_sync_of_the_current_upstream_tip_gets_the_upstream_baseline(tmp_path, env, origin):
    allowance, stdout = run(tmp_path, env, origin, 6, "devel-cfb")
    assert allowance == '{"who": "th-3"}\n'  # the upstream TIP's file
    assert "::warning::" not in stdout


@pytest.mark.parametrize(
    ("pr", "base_ref", "why"),
    [
        (2, "devel-cfb", "does not contain the devel-test-harness tip"),  # task PR
        (3, "devel-cfb", "does not contain the devel-test-harness tip"),  # only an OLD upstream commit
        (4, "devel-cfb", "does not contain the devel-test-harness tip"),  # partial sync, not the tip
        (5, "devel-cfb", "does not contain the devel-test-harness tip"),  # head branch named like the upstream
        (8, "devel-test-harness", "already contains the devel-program-setup tip"),  # task PR on a synced base
        (6, "devel-program-setup", "upstream of devel-program-setup is main"),
        (6, "main", "has no upstream branch"),
        (6, "", "not a pull request"),  # push / workflow_dispatch
    ],
)
def test_no_allowance_without_a_sync(tmp_path, env, origin, pr, base_ref, why):
    allowance, stdout = run(tmp_path, env, origin, pr, base_ref)
    assert allowance is None
    assert why in stdout
    assert "::warning::" not in stdout


def test_a_sync_that_also_carries_task_commits_is_still_a_sync(tmp_path, env, origin):
    """Documented design (quality.yml header): the allowance covers the whole PR."""
    allowance, _ = run(tmp_path, env, origin, 7, "devel-cfb")
    assert allowance is not None


@pytest.mark.parametrize(
    ("base_ref", "stub", "why"),
    [
        ("devel-cfb", "shallow", "history is still shallow"),
        ("devel-cfb", "fetchfail", "git fetch of devel-cfb and devel-test-harness failed"),
        ("devel-cfb", "mb128", "exited 128 (head) / 128 (base)"),
        ("devel-nonexistent", None, "git fetch of devel-nonexistent and devel-test-harness failed"),
        ("devel-$(touch PWNED)", None, "failed"),  # a spoofed base_ref is never executed
    ],
)
def test_every_error_adds_no_allowance_and_warns(tmp_path, env, origin, base_ref, stub, why):
    allowance, stdout = run(tmp_path, env, origin, 6, base_ref, stub)
    assert allowance is None
    assert "::warning::upstream baseline check failed" in stdout
    assert why in stdout
    assert not list(tmp_path.rglob("PWNED"))
