#!/usr/bin/env bash
# Extra scoreboard baseline for a SYNC pull request (.github/workflows/quality.yml).
#
#   BASE_REF=<pull request base> tools/upstream_baseline.sh <file under baseline/> <out path>
#
# A sync PR merges the upstream branch into its topic branch (main -> devel-program-setup ->
# devel-test-harness -> devel-<topic>). Its metrics are the upstream's, which the upstream already
# accepted and re-seeded, so tools/quality_scoreboard.py may compare them to the upstream's baseline
# too: a metric fails only when it is worse than every baseline it is given. That allowance is
# written to <out path> only when the PR head contains the upstream tip AND the base branch does
# not yet: an ordinary task PR (whose head carries no upstream commit the base lacks) gets none.
# Any git error, or a history still shallow after the fetch, means no allowance plus a warning:
# the check may only ever fail closed.
#
# Upstream map: devel-test-harness <- devel-program-setup; any other devel-<topic> <-
# devel-test-harness; devel-program-setup <- main, which keeps no baseline/ (so no allowance: a
# main sync into devel-program-setup is still waived and re-seeded by hand).
set -uo pipefail
file="$1"
out="$2"
base="${BASE_REF:-}"
rm -f "$out"

say() {
  echo "$1"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then echo "$1" >> "$GITHUB_STEP_SUMMARY"; fi
}
none() {
  say "Upstream baseline: none ($1)."
  exit 0
}
warn_none() {
  echo "::warning::upstream baseline check failed ($1); no upstream allowance added"
  none "$1"
}

case "$base" in
  "") none "not a pull request" ;;
  devel-program-setup) none "upstream of $base is main, which keeps no baseline/" ;;
  devel-test-harness) upstream=devel-program-setup ;;
  devel-*) upstream=devel-test-harness ;;
  *) none "base $base has no upstream branch" ;;
esac

# Ancestry needs full history: in a shallow clone a commit beyond the cut-off reads as "not an
# ancestor", and that false "the base lacks the upstream tip" would fail OPEN.
unshallow=()
if [ "$(git rev-parse --is-shallow-repository)" = true ]; then unshallow=(--unshallow); fi
git fetch --no-tags --quiet "${unshallow[@]}" origin \
  "+refs/heads/$base:refs/remotes/origin/$base" "+refs/heads/$upstream:refs/remotes/origin/$upstream" \
  || warn_none "git fetch of $base and $upstream failed"
[ "$(git rev-parse --is-shallow-repository)" = false ] || warn_none "history is still shallow"

git merge-base --is-ancestor "origin/$upstream" HEAD
in_head=$?
git merge-base --is-ancestor "origin/$upstream" "origin/$base"
in_base=$?
tip="$(git rev-parse --short "origin/$upstream")"
case "$in_head:$in_base" in
  0:1) ;;
  1:?) none "this PR does not contain the $upstream tip $tip, so it is not a sync" ;;
  0:0) none "$base already contains the $upstream tip $tip, so this PR brings nothing new from it" ;;
  *) warn_none "git merge-base --is-ancestor exited $in_head (head) / $in_base (base)" ;;
esac

git show "origin/$upstream:baseline/$file" > "$out" 2>/dev/null \
  || { rm -f "$out"; none "origin/$upstream has no baseline/$file"; }
say "Upstream baseline: baseline/$file at $upstream $tip (this PR contains the $upstream tip and $base does not yet, so a metric may match either baseline)."
