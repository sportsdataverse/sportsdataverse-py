---
name: address-bot-reviews
description: Use to triage and resolve automated code-review comments that land on a sportsdataverse-py (sdv-py) PR from CodeRabbit or GitHub Copilot — fetch the unresolved review threads, fix the valid ones, reply to (and decline, with rationale) the rest, then resolve each thread. Invoke for "address the coderabbit comments", "resolve the bot reviews", "handle the copilot review", or as the post-CI step in /ship before merging.
---

# Address CodeRabbit / Copilot reviews on a PR

CodeRabbit (login `coderabbitai` / `coderabbitai[bot]`) and Copilot
(`copilot-pull-request-reviewer[bot]`) post reviews a few minutes after a push /
CI run. This skill fetches the **unresolved** threads, addresses each, and
resolves it. Run it after the PR is open and CI is green, before merge.

**Evaluate — do not auto-apply.** Both bots produce false positives, especially
against this repo's *documented* choices. A suggestion that contradicts CLAUDE.md
is a decline-with-citation, not a fix (see Guardrails).

## Steps

1. **Resolve the PR + repo coordinates.**

   ```sh
   gh pr view --json number,url,headRefName
   gh repo view --json owner,name -q '.owner.login+"/"+.name'   # sportsdataverse/sportsdataverse-py
   ```

2. **List unresolved bot review threads** (verified query — returns thread id +
   the comment's `databaseId` for replies):

   ```sh
   gh api graphql -f query='
   query($owner:String!,$repo:String!,$pr:Int!){
     repository(owner:$owner,name:$repo){
       pullRequest(number:$pr){
         reviewThreads(first:100){ nodes{
           id isResolved isOutdated
           comments(first:10){ nodes{ databaseId author{login} body path line } }
         }}
       }
     }
   }' -F owner=sportsdataverse -F repo=sportsdataverse-py -F pr=<N> \
     --jq '.data.repository.pullRequest.reviewThreads.nodes
           | map(select(.isResolved==false and (.comments.nodes[0].author.login|test("(?i)coderabbit|copilot"))))'
   ```

   For a PR with >100 threads, paginate: add `pageInfo{ hasNextPage endCursor }`
   to `reviewThreads` and loop with `after: <endCursor>` until `hasNextPage` is
   false (a hard-coded `first:100` silently drops the tail).

   Also read the PR-level summary review (CodeRabbit's walkthrough / Copilot's
   overview), which often holds suggestions not attached to a line:

   ```sh
   gh pr view <N> --json reviews \
     --jq '.reviews[] | select(.author.login|test("(?i)coderabbit|copilot")) | {by:.author.login,state,body}'
   ```

3. **Triage each thread comment** into one of:
   - **Valid** → fix in code.
   - **Convention conflict / false positive** → do NOT change; reply citing the
     CLAUDE.md rule (see Guardrails).
   - **Question** → answer in a reply.
   - **Nit** → judgment call; apply if cheap and correct.

4. **Apply the valid fixes**, grouped by file, then run `/preflight` (ruff +
   mypy ratchet + targeted tests) so a fix doesn't introduce a regression.

5. **Commit + push.** Conventional-Commit subject, **no AI co-author trailer**
   (the `commit-msg` hook enforces both). Pushing flips the addressed threads to
   `isOutdated`.

   ```sh
   git commit -m "fix(<scope>): address review feedback on <area>"
   git push
   ```

6. **Reply to, then resolve, each thread.** Reply first; resolve only after the
   fix is pushed or the decline is justified.

   - Reply (github MCP `add_reply_to_pull_request_comment`, or REST):

     ```sh
     gh api -X POST repos/sportsdataverse/sportsdataverse-py/pulls/<N>/comments \
       -F in_reply_to=<databaseId> -f body='Fixed in <sha>.'      # or the decline rationale
     ```

   - Resolve the thread (GraphQL mutation, verified shape):

     ```sh
     gh api graphql -f query='mutation($id:ID!){ resolveReviewThread(input:{threadId:$id}){ thread{ isResolved } } }' -F id=<threadNodeId>
     ```

   - **CodeRabbit shortcuts** (post as a top-level PR comment):
     `@coderabbitai resolve` resolves all of its own comments at once;
     `@coderabbitai review` triggers a re-review after you push fixes. Use these
     for CodeRabbit-wide actions; use the per-thread GraphQL mutation for Copilot
     or for selective resolution.

7. **Re-request review** if the changes were substantial: `@coderabbitai review`
   (comment) for CodeRabbit, or `request_copilot_review` (github MCP) for Copilot.

8. **Report** a table: `thread | by | path:line | verdict (fixed/declined/answered) | resolved?`.
   Leave genuinely contentious threads unresolved and flag them for a human call.

## Guardrails — decline suggestions that fight CLAUDE.md

When a bot suggestion contradicts a documented repo convention, **decline with a
one-line citation** instead of "fixing" into a regression. Common false positives
here:

- `pl.col(...) == True` / `== False` — intentional for polars boolean masks
  (E712 is suppressed in `pyproject.toml`); do not rewrite to `pl.col(...)` / `~`.
- Polars **1.x** idioms — don't accept reverts to 0.18 API
  (`group_by`/`with_row_index`/`pl.len()`/`how="full"`, etc.).
- Regex with the inline case toggle `(?i)...(?-i:...)` — polars/Rust has **no
  lookaround**; a "use a lookbehind" suggestion is wrong.
- ID-dtype discipline — don't accept an `id -> Utf8` "paper-over" cast.
- Packaging — no `setup.py` / `requirements*.txt` (PEP 621 + uv only).
- Don't resolve a thread you didn't actually address; don't add an AI
  co-author trailer to fix commits (hook-enforced anyway).
