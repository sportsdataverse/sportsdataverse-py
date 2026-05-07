## Summary

<!--
Brief explanation of WHAT this PR changes and WHY. Aim for 1-3 sentences in
plain English. Avoid implementation details — those live in the diff.
-->

## Changes

<!-- Bullet list of the substantive changes. -->

-

## Type of change

- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change (existing API surface changes)
- [ ] Documentation only
- [ ] Test only
- [ ] Refactor (no behavior change)
- [ ] Tooling / CI / packaging

## Test plan

<!--
How did you verify this works? Bullet list of commands run / scenarios
exercised. Live tests should be gated by SDV_PY_LIVE_TESTS=1; offline
tests should pass without it.
-->

- [ ] `uv run pytest tests/<scope>/` passes offline
- [ ] `SDV_PY_LIVE_TESTS=1 uv run pytest tests/<scope>/` passes (if data-touching)
- [ ] `uv run mypy sportsdataverse/<modules>` passes for newly-typed modules
- [ ] `uv run ruff check sportsdataverse/<modules>` passes

## Breaking changes

<!-- If "Breaking change" is checked above, describe the migration path. Otherwise: "None." -->

## Documentation

- [ ] CHANGELOG.md entry added
- [ ] Docstring(s) updated
- [ ] CLAUDE.md / copilot-instructions.md updated (only if a convention or pattern changes)

## Checklist

- [ ] My code follows the project's [code standards](../CONTRIBUTING.md#code-standards-for-new-modules).
- [ ] I have NOT included AI agents (Claude, Copilot, GPT, etc.) as commit co-authors.
- [ ] I have searched existing PRs to confirm this isn't a duplicate.
