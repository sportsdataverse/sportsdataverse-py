# Changelog fragments

Each change merged since the last release is a file here, not an edit to `CHANGELOG.md`.
Every PR adds its own file, so two open PRs never conflict over the changelog.

Name a fragment `<slug>.<group>.md`:

- `<slug>` is lowercase letters, digits, `.`, `_` and `-` only. A branch name works once its `/`
  becomes `-`: branch `fix/espn-rankings` gives `fix-espn-rankings.fixed.md`.
- `<group>` is one of `breaking`, `added`, `changed`, `deprecated`, `removed`, `fixed`,
  `security`, `data`. They become the release's `### Breaking changes`, `Added`, `Changed`,
  `Deprecated`, `Removed`, `Fixed`, `Security` and `Data` groups, in that order.

The file holds one bullet per change, written as it will appear in the changelog: at most three
lines, continuation lines indented two spaces, and the PR number at the end. A change that belongs
in two groups is two files. For example, `changelog.d/hockeytech-schedules.fixed.md`:

```markdown
- **HockeyTech:** `<league>_schedule(season=...)` returns that season's games; it returned
  the feed's oldest 10,000. (#727)
```

`tests/test_sync_docs_changelog.py` rejects a misnamed fragment or one that is not a bullet list.
The docs site's [Unreleased](https://py.sportsdataverse.org/changelog-unreleased) page is built from
these files on every deploy. At release time `uv run python tools/release_changelog.py <x.y.z>`
writes them into `CHANGELOG.md` as the new release section (each group's bullets sorted) and
deletes them; see `CONTRIBUTING.md`, "At release time".
