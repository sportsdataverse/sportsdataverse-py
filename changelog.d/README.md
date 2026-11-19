<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Changelog fragments](#changelog-fragments)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Changelog fragments

Each change merged since the last release is a file here, not an edit to `CHANGELOG.md`.
Every PR adds its own file, so two open PRs never conflict over the changelog.

Name a fragment `<slug>.<group>.md`:

- `<slug>` is lowercase letters, digits, `.`, `_` and `-` only. To use a branch name, lowercase it
  and turn `/` and any other character into `-`: branch `Fix/ESPN-Rankings` gives
  `fix-espn-rankings.fixed.md`.
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

`python tools/hooks/sync_docs_changelog.py --check` rejects a misnamed fragment, one that is not a bullet
list, or a bullet off the style above; it runs as a pre-commit hook and in CI's lint job.
The docs site's [Unreleased](https://py.sportsdataverse.org/changelog-unreleased) page is built from
these files on every deploy. At release time `uv run python tools/release_changelog.py <x.y.z>`
writes them into `CHANGELOG.md` as the new release section (each group's bullets sorted) and
deletes them; see `CONTRIBUTING.md`, "At release time".
