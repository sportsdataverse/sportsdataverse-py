- **Docs build:** reference-page front matter quotes a `title` or `sidebar_label` only when plain YAML would misread
  it, so the ABA and TBF pages (labels with a colon and a space) build again; tests now check every page's front
  matter and every `_category_.json`. The scb `x-api-secret` note no longer claims to mirror the site's code. (#861)
