- **Codegen:** each `generate.py --check` gate renders into its own temp dir, so two concurrent checks no
  longer delete each other's files. (#761)
