export const meta = {
  name: 'validation-harness',
  description:
    'Tier-2 deep validation: run the Tier-1 deterministic checks over the registered datasets + lint targets, route each needs_judgment WARN finding to the matching sdv-toolkit judgment reviewer, and consolidate a report of ERRORs + confirmed verdicts.',
  phases: [
    { title: 'Collect', detail: 'a runner agent shells the Tier-1 CLI for Finding[] per dataset/target' },
    { title: 'Judge', detail: 'route each needs_judgment finding to its reviewer subagent for a Verdict' },
  ],
}

// --- configuration -----------------------------------------------------------
// Registered datasets (tools/validation/registry.py DATASETS) and lint targets
// (LINT_TARGETS). Keep in sync with the registry.
const DATASETS = ['cfb_model_pbp', 'nfl_model_pbp']
const LINT_TARGETS = ['nfl_native_pbp', 'sdv_nfl_ep_wp']

// finding.check -> the sdv-toolkit judgment agent that adjudicates it.
// Every check that emits needs_judgment=True has exactly one consumer here.
const CHECK_TO_AGENT = {
  extraction: 'sdv-toolkit:extraction-semantics-reviewer',
  sweep: 'sdv-toolkit:anomaly-triage-reviewer',
  numeric_parity: 'sdv-toolkit:parity-divergence-reviewer',
  leakage_lint: 'sdv-toolkit:leakage-reviewer',
  boundary_leakage: 'sdv-toolkit:leakage-reviewer',
}

// The runner agent must export the data roots so the CLI can resolve the
// env-var-rooted dataset globs / lint targets (cfb is User-scope; nfl is passed
// here). A dataset whose parquet is absent should be reported, not crash the run.
const SDV_PY = 'c:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-py'
const NFL_DATA_ROOT = 'C:/Users/saiem/Documents/GitHub-Data/sdv-dev/nflverse-dev/nfl-data'

// JSON Schemas (Finding[] from the CLI; Verdict from the reviewers) ------------
const FINDINGS_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    findings: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: true,
        properties: {
          check: { type: 'string' },
          severity: { type: 'string', enum: ['error', 'warn', 'info'] },
          domain: { type: 'string' },
          dataset: { type: 'string' },
          message: { type: 'string' },
          locator: { type: 'object', additionalProperties: true },
          metric: { type: ['number', 'null'] },
          needs_judgment: { type: 'boolean' },
          sample: { type: ['array', 'null'], items: { type: 'object', additionalProperties: true } },
        },
        required: ['check', 'severity', 'message', 'needs_judgment'],
      },
    },
    run_error: { type: ['string', 'null'], description: 'a CLI/resolve failure (e.g. data absent), or null' },
  },
  required: ['findings'],
}

const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    finding_ref: { type: 'string' },
    status: { type: 'string', enum: ['confirmed', 'dismissed', 'uncertain'] },
    confidence: { type: 'number' },
    rationale: { type: 'string' },
    suggested_fix: { type: ['string', 'null'] },
  },
  required: ['finding_ref', 'status', 'confidence', 'rationale'],
}

// --- helpers -----------------------------------------------------------------
function runnerPrompt(kind, name) {
  const cmd =
    kind === 'dataset'
      ? `uv run --frozen python -m tools.validation.cli run --dataset ${name} --json`
      : `uv run --frozen python -m tools.validation.cli lint --target ${name} --json`
  return [
    `You are a deterministic runner. Work from \`${SDV_PY}\`. Export the data roots so the`,
    `env-var-rooted globs resolve: set \`SDV_VALIDATION_NFL_DATA_ROOT="${NFL_DATA_ROOT}"\` (and leave`,
    '`SDV_VALIDATION_DATA_ROOT` as already configured at User scope). Then run EXACTLY this and',
    'capture its stdout:',
    '',
    `    ${cmd}`,
    '',
    'The command prints a JSON array of finding dicts. Return it as {"findings": [...]}.',
    'If the command FAILS (non-zero exit, FileNotFoundError on a missing parquet glob, etc.),',
    'return {"findings": [], "run_error": "<the salient error line>"} — do NOT crash the workflow.',
    'Do not add, drop, or reinterpret findings; pass the CLI output through verbatim.',
  ].join('\n')
}

function verdictPrompt(finding) {
  return [
    'Triage this single sdv-py validation finding and emit a Verdict (your agent instructions',
    'define the judgment + the exact Verdict JSON shape). The finding:',
    '',
    JSON.stringify(finding, null, 2),
  ].join('\n')
}

// --- stage 1: collect deterministic findings ---------------------------------
phase('Collect')
const datasetRuns = await parallel(
  DATASETS.map((d) => () => agent(runnerPrompt('dataset', d), { label: `run:${d}`, phase: 'Collect', schema: FINDINGS_SCHEMA })),
)
const lintRuns = await parallel(
  LINT_TARGETS.map((t) => () => agent(runnerPrompt('lint', t), { label: `lint:${t}`, phase: 'Collect', schema: FINDINGS_SCHEMA })),
)

const collected = [...datasetRuns, ...lintRuns].filter(Boolean)
const runErrors = collected.filter((r) => r.run_error).map((r) => r.run_error)
const allFindings = collected.flatMap((r) => r.findings || [])
const errors = allFindings.filter((f) => f.severity === 'error')
const toJudge = allFindings.filter((f) => f.needs_judgment === true)
log(`collected ${allFindings.length} findings (${errors.length} ERROR, ${toJudge.length} needs_judgment); ${runErrors.length} run error(s)`)

// --- stage 2: route needs_judgment findings to the reviewers -----------------
phase('Judge')
const verdicts = await parallel(
  toJudge.map((f) => () => {
    const agentType = CHECK_TO_AGENT[f.check]
    if (!agentType) {
      log(`no reviewer registered for check '${f.check}' — leaving unadjudicated`)
      return null
    }
    return agent(verdictPrompt(f), { agentType, schema: VERDICT_SCHEMA, label: `judge:${f.check}`, phase: 'Judge' }).then((v) => ({
      finding: f,
      verdict: v,
    }))
  }),
)

const adjudicated = verdicts.filter(Boolean)
const confirmed = adjudicated.filter((a) => a.verdict && a.verdict.status === 'confirmed')

// --- consolidated report -----------------------------------------------------
return {
  datasets: DATASETS,
  lint_targets: LINT_TARGETS,
  run_errors: runErrors,
  total_findings: allFindings.length,
  errors,
  needs_judgment: toJudge.length,
  adjudicated: adjudicated.length,
  confirmed: confirmed.map((a) => ({ finding: a.finding, verdict: a.verdict })),
  // exit-worthy = any deterministic ERROR, or any reviewer-CONFIRMED WARN
  actionable: errors.length + confirmed.length,
}
