export type Phase = 'Pending' | 'Validating' | 'Setup' | 'Starting' | 'Running' | 'Aggregating' | 'Completed' | 'Failed'

export interface LoadTestRow {
  namespace: string
  name: string
  created: string | null
  plan: string | null
  planFile: string | null
  workers: number
  phase: Phase
  message: string | null
  result: 'Passed' | 'SLAFailed' | null
  workersSucceeded: number
  workersFailed: number
  startedAt: string | null
  startAt: string | null
  finishedAt: string | null
  runDir: string | null
  reportPath: string | null
  report: string | null
  metricsEnabled: boolean
}

export interface JobInfo {
  name: string
  role: string | null
  completions: number | null
  succeeded: number
  failed: number
  active: number
  start: string | null
  end: string | null
}

export interface PodInfo {
  name: string
  role: string | null
  worker: number | null
  node: string | null
  podIP: string | null
  phase: string | null
  ready: boolean
  restarts: number
  start: string | null
  end: string | null
  exitCode: number | null
  reason: string | null
}

export interface LoadTestDetail extends LoadTestRow {
  spec: Record<string, unknown>
  jobs: JobInfo[]
  pods: PodInfo[]
}

export interface LiveRequest { name: string; ok: number; ko: number; mean: number | null; p95: number | null }
export interface LiveTotals { ok: number; ko: number; usersActive: number; p95?: number | null; requests: LiveRequest[]; errors: { message: string; count: number }[] }
export interface LiveWorker extends LiveTotals { pod: string; worker: number | null; finished?: boolean }
export interface LiveMetrics {
  enabled: boolean
  time?: number
  workers: LiveWorker[]
  unreachable: { pod: string; error: string }[]
  total: LiveTotals | null
}

export interface Stats { count: number; ok: number; ko: number; min: number; mean: number; p50: number; p75: number; p95: number; p99: number; max: number }
export interface NamedStats extends Stats { name: string }
export interface RunSummary {
  start: string | null
  durationSeconds: number
  users: number
  total: Stats
  requestsPerSecond: number
  errorPercent: number
  requests: NamedStats[]
  transactions: NamedStats[]
  errors: { message: string; count: number }[]
  workers?: number
  worker?: number
  slaPassed?: boolean
}
export interface SummaryResponse { available: boolean; reason?: string; run?: RunSummary | null; workers?: RunSummary[] }

export interface RunFolder { folder: string; url: string; report: string | null; summary: RunSummary | null }

export interface DashboardConfig {
  namespaces: string[]
  resultsUrl: string
  plansUrl: string
  /** kubernetes: runs are LoadTests run by the operator; local: perf processes on the dashboard's machine */
  executor?: 'kubernetes' | 'local'
  storage?: 's3' | 'folder'
  localMaxWorkers?: number
  defaults: { image: string; s3Secret: string; s3Endpoint: string }
}

/** A folder of test files (plans, API catalogs, data) kept in S3 and edited from the dashboard. */
export interface Suite {
  name: string
  files: number
  size: number
  modified: string | null
}

export interface SuiteFile {
  path: string
  size: number
  modified: string | null
  text: boolean
  plan: boolean
}

export interface SuiteDetail {
  name: string
  url: string
  files: SuiteFile[]
}

export interface FileContent {
  path: string
  size: number
  content: string | null
  editable: boolean
}

/** Result of importing a LoadRunner VuGen script as a new suite. */
export interface LrImport {
  suite: string
  plan: string
  report: string | null
  requests: number | null
  notes: string[]
  files: string[]
  skipped: number
}
