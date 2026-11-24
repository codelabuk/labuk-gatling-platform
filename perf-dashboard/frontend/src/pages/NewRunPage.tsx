import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'

import { api } from '../api'

/** "KEY=value" lines -> object; blank lines and lines without '=' are ignored. */
const parseEnv = (text: string) =>
  Object.fromEntries(text.split('\n').map((l) => l.trim()).filter((l) => l.includes('=')).map((l) => [l.slice(0, l.indexOf('=')).trim(), l.slice(l.indexOf('=') + 1).trim()]))
const parseList = (text: string) => text.split(/[,\n]/).map((s) => s.trim()).filter(Boolean)

export function NewRunPage() {
  const navigate = useNavigate()
  const qc = useQueryClient()
  const [params] = useSearchParams()   // ?suite=&file= from the Test plans page's Run button
  const cfg = useQuery({ queryKey: ['config'], queryFn: api.config })
  const [form, setForm] = useState({
    namespace: '', name: '', image: '', file: params.get('file') ?? '', suite: params.get('suite') ?? '',
    workers: '2', env: '', secrets: '', scenarios: '', cpu: '', memory: '1Gi', startDelaySeconds: '30', metrics: true,
    spread: 'soft', reportMemory: '',
  })
  const set = (k: keyof typeof form) => (e: { target: { value: string } }) => setForm({ ...form, [k]: e.target.value })
  const suites = useQuery({ queryKey: ['suites'], queryFn: api.suites })
  const suite = useQuery({ queryKey: ['suite', form.suite], queryFn: () => api.suite(form.suite), enabled: !!form.suite })
  const planFiles = (suite.data?.files ?? []).filter((f) => f.plan)

  const create = useMutation({
    mutationFn: () => api.create({
      namespace: form.namespace || cfg.data?.namespaces[0],
      name: form.name || undefined,
      image: form.image || undefined,
      workers: Number(form.workers),
      plan: { file: form.file, suite: form.suite },
      env: parseEnv(form.env),
      secretRefs: parseList(form.secrets),
      scenarios: parseList(form.scenarios),
      cpu: form.cpu || undefined,
      memory: form.memory || undefined,
      spread: form.spread,
      reportMemory: form.reportMemory || undefined,
      startDelaySeconds: Number(form.startDelaySeconds) || undefined,
      metrics: form.metrics,
    }),
    onSuccess: async (row) => {
      await qc.invalidateQueries({ queryKey: ['loadtests'] })
      navigate(`/runs/${row.namespace}/${row.name}`)
    },
  })

  const submit = (e: FormEvent) => { e.preventDefault(); create.mutate() }

  return (
    <section>
      <div className="page-head"><h1>New run</h1></div>
      <form className="card form" onSubmit={submit}>
        <fieldset>
          <legend>Test plan</legend>
          <>
              <div className="row3">
                <label>Suite<select required value={form.suite} onChange={(e) => setForm({ ...form, suite: e.target.value, file: '' })}>
                  <option value="">choose…</option>
                  {(suites.data ?? []).map((s) => <option key={s.name}>{s.name}</option>)}
                </select></label>
                <label>Plan<select required value={form.file} onChange={set('file')} disabled={!form.suite}>
                  <option value="">{form.suite && suite.data && !planFiles.length ? 'no plan in this suite' : 'choose…'}</option>
                  {planFiles.map((f) => <option key={f.path}>{f.path}</option>)}
                </select></label>
                <div className="form-note">
                  {suites.data?.length === 0 ? <>No saved plans yet - <Link to="/plans">create or upload a suite</Link>.</>
                    : <><Link to={form.suite ? `/plans/${form.suite}` : '/plans'}>Manage test plans</Link> · the run uses a copy taken at start</>}
                </div>
              </div>
              {suites.error && <div className="alert bad">{(suites.error as Error).message}</div>}
          </>
        </fieldset>

        <fieldset>
          <legend>Load</legend>
          <div className="row3">
            <label>Workers (pods)<input type="number" min={1} max={200} value={form.workers} onChange={set('workers')} /></label>
            <label>CPU per worker<input value={form.cpu} onChange={set('cpu')} placeholder="e.g. 1 (empty = no limit)" /></label>
            <label>Memory per worker<input value={form.memory} onChange={set('memory')} /></label>
          </div>
          <div className="row3">
            <label>Start delay (s)<input type="number" min={5} value={form.startDelaySeconds} onChange={set('startDelaySeconds')} /></label>
            <label>Scenarios (empty = all)<input value={form.scenarios} onChange={set('scenarios')} placeholder="checkout, browse" /></label>
            <label className="check"><input type="checkbox" checked={form.metrics} onChange={(e) => setForm({ ...form, metrics: e.target.checked })} /> Live metrics</label>
          </div>
          <div className="row3">
            <label>Spread workers over nodes<select value={form.spread} onChange={set('spread')}>
              <option value="soft">soft — one per node where possible (default)</option>
              <option value="hard">hard — at most one per node (extra workers wait)</option>
              <option value="none">none — Kubernetes decides</option>
            </select></label>
            <label>Report memory<input value={form.reportMemory} onChange={set('reportMemory')} placeholder="e.g. 4Gi (large runs; empty = no limit)" /></label>
          </div>
        </fieldset>

        <fieldset>
          <legend>Environment</legend>
          <label>Variables — one NAME=value per line (fill ${'{'}NAME{'}'} in the plan)<textarea rows={3} value={form.env} onChange={set('env')} placeholder="BASE_URL=https://sit.api.example.com" /></label>
          <label>Secrets (as environment variables)<input value={form.secrets} onChange={set('secrets')} placeholder="perf-secrets" /></label>
        </fieldset>

        <fieldset>
          <legend>Run</legend>
          <div className="row3">
            <label>Namespace<select value={form.namespace} onChange={set('namespace')}>
              {(cfg.data?.namespaces ?? []).map((n) => <option key={n}>{n}</option>)}
            </select></label>
            <label>Name (optional)<input value={form.name} onChange={set('name')} placeholder="from the plan file + time" /></label>
            <label>Runner image<input value={form.image} onChange={set('image')} placeholder={cfg.data?.defaults.image} /></label>
          </div>
          <p className="muted-text">Results go to {cfg.data?.resultsUrl ?? '…'}.</p>
        </fieldset>

        {create.error && <div className="alert bad">{(create.error as Error).message}</div>}
        <div className="actions"><button className="btn" disabled={create.isPending}>{create.isPending ? 'Starting…' : 'Start run'}</button></div>
      </form>
    </section>
  )
}
