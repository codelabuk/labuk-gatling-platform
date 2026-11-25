import { useQuery } from '@tanstack/react-query'
import { NavLink, Route, Routes } from 'react-router-dom'

import { api } from './api'

import { HistoryPage } from './pages/HistoryPage'
import { NewRunPage } from './pages/NewRunPage'
import { PlansPage } from './pages/PlansPage'
import { RunPage } from './pages/RunPage'
import { RunsPage } from './pages/RunsPage'

export function App() {
  const cfg = useQuery({ queryKey: ['config'], queryFn: api.config })
  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">perf<span>dashboard</span></div>
        <nav>
          <NavLink to="/" end>Runs</NavLink>
          <NavLink to="/new">New run</NavLink>
          <NavLink to="/plans">Test plans</NavLink>
          <NavLink to="/history">History</NavLink>
        </nav>
        <div className="sidebar-foot">{cfg.data?.executor === 'local' ? 'Gatling load tests on this machine' : 'Gatling load tests on Kubernetes'}</div>
      </aside>
      <main>
        <Routes>
          <Route path="/" element={<RunsPage />} />
          <Route path="/new" element={<NewRunPage />} />
          <Route path="/plans" element={<PlansPage />} />
          <Route path="/plans/:suite" element={<PlansPage />} />
          <Route path="/history" element={<HistoryPage />} />
          <Route path="/runs/:ns/:name" element={<RunPage />} />
          <Route path="*" element={<p>Not found.</p>} />
        </Routes>
      </main>
    </div>
  )
}
