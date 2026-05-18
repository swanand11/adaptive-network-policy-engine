interface Props {
  loading?: boolean
  weights: Record<string, number>
  metrics: { pending: number; executed: number; ingestion: string }
  cspMetrics: Array<{
    cloud: string
    count: number
    avg_risk: number
    latency_ms?: number
    error_rate_percent?: number
    cpu_usage_percent?: number
    memory_usage_percent?: number
    series?: Record<string, number[]>
  }>
}

export function WeightsPage({ loading, weights, metrics, cspMetrics }: Props) {
  if (loading) {
    return <div className="glass-panel">Loading weights...</div>
  }

  const entries = Object.entries(weights || {})

  return (
    <div className="space-y-5">
      <h2 className="text-2xl font-outfit font-bold">Weights</h2>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="glass-panel">
          <p className="text-xs text-text-muted uppercase tracking-wider">Pending Approvals</p>
          <p className="text-3xl font-bold text-warn">{metrics.pending}</p>
        </div>
        <div className="glass-panel">
          <p className="text-xs text-text-muted uppercase tracking-wider">Executed Actions</p>
          <p className="text-3xl font-bold text-good">{metrics.executed}</p>
        </div>
        <div className="glass-panel">
          <p className="text-xs text-text-muted uppercase tracking-wider">Pipeline Health</p>
          <p className={`text-xl font-bold ${metrics.ingestion === 'healthy' ? 'text-good' : 'text-warn'}`}>
            {metrics.ingestion}
          </p>
        </div>
      </div>

      <div className="glass-panel">
        <h3 className="text-lg font-bold mb-4">Current Distribution</h3>
        {entries.length === 0 ? (
          <p className="text-text-muted">No weight data yet from policy.approved metadata.</p>
        ) : (
          <div className="space-y-3">
            {entries.map(([name, value]) => (
              <div key={name}>
                <div className="flex justify-between text-sm mb-1">
                  <span className="uppercase font-bold">{name}</span>
                  <span className="font-mono">{value}%</span>
                </div>
                <div className="w-full h-2 rounded bg-black/30 overflow-hidden">
                  <div className="h-full bg-gradient-to-r from-accent-primary to-accent-secondary" style={{ width: `${value}%` }} />
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="glass-panel">
        <h3 className="text-lg font-bold mb-4">Live Metrics Per CSP</h3>
        {cspMetrics.length === 0 ? (
          <p className="text-text-muted">No CSP metrics received yet from metrics pipeline.</p>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            {cspMetrics.map((csp) => (
              <div key={csp.cloud} className="bg-black/20 border border-border/60 rounded-lg p-4">
                <div className="flex items-center justify-between mb-4">
                  <p className="uppercase font-bold">{csp.cloud}</p>
                  <p className="text-xs text-text-muted">Prometheus</p>
                </div>
                <div className="grid grid-cols-2 gap-3 text-sm">
                  <Metric label="Latency" value={`${Number(csp.latency_ms || 0).toFixed(2)} ms`} />
                  <Metric label="Error Rate" value={`${Number(csp.error_rate_percent || 0).toFixed(2)}%`} danger={Number(csp.error_rate_percent || 0) > 5} />
                  <Metric label="CPU" value={`${Number(csp.cpu_usage_percent || 0).toFixed(1)}%`} />
                  <Metric label="Memory" value={`${Number(csp.memory_usage_percent || 0).toFixed(1)}%`} />
                </div>
                <div className="mt-3">
                  <p className="text-[11px] text-text-muted mb-1">Latency trend</p>
                  <MiniLine values={csp.series?.latency_ms || []} />
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function Metric({ label, value, danger }: { label: string; value: string; danger?: boolean }) {
  return (
    <div className="bg-black/25 border border-border/40 rounded-md p-2">
      <div className="text-[11px] text-text-muted uppercase tracking-wider">{label}</div>
      <div className={`font-mono font-bold ${danger ? 'text-bad' : 'text-white'}`}>{value}</div>
    </div>
  )
}

function MiniLine({ values }: { values: number[] }) {
  if (!values.length) return <div className="h-12 rounded bg-black/25" />
  const max = Math.max(...values, 1)
  const points = values.map((v, i) => {
    const x = values.length === 1 ? 100 : (i / (values.length - 1)) * 100
    const y = 44 - (v / max) * 38
    return `${x},${y}`
  }).join(' ')
  return (
    <svg viewBox="0 0 100 48" className="h-12 w-full rounded bg-black/25 overflow-visible" preserveAspectRatio="none">
      <polyline points={points} fill="none" stroke="rgb(0,240,255)" strokeWidth="2" vectorEffect="non-scaling-stroke" />
    </svg>
  )
}
