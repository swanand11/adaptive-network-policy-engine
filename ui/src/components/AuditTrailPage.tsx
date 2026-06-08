import { AuditEntry } from '../types'

interface Props {
  entries: AuditEntry[]
  search: string
  onSearch: (v: string) => void
  loading?: boolean
}

export function AuditTrailPage({ entries, search, onSearch, loading }: Props) {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-outfit font-bold">Logs</h2>
      <input
        className="w-full bg-panel border border-border rounded p-3"
        placeholder="Search logs by actor, action, entity, payload..."
        value={search}
        onChange={(e) => onSearch(e.target.value)}
      />
      <div className="glass-panel">
        {loading ? (
          <p>Loading audit entries...</p>
        ) : entries.length === 0 ? (
          <p className="text-text-muted">No audit entries.</p>
        ) : (
          <div className="space-y-2 max-h-[70vh] overflow-auto">
            {entries.map((e, idx) => (
              <div key={idx} className="bg-black/20 border border-border/60 rounded-lg p-4 hover:border-accent-primary/50 transition-colors">
                <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-2 mb-3">
                  <div className="flex items-center gap-2">
                    <span className={`px-2 py-1 rounded text-xs font-bold ${logTone(e.action)}`}>{e.action}</span>
                    <span className="text-sm text-text-muted">{e.actor}</span>
                  </div>
                  <span className="text-xs text-text-muted">{new Date(e.timestamp).toLocaleString()}</span>
                </div>
                <div className="text-sm mb-2">
                  <span className="text-text-muted">Entity:</span> <span className="font-mono">{e.entity_id}</span>
                </div>
                <div className="text-sm text-white/90 mb-3">{summarize(e.payload)}</div>
                <details>
                  <summary className="cursor-pointer text-xs text-accent-primary">Payload</summary>
                  <pre className="text-xs mt-2 bg-black/30 rounded p-3 overflow-auto max-h-64">{JSON.stringify(e.payload, null, 2)}</pre>
                </details>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function logTone(action: string) {
  if (action === 'APPROVED' || action === 'EXECUTED') return 'bg-good/10 text-good border border-good/30'
  if (action === 'REJECTED' || action === 'EXPIRED') return 'bg-bad/10 text-bad border border-bad/30'
  return 'bg-accent-primary/10 text-accent-primary border border-accent-primary/30'
}

function summarize(payload: Record<string, any>) {
  const details = payload?.details || payload
  const inner = details?.payload || details
  return inner?.decision || details?.action || inner?.action_type || 'System event recorded'
}
