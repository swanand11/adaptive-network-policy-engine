import { useState } from 'react'
import { Approval } from '../types'
import { AlertTriangle } from 'lucide-react'

interface Props {
  approvals: Approval[]
  onApprove: (id: string) => void
  onReject: (id: string) => void
  loading?: boolean
}

export function GovernanceQueuePage({ approvals, onApprove, onReject, loading }: Props) {
  const [selected, setSelected] = useState<Approval | null>(null)

  if (loading) return <div className="glass-panel">Loading governance queue...</div>

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-outfit font-bold">Governance Queue</h2>
      {approvals.length === 0 && <div className="glass-panel text-text-muted">No high-risk actions pending.</div>}
      <div className="grid grid-cols-1 gap-4">
        {approvals.map((a) => (
          <div key={a.id} className="glass-panel border border-bad shadow-[0_0_18px_rgba(255,51,102,0.25)] animate-pulse-slow">
            <div className="flex items-start justify-between">
              <div>
                <div className="flex items-center gap-2 text-bad font-bold"><AlertTriangle className="w-4 h-4" />Human Approval Required</div>
                <h3 className="text-lg font-bold mt-2">{a.decision?.service || a.action_type || 'Unknown service'}</h3>
                <p className="text-sm text-text-muted">{a.decision?.decision || a.reasoning || 'No reasoning provided'}</p>
              </div>
              <div className="text-right text-sm">
                <div className="text-bad font-bold">Risk {(Number(a.risk_score || 0) * 100).toFixed(1)}%</div>
                <div className="text-text-muted">{a.decision?.risk_level || a.severity || 'HIGH'}</div>
              </div>
            </div>
            <div className="mt-4 flex gap-2">
              <button className="btn-primary" onClick={() => onApprove(a.id)}>Approve</button>
              <button className="btn-danger" onClick={() => onReject(a.id)}>Reject</button>
              <button className="px-3 py-2 rounded border border-accent-primary text-accent-primary" onClick={() => setSelected(a)}>View Details</button>
            </div>
          </div>
        ))}
      </div>

      {selected && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50">
          <div className="bg-panel border border-border rounded-xl p-6 w-[min(900px,92vw)]">
            <h3 className="text-xl font-bold mb-4">Governance Action Details</h3>
            <pre className="text-xs bg-black/30 p-3 rounded overflow-auto max-h-[60vh]">{JSON.stringify(selected, null, 2)}</pre>
            <div className="mt-4 text-right">
              <button className="px-4 py-2 rounded bg-accent-secondary" onClick={() => setSelected(null)}>Close</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
