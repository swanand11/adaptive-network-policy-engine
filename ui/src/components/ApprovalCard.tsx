import { Approval } from '../types'
import { AlertCircle, CheckCircle, XCircle } from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'

interface ApprovalCardProps {
  approval: Approval
  onApprove: (id: string) => void
  onReject: (id: string) => void
  isLoading?: boolean
}

export function ApprovalCard({ approval, onApprove, onReject, isLoading }: ApprovalCardProps) {
  const getRiskColor = (risk: string) => {
    switch (risk) {
      case 'low':
        return 'text-good'
      case 'medium':
        return 'text-warn'
      case 'high':
      case 'critical':
        return 'text-bad'
      default:
        return 'text-text-muted'
    }
  }

  const getRiskBgColor = (risk: string) => {
    switch (risk) {
      case 'low':
        return 'bg-good/10'
      case 'medium':
        return 'bg-warn/10'
      case 'high':
      case 'critical':
        return 'bg-bad/10'
      default:
        return 'bg-text-muted/10'
    }
  }

  const getRiskIcon = (risk: string) => {
    switch (risk) {
      case 'low':
        return <CheckCircle className="w-5 h-5" />
      case 'medium':
        return <AlertCircle className="w-5 h-5" />
      case 'high':
      case 'critical':
        return <XCircle className="w-5 h-5" />
      default:
        return <AlertCircle className="w-5 h-5" />
    }
  }

  return (
    <div className="glass-panel">
      <div className="flex items-start justify-between mb-4">
        <div className="flex-1">
          <h3 className="text-lg font-outfit font-bold mb-2">
            {approval.decision.service}
          </h3>
          <p className="text-text-muted text-sm mb-3">
            {approval.decision.decision}
          </p>
        </div>
        <div className={`flex items-center gap-2 px-3 py-1 rounded-lg ${getRiskBgColor(approval.decision.risk_level)} ${getRiskColor(approval.decision.risk_level)}`}>
          {getRiskIcon(approval.decision.risk_level)}
          <span className="text-xs font-outfit font-bold uppercase">
            {approval.decision.risk_level}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4 mb-4 pb-4 border-b border-border">
        <div>
          <p className="text-xs text-text-muted uppercase tracking-wider mb-1">Risk Score</p>
          <p className="text-lg font-outfit font-bold">{(approval.risk_score * 100).toFixed(1)}%</p>
        </div>
        <div>
          <p className="text-xs text-text-muted uppercase tracking-wider mb-1">Created</p>
          <p className="text-sm">{formatDistanceToNow(new Date(approval.created_at || approval.timestamp || Date.now()), { addSuffix: true })}</p>
        </div>
      </div>

      {approval.decision?.metadata && Object.keys(approval.decision.metadata).length > 0 && (
        <div className="mb-4 pb-4 border-b border-border">
          <p className="text-xs text-text-muted uppercase tracking-wider mb-2">Metadata</p>
          <div className="bg-black/20 rounded p-2 text-xs font-mono text-accent-primary overflow-auto max-h-32">
            {JSON.stringify(approval.decision.metadata, null, 2)}
          </div>
        </div>
      )}

      <div className="flex gap-3">
        <button
          onClick={() => onApprove(approval.id)}
          disabled={isLoading}
          className="flex-1 btn-primary disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Approve
        </button>
        <button
          onClick={() => onReject(approval.id)}
          disabled={isLoading}
          className="flex-1 btn-danger disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Reject
        </button>
      </div>
    </div>
  )
}
