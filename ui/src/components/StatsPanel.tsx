import { ApprovalStats } from '../types'
import { CheckCircle, XCircle, Clock, TrendingUp } from 'lucide-react'

interface StatsPanelProps {
  stats: ApprovalStats | null
  loading?: boolean
}

export function StatsPanel({ stats, loading }: StatsPanelProps) {
  if (loading || !stats) {
    return (
      <div className="glass-panel">
        <div className="animate-pulse space-y-4">
          <div className="h-4 bg-border rounded w-1/3"></div>
          <div className="grid grid-cols-4 gap-4">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-20 bg-border rounded"></div>
            ))}
          </div>
        </div>
      </div>
    )
  }

  const statItems = [
    {
      label: 'Pending',
      value: stats.pending_count,
      icon: Clock,
      color: 'text-warn',
      bgColor: 'bg-warn/10',
    },
    {
      label: 'Approved',
      value: stats.approved_count,
      icon: CheckCircle,
      color: 'text-good',
      bgColor: 'bg-good/10',
    },
    {
      label: 'Rejected',
      value: stats.rejected_count,
      icon: XCircle,
      color: 'text-bad',
      bgColor: 'bg-bad/10',
    },
    {
      label: 'Total Processed',
      value: stats.total_processed,
      icon: TrendingUp,
      color: 'text-accent-primary',
      bgColor: 'bg-accent-primary/10',
    },
  ]

  return (
    <div className="glass-panel">
      <h2 className="text-xl font-outfit font-bold mb-6">Approval Statistics</h2>
      <div className="grid grid-cols-4 gap-4">
        {statItems.map((item) => {
          const Icon = item.icon
          return (
            <div key={item.label} className={`${item.bgColor} rounded-lg p-4 border border-border`}>
              <div className="flex items-center gap-2 mb-2">
                <Icon className={`w-5 h-5 ${item.color}`} />
                <p className="text-xs text-text-muted uppercase tracking-wider">{item.label}</p>
              </div>
              <p className={`text-3xl font-outfit font-bold ${item.color}`}>
                {item.value}
              </p>
            </div>
          )
        })}
      </div>
    </div>
  )
}
