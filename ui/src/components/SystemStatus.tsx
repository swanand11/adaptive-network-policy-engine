import { ProcessStatus } from '../types'
import { CheckCircle, AlertCircle, XCircle } from 'lucide-react'

interface SystemStatusProps {
  status: ProcessStatus[]
  loading?: boolean
}

export function SystemStatus({ status, loading }: SystemStatusProps) {
  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'healthy':
        return <CheckCircle className="w-5 h-5 text-good" />
      case 'degraded':
        return <AlertCircle className="w-5 h-5 text-warn" />
      case 'failed':
      case 'unknown':
        return <XCircle className="w-5 h-5 text-bad" />
      default:
        return <AlertCircle className="w-5 h-5 text-text-muted" />
    }
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'healthy':
        return 'text-good'
      case 'degraded':
        return 'text-warn'
      case 'failed':
      case 'unknown':
        return 'text-bad'
      default:
        return 'text-text-muted'
    }
  }

  if (loading) {
    return (
      <div className="glass-panel">
        <div className="animate-pulse space-y-3">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="h-12 bg-border rounded"></div>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="glass-panel">
      <h2 className="text-xl font-outfit font-bold mb-4">System Status</h2>
      <div className="space-y-2 max-h-96 overflow-y-auto">
        {status.map((service) => (
          <div
            key={service.name}
            className="flex items-center justify-between p-3 bg-black/20 rounded-lg border border-border/50 hover:border-border transition-colors"
          >
            <div className="flex items-center gap-3 flex-1">
              {getStatusIcon(service.status)}
              <div className="flex-1">
                <p className="font-outfit font-bold text-sm">{service.name}</p>
                <p className={`text-xs ${getStatusColor(service.status)} uppercase tracking-wider`}>
                  {service.status}
                </p>
              </div>
            </div>
            <div className="text-right text-xs text-text-muted">
              <p>PID: {service.pid || 'N/A'}</p>
              <p>CPU: {service.cpu_percent.toFixed(1)}% | MEM: {service.memory_mb.toFixed(1)}MB</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
