export interface Approval {
  id: string
  decision_id?: string
  requested_by?: string
  action_type?: string
  cloud_provider?: string
  target_resource?: string
  severity?: string
  reasoning?: string
  ai_recommendation?: string
  human_decision?: string
  approver?: string | null
  approval_time?: string | null
  payload?: Record<string, any>
  decision: {
    service: string
    decision: string
    risk_level: 'low' | 'medium' | 'high' | 'critical'
    status: 'pending' | 'approved' | 'rejected'
    metadata: Record<string, any>
  }
  risk_score: number
  status: 'pending' | 'approved' | 'rejected' | 'expired' | 'auto_approved'
  created_at?: string
  updated_at?: string
  timestamp?: string
  approved_by?: string
  approval_note?: string
  expires_at?: string
}

export interface ApprovalStats {
  pending_count: number
  approved_count: number
  rejected_count: number
  expired_count?: number
  executed_count?: number
  total_processed: number
}

export interface AuditEntry {
  timestamp: string
  action: string
  entity_id: string
  actor: string
  payload: Record<string, any>
}

export interface SystemMetrics {
  timestamp: string
  service: string
  cloud: string
  metrics: {
    latency_ms: number
    cpu_usage_percent: number
    error_rate_percent: number
    memory_usage_percent: number
    requests_per_second: number
    active_connections: number
  }
}

export interface ServiceState {
  service: string
  cloud: string
  timestamp: string
  belief: {
    latency_ewma: number
    trend: string
    confidence: number
    status: 'healthy' | 'stressed' | 'overloaded'
  }
  intent: {
    current_load: number
    optimal_load: number
  }
}

export interface ProcessStatus {
  name: string
  status: 'healthy' | 'degraded' | 'failed' | 'unknown' | 'stopped'
  pid: number | null
  cpu_percent: number
  memory_mb: number
  restart_count: number
  started_at: string | null
  port: number | null
}

export interface WebSocketMessage {
  type: 'logs' | 'metrics' | 'events' | 'health'
  data: any
  timestamp: string
}

export interface CSPHealth {
  cloud: string
  status: 'healthy' | 'degraded' | 'failed'
  latency_ms: number
  error_rate: number
  cpu_usage: number
  memory_usage: number
  active_connections: number
  requests_per_second: number
  timestamp: string
}

export interface CSPTraffic {
  cloud: string
  total_requests: number
  successful_requests: number
  failed_requests: number
  average_latency_ms: number
  p95_latency_ms: number
  p99_latency_ms: number
  requests_per_second: number
  bandwidth_mbps: number
  timestamp: string
}
