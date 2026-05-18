import { useEffect, useState } from 'react'
import { Header, ApprovalCard, StatsPanel, SystemStatus, Navigation, GovernanceQueuePage, AuditTrailPage, WeightsPage, MetricsGraphsPage } from './components'
import { approvalsAPI } from './api/approvals'
import { metricsAPI } from './api/metrics'
import { useAppStore } from './store'
import { AuditEntry } from './types'

function App() {
  const {
    pendingApprovals,
    approvalStats,
    weights,
    loading,
    error,
    systemStatus,
    currentPage,
    setPendingApprovals,
    setApprovalStats,
    setWeights,
    setSystemStatus,
    setWsConnected,
    setLoading,
    setError,
  } = useAppStore()

  const [actionLoading, setActionLoading] = useState<string | null>(null)
  const [auditEntries, setAuditEntries] = useState<AuditEntry[]>([])
  const [auditSearch, setAuditSearch] = useState('')
  const [metricsSummary, setMetricsSummary] = useState<any>({})

  // Initialize polling-only dashboard (no websocket)
  useEffect(() => {
    const initializeApp = async () => {
      try {
        setLoading(true)
        setWsConnected(true)

        // Fetch initial data
        await fetchApprovals()
        await fetchStats()
        await fetchMetrics()
        await fetchAudit()
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to initialize app')
      } finally {
        setLoading(false)
      }
    }

    initializeApp()
  }, [])

  // Stable REST polling every 2 seconds
  useEffect(() => {
    const interval = setInterval(async () => {
      try {
        await fetchApprovals()
        await fetchStats()
        await fetchMetrics()
        await fetchAudit()
      } catch (e) {
        console.error('Failed to refresh data:', e)
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [])

  const fetchApprovals = async () => {
    try {
      const data = await approvalsAPI.getPending()
      setPendingApprovals(data.requests)
    } catch (e) {
      console.error('Failed to fetch approvals:', e)
    }
  }

  const fetchStats = async () => {
    try {
      const stats = await approvalsAPI.getStats()
      setApprovalStats(stats)
    } catch (e) {
      console.error('Failed to fetch stats:', e)
    }
  }

  const fetchMetrics = async () => {
    try {
      const [summary, latestWeights, status] = await Promise.all([
        metricsAPI.getMetricsSummary(),
        metricsAPI.getLatestWeights(),
        metricsAPI.getSystemStatus(),
      ])
      setMetricsSummary(summary)
      setWeights(latestWeights)
      setSystemStatus(status)
    } catch (e) {
      console.error('Failed to fetch metrics:', e)
    }
  }

  const fetchAudit = async () => {
    try {
      const data = await approvalsAPI.getAudit(auditSearch)
      setAuditEntries(data.entries || [])
    } catch (e) {
      console.error('Failed to fetch audit:', e)
    }
  }

  const handleApprove = async (id: string) => {
    try {
      setActionLoading(id)
      await approvalsAPI.approve(id, 'admin', 'Approved via UI')
      await fetchApprovals()
      await fetchStats()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to approve')
    } finally {
      setActionLoading(null)
    }
  }

  const handleReject = async (id: string) => {
    try {
      setActionLoading(id)
      await approvalsAPI.reject(id, 'admin', 'Rejected via UI')
      await fetchApprovals()
      await fetchStats()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to reject')
    } finally {
      setActionLoading(null)
    }
  }

  return (
    <div className="min-h-screen bg-dark p-8">
      <div className="max-w-7xl mx-auto">
        <Header />

        {error && (
          <div className="mb-6 p-4 bg-bad/10 border border-bad rounded-lg text-bad">
            {error}
          </div>
        )}

        <Navigation />

        {currentPage === 'approvals' && (
          <>
            <div className="grid grid-cols-3 gap-6 mb-8">
              <div className="col-span-2">
                <StatsPanel stats={approvalStats} loading={loading} />
              </div>
              <div>
                <SystemStatus status={systemStatus} loading={loading} />
              </div>
            </div>

            <div className="mb-6">
              <h2 className="text-2xl font-outfit font-bold mb-4">Pending Approvals</h2>
              {pendingApprovals.length === 0 ? (
                <div className="glass-panel text-center py-12">
                  <p className="text-text-muted">No pending approvals</p>
                </div>
              ) : (
                <div className="grid grid-cols-1 gap-4">
                  {pendingApprovals.map((approval) => (
                    <ApprovalCard
                      key={approval.id}
                      approval={approval}
                      onApprove={handleApprove}
                      onReject={handleReject}
                      isLoading={actionLoading === approval.id}
                    />
                  ))}
                </div>
              )}
            </div>
          </>
        )}

        {currentPage === 'weights' && (
          <WeightsPage
            loading={loading}
            weights={weights}
            metrics={{
              pending: metricsSummary?.pending_approvals || 0,
              executed: metricsSummary?.approval_stats?.EXECUTED || 0,
              ingestion: metricsSummary?.kafka_ingestion_health || 'unknown',
            }}
            cspMetrics={Object.entries(metricsSummary?.cloud_summary || {}).map(([cloud, row]: any) => ({
              cloud,
              count: Number(row?.count || 0),
              avg_risk: Number(row?.avg_risk || 0),
              latency_ms: Number(row?.latency_ms || 0),
              error_rate_percent: Number(row?.error_rate_percent || 0),
              cpu_usage_percent: Number(row?.cpu_usage_percent || 0),
              memory_usage_percent: Number(row?.memory_usage_percent || 0),
              series: row?.series || {},
            }))}
          />
        )}

        {currentPage === 'metrics-graphs' && (
          <MetricsGraphsPage
            loading={loading}
            cspMetrics={Object.entries(metricsSummary?.cloud_summary || {}).map(([cloud, row]: any) => ({
              cloud,
              series: row?.series || {},
            }))}
          />
        )}

        {currentPage === 'governance-queue' && (
          <GovernanceQueuePage
            approvals={pendingApprovals}
            onApprove={handleApprove}
            onReject={handleReject}
            loading={loading}
          />
        )}

        {currentPage === 'audit-trail' && (
          <AuditTrailPage
            entries={auditEntries}
            search={auditSearch}
            onSearch={setAuditSearch}
            loading={loading}
          />
        )}
      </div>
    </div>
  )
}

export default App
