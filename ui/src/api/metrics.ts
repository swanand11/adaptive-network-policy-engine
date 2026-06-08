import client from './client'
import { ProcessStatus } from '../types'

export const metricsAPI = {
  getMetricsSummary: async (): Promise<any> => {
    const response = await client.get('/api/metrics')
    return response.data || {}
  },

  getLatestWeights: async (): Promise<Record<string, number>> => {
    const response = await client.get('/api/audit', { params: { limit: 200, search: 'weights' } })
    const rows = response.data?.entries || []
    for (const row of rows) {
      const maybeWeights = row?.payload?.details?.payload?.metadata?.weights || row?.payload?.payload?.metadata?.weights
      if (maybeWeights && typeof maybeWeights === 'object') return maybeWeights
    }
    return {}
  },

  getSystemStatus: async (): Promise<ProcessStatus[]> => {
    const healthResp = await client.get('/health')
    const metricsResp = await client.get('/api/metrics')
    const healthy = healthResp.data?.status === 'healthy' && metricsResp.data?.kafka_ingestion_health === 'healthy'
    return [{
      name: 'policy-engine-api',
      status: healthy ? 'healthy' : 'degraded',
      pid: null,
      cpu_percent: 0,
      memory_mb: 0,
      restart_count: 0,
      started_at: new Date().toISOString(),
      port: 8080,
    }]
  },
}
