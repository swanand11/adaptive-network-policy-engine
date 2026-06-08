import client from './client'
import { Approval, ApprovalStats, AuditEntry } from '../types'

export const approvalsAPI = {
  getPending: async (): Promise<{ count: number; requests: Approval[] }> => {
    const response = await client.get('/api/highrisk/pending')
    return response.data
  },

  getApproval: async (id: string): Promise<Approval | null> => {
    const response = await client.get('/api/highrisk/history', { params: { limit: 500 } })
    const rows = response.data?.requests || []
    return rows.find((r: Approval) => r.id === id) || null
  },

  approve: async (
    id: string,
    approvedBy: string,
    note?: string,
    overrideWeights?: Record<string, number>
  ): Promise<{ status: string; approval_id: string; approved_by: string }> => {
    const response = await client.post(`/api/highrisk/approve/${id}`, {
      approver: approvedBy,
      note,
      override_weights: overrideWeights,
    })
    return response.data
  },

  reject: async (
    id: string,
    rejectedBy: string,
    note?: string
  ): Promise<{ status: string; approval_id: string; rejected_by: string }> => {
    const response = await client.post(`/api/highrisk/reject/${id}`, {
      approver: rejectedBy,
      note,
    })
    return response.data
  },

  getHistory: async (limit: number = 100): Promise<{ count: number; requests: Approval[] }> => {
    const response = await client.get('/api/highrisk/history', { params: { limit } })
    return response.data
  },

  getStats: async (): Promise<ApprovalStats> => {
    const [pendingResp, historyResp] = await Promise.all([
      client.get('/api/highrisk/pending'),
      client.get('/api/highrisk/history', { params: { limit: 500 } }),
    ])
    const pending = pendingResp.data?.requests || []
    const history = historyResp.data?.requests || []
    const approved = history.filter((r: any) => r.status === 'APPROVED').length
    const rejected = history.filter((r: any) => r.status === 'REJECTED').length
    const statsResp = await client.get('/api/highrisk/stats')
    return {
      pending_count: pending.length,
      approved_count: approved,
      rejected_count: rejected,
      expired_count: statsResp.data?.EXPIRED || 0,
      executed_count: statsResp.data?.EXECUTED || 0,
      total_processed: approved + rejected,
    }
  },

  getAudit: async (search: string = '', limit: number = 300): Promise<{ count: number; entries: AuditEntry[] }> => {
    const response = await client.get('/api/audit', { params: { search, limit } })
    return response.data
  },

  getHealth: async (): Promise<{ status: string; service: string; timestamp: string }> => {
    const response = await client.get('/health')
    return response.data
  },
}
