import { create } from 'zustand'
import { Approval, ApprovalStats, ProcessStatus } from '../types'

interface AppStore {
  // Approvals
  pendingApprovals: Approval[]
  approvalStats: ApprovalStats | null
  selectedApproval: Approval | null
  
  weights: Record<string, number>
  
  // System
  systemStatus: ProcessStatus[]
  wsConnected: boolean
  
  // UI
  loading: boolean
  error: string | null
  currentPage: 'approvals' | 'weights' | 'metrics-graphs' | 'governance-queue' | 'audit-trail'

  // Actions
  setPendingApprovals: (approvals: Approval[]) => void
  setApprovalStats: (stats: ApprovalStats) => void
  setSelectedApproval: (approval: Approval | null) => void
  setWeights: (weights: Record<string, number>) => void
  setSystemStatus: (status: ProcessStatus[]) => void
  setWsConnected: (connected: boolean) => void
  setLoading: (loading: boolean) => void
  setError: (error: string | null) => void
  setCurrentPage: (page: 'approvals' | 'weights' | 'metrics-graphs' | 'governance-queue' | 'audit-trail') => void
}

export const useAppStore = create<AppStore>((set) => ({
  pendingApprovals: [],
  approvalStats: null,
  selectedApproval: null,
  weights: {},
  systemStatus: [],
  wsConnected: false,
  loading: false,
  error: null,
  currentPage: 'approvals',

  setPendingApprovals: (approvals) => set({ pendingApprovals: approvals }),
  setApprovalStats: (stats) => set({ approvalStats: stats }),
  setSelectedApproval: (approval) => set({ selectedApproval: approval }),
  setWeights: (weights) => set({ weights }),
  setSystemStatus: (status) => set({ systemStatus: status }),
  setWsConnected: (connected) => set({ wsConnected: connected }),
  setLoading: (loading) => set({ loading }),
  setError: (error) => set({ error }),
  setCurrentPage: (page) => set({ currentPage: page }),
}))
