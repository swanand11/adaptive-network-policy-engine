import { useAppStore } from '../store'
import { BarChart3, Scale, ShieldAlert, ClipboardList, LineChart } from 'lucide-react'

export function Navigation() {
  const currentPage = useAppStore((state) => state.currentPage)
  const setCurrentPage = useAppStore((state) => state.setCurrentPage)

  const pages = [
    { id: 'approvals', label: 'Approvals', icon: BarChart3 },
    { id: 'weights', label: 'Weights', icon: Scale },
    { id: 'metrics-graphs', label: 'Metrics Graphs', icon: LineChart },
    { id: 'governance-queue', label: 'Governance Queue', icon: ShieldAlert },
    { id: 'audit-trail', label: 'Logs', icon: ClipboardList },
  ] as const

  return (
    <nav className="flex gap-2 mb-8 border-b border-border pb-4">
      {pages.map(({ id, label, icon: Icon }) => (
        <button
          key={id}
          onClick={() => setCurrentPage(id)}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg font-outfit font-bold uppercase text-sm transition-all ${
            currentPage === id
              ? 'bg-accent-secondary text-white'
              : 'bg-panel border border-border text-text-muted hover:border-accent-primary'
          }`}
        >
          <Icon className="w-4 h-4" />
          {label}
        </button>
      ))}
    </nav>
  )
}
