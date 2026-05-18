import { useAppStore } from '../store'
import { Activity } from 'lucide-react'

export function Header() {
  const wsConnected = useAppStore((state) => state.wsConnected)

  return (
    <header className="border-b border-border pb-4 mb-5">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl md:text-4xl gradient-text font-outfit font-bold uppercase tracking-wide">
            Nexus Policy Engine
          </h1>
          <p className="text-text-muted text-xs md:text-sm uppercase tracking-wider mt-1">
            Proactive Agentic Multi-Cloud Orchestrator
          </p>
        </div>
        <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full border text-xs md:text-sm ${
          wsConnected 
            ? 'bg-green-500/10 border-good text-good' 
            : 'bg-yellow-500/10 border-warn text-warn'
        }`}>
          <Activity className="w-4 h-4" />
          <span className="text-sm font-outfit font-bold uppercase">
            {wsConnected ? 'Connected' : 'Connecting...'}
          </span>
        </div>
      </div>
    </header>
  )
}
