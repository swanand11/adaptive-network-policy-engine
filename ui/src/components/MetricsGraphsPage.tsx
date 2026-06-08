import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

interface CSPMetric {
  cloud: string
  series?: Record<string, number[]>
}

interface Props {
  loading?: boolean
  cspMetrics: CSPMetric[]
}

const METRIC_KEYS = [
  { key: 'latency_ms', label: 'Latency (ms)', color: '#3b82f6' },
  { key: 'error_rate_percent', label: 'Error Rate (%)', color: '#ef4444' },
  { key: 'cpu_usage_percent', label: 'CPU (%)', color: '#10b981' },
  { key: 'memory_usage_percent', label: 'Memory (%)', color: '#8b5cf6' },
]

export function MetricsGraphsPage({ loading, cspMetrics }: Props) {
  if (loading) return <div className="glass-panel">Loading metric graphs...</div>

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-outfit font-bold tracking-tight">Metrics Dashboard</h2>
      </div>
      
      {cspMetrics.length === 0 ? (
        <div className="glass-panel flex flex-col items-center justify-center p-12 text-text-muted">
          <div className="w-16 h-16 border-4 border-t-primary border-r-transparent border-b-transparent border-l-transparent rounded-full animate-spin mb-4 opacity-50"></div>
          <p>Awaiting real-time metric streams from Kafka...</p>
        </div>
      ) : (
        cspMetrics.map((csp) => (
          <div key={csp.cloud} className="bg-background-elevated/40 border border-border/50 rounded-2xl p-6 backdrop-blur-md shadow-2xl">
            <div className="flex items-center space-x-3 mb-6">
              <div className="h-4 w-1.5 bg-primary rounded-full shadow-[0_0_8px_rgba(0,240,255,0.6)]"></div>
              <h3 className="text-xl font-bold uppercase tracking-widest text-text-primary">{csp.cloud}</h3>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              {METRIC_KEYS.map((m) => {
                const series = csp.series?.[m.key] || []
                const latest = series.length ? series[series.length - 1] : 0
                
                const chartData = series.map((v, i) => ({
                  index: i,
                  value: v
                }))
                
                return (
                  <div key={m.key} className="bg-black/30 border border-border/40 rounded-xl p-5 hover:border-border/80 transition-colors duration-300">
                    <div className="flex items-center justify-between mb-4">
                      <div className="flex items-center space-x-2">
                        <div className="w-2.5 h-2.5 rounded-full shadow-sm" style={{ backgroundColor: m.color, boxShadow: `0 0 8px ${m.color}80` }} />
                        <h4 className="text-sm font-semibold text-text-secondary uppercase tracking-wider">{m.label}</h4>
                      </div>
                      <p className="font-mono text-xl font-bold drop-shadow-sm" style={{ color: m.color }}>
                        {latest.toFixed(2)}
                      </p>
                    </div>
                    
                    <div className="h-44 w-full">
                      <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={chartData} margin={{ top: 5, right: 0, left: -25, bottom: 0 }}>
                          <defs>
                            <linearGradient id={`color_${csp.cloud}_${m.key}`} x1="0" y1="0" x2="0" y2="1">
                              <stop offset="5%" stopColor={m.color} stopOpacity={0.35}/>
                              <stop offset="95%" stopColor={m.color} stopOpacity={0.0}/>
                            </linearGradient>
                          </defs>
                          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" vertical={false} />
                          <XAxis dataKey="index" hide />
                          <YAxis 
                            domain={['auto', 'auto']} 
                            tick={{ fill: 'rgba(255,255,255,0.3)', fontSize: 10, fontFamily: 'monospace' }}
                            tickFormatter={(val) => val.toFixed(0)}
                            axisLine={false}
                            tickLine={false}
                          />
                          <Tooltip 
                            contentStyle={{ 
                              backgroundColor: 'rgba(15, 23, 42, 0.9)', 
                              border: '1px solid rgba(255,255,255,0.1)', 
                              borderRadius: '8px',
                              backdropFilter: 'blur(8px)',
                              boxShadow: '0 10px 25px -5px rgba(0, 0, 0, 0.5)'
                            }}
                            itemStyle={{ color: m.color, fontWeight: 'bold', fontFamily: 'monospace' }}
                            labelStyle={{ display: 'none' }}
                            formatter={(value: number) => [value.toFixed(2), m.label]}
                            animationDuration={150}
                          />
                          <Area 
                            type="monotone" 
                            dataKey="value" 
                            stroke={m.color} 
                            strokeWidth={2.5}
                            fillOpacity={1} 
                            fill={`url(#color_${csp.cloud}_${m.key})`} 
                            isAnimationActive={false}
                          />
                        </AreaChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        ))
      )}
    </div>
  )
}

