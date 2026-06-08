import { WebSocketMessage } from '../types'

export class WebSocketClient {
  private ws: WebSocket | null = null
  private url: string
  private reconnectAttempts = 0
  private maxReconnectAttempts = 5
  private reconnectDelay = 3000
  private reconnectTimer: number | null = null
  private manualClose = false
  private listeners: Map<string, Set<(data: any) => void>> = new Map()

  constructor(url?: string) {
    const configured = (import.meta as any).env?.VITE_WS_URL as string | undefined
    this.url = url || configured || 'ws://localhost:8765'
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      let settled = false
      try {
        this.manualClose = false
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          settled = true
          resolve()
          return
        }

        this.ws = new WebSocket(this.url)

        this.ws.onopen = () => {
          console.log('WebSocket connected')
          this.reconnectAttempts = 0
          if (!settled) {
            settled = true
            resolve()
          }
        }

        this.ws.onmessage = (event) => {
          try {
            const message: WebSocketMessage = JSON.parse(event.data)
            this.emit(message.type, message.data)
          } catch (e) {
            console.error('Failed to parse WebSocket message:', e)
          }
        }

        this.ws.onerror = (error) => {
          console.error('WebSocket error:', error)
          if (!settled) {
            settled = true
            reject(error)
          }
        }

        this.ws.onclose = () => {
          console.log('WebSocket disconnected')
          if (!this.manualClose) {
            this.attemptReconnect()
          }
        }
      } catch (e) {
        if (!settled) {
          settled = true
          reject(e)
        }
      }
    })
  }

  private attemptReconnect() {
    if (this.reconnectTimer !== null) {
      return
    }
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++
      console.log(`Attempting to reconnect (${this.reconnectAttempts}/${this.maxReconnectAttempts})...`)
      this.reconnectTimer = window.setTimeout(() => {
        this.reconnectTimer = null
        this.connect().catch((e) => console.error('Reconnection failed:', e))
      }, this.reconnectDelay)
    }
  }

  on(type: string, callback: (data: any) => void) {
    if (!this.listeners.has(type)) {
      this.listeners.set(type, new Set())
    }
    this.listeners.get(type)!.add(callback)

    return () => {
      this.listeners.get(type)?.delete(callback)
    }
  }

  private emit(type: string, data: any) {
    const callbacks = this.listeners.get(type)
    if (callbacks) {
      callbacks.forEach((callback) => callback(data))
    }
  }

  send(message: any) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message))
    }
  }

  disconnect() {
    this.manualClose = true
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    if (this.ws) {
      this.ws.close()
      this.ws = null
    }
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN
  }
}
