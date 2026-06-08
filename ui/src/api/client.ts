import axios, { AxiosInstance } from 'axios'

const API_BASE_URL = 'http://localhost:5000'

const client: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Add CORS headers
client.interceptors.request.use((config) => {
  config.headers['Access-Control-Allow-Origin'] = '*'
  return config
})

export default client
