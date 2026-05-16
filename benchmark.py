import time
import requests
import concurrent.futures
import csv
import sys
import argparse
from statistics import quantiles, mean

def fetch_prometheus_metric(url, metric_name):
    try:
        res = requests.get(url, timeout=2)
        if res.ok:
            for line in res.text.split('\n'):
                if line.startswith(metric_name):
                    try:
                        return float(line.split(' ')[1])
                    except:
                        pass
    except:
        pass
    return None

def make_request(url):
    start = time.time()
    try:
        res = requests.get(url, timeout=5)
        latency = time.time() - start
        return latency, res.status_code
    except Exception as e:
        latency = time.time() - start
        return latency, 500

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default="http://localhost:9000/")
    parser.add_argument('--concurrency', type=int, default=50)
    parser.add_argument('--requests', type=int, default=1000)
    parser.add_argument('--output', default="benchmark_results.csv")
    args = parser.parse_args()

    print(f"Starting benchmark to {args.url} with concurrency {args.concurrency}, total requests {args.requests}")
    
    latencies = []
    status_codes = []
    
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [executor.submit(make_request, args.url) for _ in range(args.requests)]
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            lat, code = future.result()
            latencies.append(lat)
            status_codes.append(code)
            if (i+1) % 100 == 0:
                print(f"Completed {i+1}/{args.requests} requests...")
                
    total_time = time.time() - start_time
    
    latencies.sort()
    
    # Calculate quantiles safely
    if len(latencies) >= 100:
        p99 = quantiles(latencies, n=100)[98] * 1000
    else:
        p99 = latencies[-1] * 1000
        
    p50 = latencies[len(latencies)//2] * 1000
    throughput = args.requests / total_time
    errors = sum(1 for c in status_codes if c >= 400)
    error_rate = (errors / args.requests) * 100
    
    print("\nBenchmark Complete!")
    print(f"Time taken: {total_time:.2f}s")
    print(f"Throughput: {throughput:.2f} req/s")
    print(f"p50 Latency: {p50:.2f} ms")
    print(f"p99 Latency: {p99:.2f} ms")
    print(f"Error Rate: {error_rate:.2f}%")
    
    # Collect mock CSP metrics
    clouds = {
        'aws': 'http://localhost:8001/metrics',
        'aks': 'http://localhost:8002/metrics',
        'do': 'http://localhost:8003/metrics',
    }
    
    csp_metrics = {}
    for cloud, url in clouds.items():
        cpu = fetch_prometheus_metric(url, 'cpu_usage_percent')
        csp_metrics[f"{cloud}_cpu"] = cpu if cpu is not None else -1
    
    # Write CSV
    write_header = False
    import os
    if not os.path.exists(args.output):
        write_header = True
        
    with open(args.output, mode='a', newline='') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(['concurrency', 'total_requests', 'throughput_req_s', 'p50_latency_ms', 'p99_latency_ms', 'error_rate_percent', 'aws_cpu', 'aks_cpu', 'do_cpu'])
            
        writer.writerow([
            args.concurrency,
            args.requests,
            round(throughput, 2),
            round(p50, 2),
            round(p99, 2),
            round(error_rate, 2),
            csp_metrics.get('aws_cpu', -1),
            csp_metrics.get('aks_cpu', -1),
            csp_metrics.get('do_cpu', -1)
        ])
    print(f"Results appended to {args.output}")

if __name__ == "__main__":
    main()
