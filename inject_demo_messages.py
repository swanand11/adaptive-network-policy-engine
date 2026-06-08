import time
import json
import random
import uuid
from kafka import KafkaProducer

def main():
    print("Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("Starting to inject demo messages. Press Ctrl+C to stop.")
    
    clouds = ["aws", "aks", "digitalocean"]
    
    try:
        while True:
            # 1. Inject Metrics
            for cloud in clouds:
                metrics = {
                    "cloud": cloud,
                    "cpu_usage_percent": random.uniform(20.0, 85.0),
                    "memory_usage_percent": random.uniform(30.0, 75.0),
                    "latency_ms": random.uniform(10.0, 150.0),
                    "error_rate_percent": random.uniform(0.0, 2.0),
                    "timestamp": time.time()
                }
                # Randomly spike some metrics
                if random.random() < 0.1:
                    metrics["cpu_usage_percent"] = random.uniform(85.0, 99.0)
                    metrics["error_rate_percent"] = random.uniform(5.0, 15.0)
                
                producer.send("metrics.events", value=metrics)
                print(f"Sent metrics for {cloud}")

            # 2. Randomly inject Decisions
            if random.random() < 0.3:
                decision_id = str(uuid.uuid4())
                decision = {
                    "decision_id": decision_id,
                    "risk_level": random.choice(["low", "medium", "high", "critical"]),
                    "status": "pending",
                    "metadata": {
                        "action": "scale_up",
                        "target": random.choice(clouds),
                        "reason": "High CPU utilization detected"
                    },
                    "timestamp": time.time()
                }
                producer.send("policy.decisions", value=decision)
                print(f"Sent decision {decision_id}")
                
                # Sometime simulate approval shortly after
                if random.random() < 0.5:
                    approval = {
                        "decision_id": decision_id,
                        "status": "approved",
                        "metadata": decision["metadata"],
                        "timestamp": time.time() + 1
                    }
                    producer.send("policy.approved", value=approval)
                    print(f"Sent approval for {decision_id}")

            producer.flush()
            time.sleep(2)  # Inject messages every 2 seconds
            
    except KeyboardInterrupt:
        print("\nStopping message injection.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
