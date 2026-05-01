"""Send test event to Kafka metrics.events topic."""

from kafka import KafkaProducer
import json
import datetime

p = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

event = {
    'service': 'service-cache-aws',
    'cloud': 'aws',
    'timestamp': datetime.datetime.utcnow().isoformat(),
    'metrics': {'request_latency_ms': 42.0, 'request_count': 1}
}

partition = 1
f = p.send('metrics.events', value=event, partition=partition)
res = f.get(timeout=10)
print('SENT', res.topic, res.partition, res.offset)
p.flush()
