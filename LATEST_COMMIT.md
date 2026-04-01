## DETAILED EXPLANATION OF MOCKS & PROMETHEUS WORK

### THE FOUNDATIONAL PROBLEM WE SOLVED

You started with a distributed event-driven multi-cloud traffic management system where the infrastructure (Docker Compose, Kafka, Zookeeper, MongoDB, Redis) was already set up. However, there were no actual metric sources to feed into this system. The system needed to simulate three different cloud providers (AWS, Azure Kubernetes Service (AKS), and DigitalOcean) generating realistic metrics. These metrics needed to be collected in a standardized way, labeled properly for multi-cloud identification, and prepared for downstream Kafka integration.

### WHAT WE BUILT: THE MULTI-CLOUD METRICS SIMULATION LAYER

The solution we implemented creates a complete metrics ingestion pipeline that mimics real-world cloud infrastructure monitoring. Here's what each component does:

**The Core Architecture:**
- Three independent cloud provider simulators run as Flask web services in Docker containers
- Each simulator generates realistic metrics specific to its cloud provider
- Prometheus scrapes metrics from these three services every 5 seconds
- All metrics include consistent labels (cloud provider name and service ID)
- The structured metrics are now ready to be consumed by a Kafka producer in the next phase

### THE FILES WE CREATED AND WHAT THEY DO

**1. `/mocks/base_simulator.py` - THE FOUNDATION**

This is the abstract base class that all cloud simulators inherit from. Think of it as a blueprint that ensures all simulators behave consistently. Here's what it provides:

- It sets up a Flask web server that listens on a specified port
- It initializes Prometheus metrics using the prometheus_client library
- It defines six core metrics that EVERY simulator will expose: request latency (as a distribution over time), total request count, total error count, current CPU usage percentage, current memory usage percentage, and current error rate percentage
- All metrics include two labels: the cloud provider name and the service identifier
- It provides three HTTP endpoints: a root "/" endpoint that returns health status as JSON, a "/metrics" endpoint that returns all metrics in Prometheus text format, and a "/simulate" endpoint that can manually trigger metric generation
- It runs metric generation in a background thread that creates new metrics every 2 seconds continuously
- Each metric is stored in a separate registry to prevent any naming conflicts when all three services are running in Prometheus

Why we structured it this way: Flask is lightweight and perfect for this use case. Prometheus client library ensures we're following Prometheus conventions exactly. The background thread approach means the web server can serve metrics requests instantly without waiting for metric generation. The separate registry per service is crucial because it prevents metrics from one cloud provider from accidentally overwriting metrics from another cloud provider.

**2. `/mocks/aws_simulator.py` - AWS CLOUD PROVIDER**

This simulator models AWS EC2 and ECS services. It inherits from BaseSimulator and implements AWS-specific metric generation logic. Here's what makes it AWS-specific:

- Service name defaults to "service-api" and runs on port 8001
- AWS is known for having the lowest latency among cloud providers, so we set the baseline latency to 45 milliseconds with only a small variance of plus or minus 15ms
- CPU usage baseline is 35 percent, representing a well-provisioned AWS infrastructure
- CPU variance is plus or minus 10 percent to show realistic fluctuations
- Error rate baseline is 0.5 percent because AWS has excellent SLA compliance
- Memory usage is random between 20-80 percent
- The simulator generates between 100-500 requests per second, typical for an API tier
- It adds a region label that randomly selects from real AWS regions: us-east-1, us-west-2, or eu-west-1
- Importantly, AWS metrics have NO artificial spikes or anomalies because AWS infrastructure is typically stable and predictable

The logic ensures all values stay within realistic bounds: latency never goes below 10ms, CPU stays between 5-100 percent, error rates stay between 0-10 percent.

**3. `/mocks/aks_simulator.py` - AZURE KUBERNETES SERVICE**

This simulator models containerized workloads running on Azure's managed Kubernetes service. It inherits from BaseSimulator and implements AKS-specific behavior. Here's what makes it AKS-specific:

- Service name defaults to "service-db" and runs on port 8002
- AKS has higher latency than AWS because of Kubernetes orchestration overhead, so baseline is 65 milliseconds with variance of plus or minus 20ms (higher variance than AWS)
- CPU baseline is 45 percent because of Kubernetes system overhead consuming extra CPU
- Error rate baseline is 1.0 percent, slightly higher than AWS due to complexity
- Memory usage ranges from 30-85 percent, representing pod resource constraints
- Requests per second range is 80-450, lower than AWS due to orchestration overhead
- It includes a pod_count metric showing 1-5 replicas, demonstrating Kubernetes horizontal scaling
- It includes a namespace label that randomly selects from Kubernetes namespaces: default, production, or staging

Crucially, AKS includes a realistic failure mode: 10 percent of the time, the simulator generates a "spike" event that simulates pod rebalancing. When this happens, latency multiplies by 1.5, CPU multiplies by 1.3, and error rate multiplies by 2. These spikes represent real Kubernetes events like pod eviction, rolling updates, or node pressure. This gives us realistic scenarios for our traffic management system to handle.

**4. `/mocks/dodroplets_simulator.py` - DIGITALOCEAN DROPLETS**

This simulator models simple virtual machine instances on DigitalOcean. It inherits from BaseSimulator and implements DO-specific behavior. Here's what makes it DigitalOcean-specific:

- Service name defaults to "service-cache" and runs on port 8003
- Baseline latency is 55 milliseconds with variance of plus or minus 12ms - faster than AKS because there's no orchestration overhead, but not quite as fast as AWS
- CPU baseline is 40 percent with 12 percent variance
- Error rate baseline is 0.8 percent, between AWS and AKS
- Memory usage is 25-75 percent, typical for VMs
- Requests per second range is 120-550, good performance for a cache tier
- It includes a bandwidth metric showing 50-300 Mbps, which is DigitalOcean-specific and useful for network-heavy workloads and CDN decisions
- It includes a region label with real DigitalOcean datacenter codes: nyc3 (New York), sfo3 (San Francisco), lon1 (London), sgp1 (Singapore)

DigitalOcean includes a different kind of anomaly: 15 percent of the time, a traffic burst occurs where CPU multiplies by 1.3. This represents sudden traffic spikes that VMs might experience.

**5. `/mocks/__init__.py` - MODULE INITIALIZATION**

This small file simply exports all three simulator classes so that anywhere in the codebase, you can do:
```
from mocks import AWSSimulator, AKSSimulator, DigitalOceanSimulator
```
Instead of having to import from individual files.

**6. `/mocks/Dockerfile` - CONTAINERIZATION**

This Docker file defines how to build a container image for the mock services. Here's what it does:

- Starts with a Python 3.11 slim image, which is minimal and efficient
- Sets working directory to /app
- Copies the entire project into the container
- Uses pip to install all dependencies from requirements.txt
- Exposes ports 8001, 8002, 8003 (for the three services) and 9090 (for Prometheus)
- Sets a default command to run the AWS simulator, but this can be overridden in docker-compose

Why we did this: A single Dockerfile is used by all three simulators because they have identical dependencies and setup. The dockerfile-compose file then specifies which simulator to run as a command override. This reduces duplication and makes the system easier to maintain. The slim variant of Python keeps the image small, which matters when you're building and deploying containers.

**7. `/prometheus.yml` - PROMETHEUS CONFIGURATION**

This YAML configuration file tells Prometheus what services to scrape and how to label the metrics. Here's what it does:

- Sets global scrape interval to 5 seconds, meaning Prometheus will pull metrics from each service every 5 seconds
- Defines three scrape jobs, one for each cloud provider

For AWS job named "aws-service":
- Tells Prometheus to scrape the target "aws-simulator:8001" (docker-compose service name and port)
- Uses relabel configs to ADD labels to every metric that Prometheus scrapes
- Adds an "instance" label with the scrape target address
- Adds a "cloud" label with static value "aws"
- Adds a "service_id" label with static value "service-api"
- So every metric from AWS will have these three labels attached

For AKS job named "aks-service":
- Same concept but targets "aks-simulator:8002"
- Adds labels: instance, cloud="aks", service_id="service-db"

For DigitalOcean job named "do-service":
- Same concept but targets "digitalocean-simulator:8003"
- Adds labels: instance, cloud="digitalocean", service_id="service-cache"

Why relabel configs are CRITICAL: Without these, Prometheus would scrape the metrics but they wouldn't have labels identifying which cloud provider they came from. With these labels, you can query Prometheus like:
```
request_latency_seconds{cloud="aws"}
```
and get only AWS metrics, or compare:
```
cpu_usage_percent{cloud="aws"} vs {cloud="aks"} vs {cloud="digitalocean"}
```
to see CPU across all clouds. These labels are essential for the Kafka producer we'll build next, which needs to extract the cloud provider and service ID from the metrics.

**8. `/requirements.txt` - PYTHON DEPENDENCIES**

This file lists the Python packages needed:
- flask==2.3.0 - The web framework used by all simulators
- prometheus-client==0.17.0 - Library for creating and serving Prometheus metrics
- kafka-python==2.0.2 - Library for connecting to Kafka (needed by the producer we'll build next)
- python-dotenv==1.0.0 - Library for reading environment variables

We specified exact versions to ensure reproducibility - if everyone uses different versions, metrics might be formatted differently or features might behave differently.

**9. `/docker-compose.yml` - COMPLETE SYSTEM ORCHESTRATION (UPDATED)**

We updated the entire docker-compose file to add the new components. Here's what changed:

- Added a network definition called "app-network" that all services use. This allows services to reach each other by hostname (like "aws-simulator:8001") instead of IP addresses
- Updated existing services (zookeeper, kafka, redis, mongo) to use the app-network so everything is on the same virtual network
- Added three mock service definitions (aws-simulator, aks-simulator, digitalocean-simulator) that:
  - Build from the same Dockerfile in mocks/
  - Run different commands to start the correct simulator
  - Expose different ports (8001, 8002, 8003)
  - Set the SERVICE_PORT environment variable so they run on the correct port
  - Join the app-network
- Added Prometheus service that:
  - Uses the official Prometheus Docker image
  - Exposes port 9090 (the Prometheus web UI)
  - Mounts the prometheus.yml config file (read-only) so it knows what services to scrape
  - Sets startup arguments to use the config file and store data in /prometheus
  - Depends on all three simulators being started first
  - Joins the app-network so it can reach the simulators by hostname

This means you can now run:
```
docker-compose up --build
```
And the entire system (Kafka, Redis, MongoDB, three cloud simulators, and Prometheus) all start together, all connected, ready to go.

**10. `/kafka/enums.py` - CLOUD PROVIDER ENUM (UPDATED)**

We updated the CloudProvider enum from:
```
AWS = "aws"
AZURE = "azure"
GCP = "gcp"
```
To:
```
AWS = "aws"
AKS = "aks"
DIGITALOCEAN = "digitalocean"
```

This is important for type safety when the Kafka producer is built. It ensures that anywhere in the Kafka system, cloud provider values are validated against this enum. If someone tries to create a Kafka event with cloud="gcp", it will fail because that's no longer a valid option. This prevents typos and inconsistencies.

### HOW IT ALL WORKS TOGETHER

Here's the complete flow of what happens:

1. You run docker-compose up
2. Docker builds the simulators into a container image
3. All services start and connect to app-network
4. AWS simulator begins running Flask on port 8001, continuously generating AWS-specific metrics every 2 seconds in the background
5. AKS simulator begins running Flask on port 8002, continuously generating AKS-specific metrics every 2 seconds
6. DigitalOcean simulator begins running Flask on port 8003, continuously generating DO-specific metrics every 2 seconds
7. Prometheus starts up and reads prometheus.yml configuration
8. Prometheus says: "I need to scrape aws-simulator:8001/metrics every 5 seconds"
9. Every 5 seconds, Prometheus makes HTTP request to aws-simulator:8001/metrics
10. AWS simulator returns all its metrics in Prometheus text format
11. Prometheus adds the labels (cloud="aws", service_id="service-api", instance=...) to every metric
12. Prometheus stores these time series in its database
13. Same happens for AKS and DigitalOcean
14. You can now open http://localhost:9090 and see the Prometheus dashboard
15. You can query metrics like: request_latency_seconds{cloud="aws"} and see latency trends for AWS
16. You can query: error_rate_percent{cloud="aks"} and see AKS error rates
17. You can compare: cpu_usage_percent{cloud=~"aws|aks|digitalocean"} to see CPU across all clouds

### WHAT THIS ENABLES FOR NEXT PHASES

This foundation enables several things:

1. **Kafka Producer (Phase 1, next step)**: We can now write a service that queries Prometheus every few seconds like:
   - Query: request_latency_seconds{cloud="aws"}
   - Get response: [{"labels": {"cloud": "aws", "service_id": "service-api"}, "value": 47.2}]
   - Convert to MetricsEvent schema
   - Publish to Kafka metrics.events topic with partition key service_id@cloud
   - This gives Kafka a stream of real-time metrics

2. **Service Agent (Phase 2)**: Consumes from metrics.events, applies business logic like:
   - If latency from AWS > 100ms, create a policy decision to shift traffic
   - If AKS error rate > 2%, create a decision to scale up
   - Publishes decisions to policy.decisions topic

3. **Testing Traffic Management Decisions**: By having cloud-specific behavior (AWS stability, AKS spikes, DO bursts), we can test whether our traffic management agents make smart decisions

4. **Scaling**: New mock services can easily be added by creating new simulator classes, adding them to docker-compose, and adding new Prometheus scrape jobs

### WHY THIS APPROACH IS CORRECT

This implementation follows best practices:

1. **Separation of Concerns**: Each cloud provider is its own service, simulations are separate from Prometheus, Prometheus is separate from the application layer

2. **Realistic Metrics**: Each cloud has different characteristics that real applications experience (AWS is fast/stable, AKS has orchestration overhead and spikes, DO has traffic bursts)

3. **Standard Telemetry Format**: Using Prometheus metrics format means any tool that understands Prometheus (Grafana, alerting systems, observability platforms) can work with this

4. **Proper Labeling**: Every metric includes context (cloud, service) through labels, not just in the metric name, following Prometheus best practices

5. **Containerization**: Everything is containerized and defined in docker-compose, making it reproducible and environment-agnostic

6. **No Breaking Changes**: All existing infrastructure (Kafka, MongoDB, Redis) continues working unchanged

### WHAT YOU CAN DO NOW

Right now, you can:
```bash
docker-compose up --build
# Wait for services to start
curl http://localhost:8001/metrics  # See AWS metrics
curl http://localhost:8002/metrics  # See AKS metrics
curl http://localhost:8003/metrics  # See DO metrics
open http://localhost:9090          # View Prometheus dashboard
```

In Prometheus, you can run queries to see multi-cloud comparison:
- Which cloud has lowest latency?
- Which cloud has highest error rate?
- How does CPU usage compare across clouds?
- When does AKS have spikes?
- How often do DO traffic bursts occur?

### NEXT STEP

Build a Prometheus Adapter / Kafka Producer that:
1. Queries Prometheus API every X seconds
2. Reads latest metrics with their cloud and service_id labels
3. Converts each metric to a MetricsEvent (matching the Kafka schema in kafka/schemas.py)
4. Publishes to metrics.events topic with proper partitioning
5. Includes correlation IDs for tracing through the system

This bridges the monitoring layer (Prometheus + mock services) with the event processing layer (Kafka + agents).