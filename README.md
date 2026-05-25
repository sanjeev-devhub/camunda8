# The Complete Camunda 8 + Spring Boot Guide
### From Beginner to Production — A Senior Architect's Playbook

> **Audience**: Backend developers (especially Java/Spring) transitioning into workflow orchestration with Camunda 8 / Zeebe.
> **Style**: Practical, production-oriented, with working code, configs, diagrams, and troubleshooting.

---

## Table of Contents

1. [Introduction to Camunda 8](#1-introduction-to-camunda-8)
2. [Zeebe Architecture Deep Dive](#2-zeebe-architecture-deep-dive)
3. [How to Start Zeebe Locally](#3-how-to-start-zeebe-locally)
4. [Create Your First BPMN Process](#4-create-your-first-bpmn-process)
5. [Connect Camunda 8 with Spring Boot](#5-connect-camunda-8-with-spring-boot)
6. [Spring Boot + Zeebe Full Integration](#6-spring-boot--zeebe-full-integration)
7. [Zeebe Job Workers in Detail](#7-zeebe-job-workers-in-detail)
8. [Advanced Workflow Features](#8-advanced-workflow-features)
9. [Production Deployment](#9-production-deployment)
10. [Observability and Troubleshooting](#10-observability-and-troubleshooting)
11. [Performance Optimization](#11-performance-optimization)
12. [Real-World Architecture Example](#12-real-world-architecture-example)
13. [Best Practices](#13-best-practices)
14. [Full Sample Project Structure](#14-full-sample-project-structure)
15. [Testing, CI/CD, and Learning Roadmap](#15-testing-cicd-and-learning-roadmap)

---

## 1. Introduction to Camunda 8

### 1.1 What is Camunda 8?

Camunda 8 is a **cloud-native, distributed workflow and decision orchestration platform**. Unlike its predecessor (Camunda 7), it is not a Java library embedded in your application — it is a horizontally scalable distributed system you talk to over **gRPC/REST**. Your application becomes a *client* of the orchestrator.

The platform models business processes using **BPMN 2.0** (Business Process Model and Notation) and decision logic using **DMN 1.3** (Decision Model and Notation). Workflows are designed visually in **Camunda Modeler**, then deployed to the runtime engine where they are executed as state machines.

### 1.2 Camunda 7 vs Camunda 8

| Aspect | Camunda 7 | Camunda 8 |
|--------|-----------|-----------|
| **Engine** | Java library, embeds in JVM | Standalone distributed cluster (Zeebe) |
| **Database** | Relational (Postgres, Oracle, MySQL) | Event-sourced log + RocksDB (built-in) |
| **Communication** | In-process Java API, REST | gRPC + REST (Camunda 8.5+) |
| **Scalability** | Vertical, DB-bound | Horizontal, partition-based |
| **Job Workers** | External tasks (long-polling REST) | gRPC streaming workers |
| **Tenancy** | Multi-tenant via DB | Native multi-tenancy |
| **Tx model** | ACID with your business DB | Eventual consistency, no shared TX |
| **Deployment** | War in app server, or embedded | Docker / Kubernetes / SaaS |
| **Hot reload** | Possible | Not possible — process is versioned |

**The big mental shift**: in C7, the engine ran *inside* your transaction. In C8, the engine is a *remote service*. You can no longer assume "if my service task throws, the database rollback covers everything." This drives most architectural decisions (idempotency, compensation, sagas).

### 1.3 Core Components

```
+-------------------------------------------------------------+
|                      Camunda 8 Platform                     |
+-------------------------------------------------------------+
|                                                             |
|   +-----------+   +----------+   +-----------+              |
|   |  Modeler  |   | Console  |   | Identity  |              |
|   | (design)  |   | (mgmt)   |   | (auth)    |              |
|   +-----------+   +----------+   +-----------+              |
|                                                             |
|   +------------+  +-----------+  +-----------+              |
|   |  Operate   |  | Tasklist  |  | Optimize  |              |
|   | (monitor)  |  | (humans)  |  | (BI/KPIs) |              |
|   +-----+------+  +-----+-----+  +-----+-----+              |
|         |               |              |                    |
|         v               v              v                    |
|   +-----------------------------------------+               |
|   |        Elasticsearch / OpenSearch       |  <-- read     |
|   +-----------------------------------------+      models   |
|                       ^                                     |
|                       |  exporter                           |
|   +-----------------------------------------+               |
|   |        ZEEBE (broker + gateway)         |  <-- engine   |
|   +-----------------------------------------+               |
|         ^                       ^                           |
|         | gRPC (workers)        | gRPC (clients)            |
|         |                       |                           |
|   +-----+-------+         +-----+--------+                  |
|   | Job Workers |         | Your App     |                  |
|   | (Spring)    |         | (REST API)   |                  |
|   +-------------+         +--------------+                  |
+-------------------------------------------------------------+
```

- **Zeebe** — the workflow engine itself. Streams events to Elasticsearch.
- **Operate** — web UI to monitor running instances, see incidents, retry failures.
- **Tasklist** — web UI where humans claim and complete user tasks.
- **Optimize** — BI/analytics over historical process data (Enterprise).
- **Identity** — Keycloak-based IAM for users, clients, permissions.
- **Connectors** — pre-built integrations (HTTP, Kafka, SendGrid, AWS, etc.) running as out-of-process workers.
- **Web Modeler / Desktop Modeler** — BPMN/DMN design tools.

### 1.4 Orchestration Concepts

Workflow **orchestration** ≠ **choreography**:

- **Choreography**: services react to events, no central brain ("Kafka spaghetti"). Hard to debug, no global view.
- **Orchestration**: a central engine drives the flow, calls services, waits for replies, handles compensation. Explicit, visible, recoverable.

Camunda 8 is an orchestrator. The BPMN diagram **is** the source of truth for "what happens when an order arrives." Operate gives you the live view: where is each instance, what variables, what failed, why.

### 1.5 BPMN Basics

The elements you'll use 90% of the time:

```
○         Start Event           — where the process begins
◉         End Event             — where it terminates
□         Task (rectangle)      — work to be done
◇         Gateway (diamond)     — branching/merging logic
─→        Sequence Flow         — execution order
✉         Message Event         — wait for/throw a message
⏰        Timer Event           — wait for a duration/date
```

Task subtypes:

| Symbol/Marker | Task Type | When to use |
|---|---|---|
| ⚙ Service Task | Service Task | Code runs — calls your worker |
| 👤 User Task | User Task | Human action via Tasklist |
| ✉ Send/Receive | Message Task | Sync/async messaging |
| 📞 Call Activity | Call Activity | Invoke another BPMN process |
| ◇ XOR Gateway | Exclusive | Single path based on condition |
| ◈ AND Gateway | Parallel | All paths execute |
| ◊ Event Gateway | Event-based | First event wins (timeout race) |

### 1.6 Workflow Lifecycle

```
DESIGNED → DEPLOYED → INSTANTIATED → ACTIVE → (COMPLETED | TERMINATED | INCIDENT)
   |          |             |           |
 .bpmn    Zeebe stores    new PI      jobs being
  file    process def     created     activated &
                                      completed
```

A **process definition** is the BPMN model (versioned by `processDefinitionKey`). A **process instance** is one running execution of that definition. Each token (the conceptual "where am I in the flow") creates **jobs** when it hits service tasks — these are what workers pull and complete.

### 1.7 Event-Driven Workflow Orchestration

Zeebe is event-sourced internally. Every state change is an immutable record appended to a log. This gives you:

- **Replay**: rebuild state by reading the log.
- **Auditability**: every variable change, every job activation, every incident is recorded.
- **Decoupling**: workers don't have to be online when a job is created; they pull when they're ready.
- **Time travel**: Operate shows you exactly what happened, in order.

---

## 2. Zeebe Architecture Deep Dive

### 2.1 The High-Level Picture

```
                                gRPC :26500
                                    |
                            +-------+-------+
                            |    Gateway    |   <-- stateless, load balancer
                            |   (gateway)   |
                            +-------+-------+
                                    | internal gRPC :26502
        +---------------------------+---------------------------+
        |                           |                           |
   +----v----+                 +----v----+                 +----v----+
   | Broker 0|                 | Broker 1|                 | Broker 2|
   +---------+                 +---------+                 +---------+
   | Part 0 L|                 | Part 1 L|                 | Part 2 L|
   | Part 1 F|                 | Part 2 F|                 | Part 0 F|
   | Part 2 F|                 | Part 0 F|                 | Part 1 F|
   +---------+                 +---------+                 +---------+
   RocksDB                     RocksDB                     RocksDB
   + log                       + log                       + log

   L = Leader of partition       F = Follower (replica)
```

### 2.2 Broker

The **broker** is the heart of Zeebe. Each broker node:

- Stores one or more **partition** replicas on local disk.
- Runs a **stream processor** that reads commands, applies them, writes events.
- Persists state in **RocksDB** (embedded key-value store).
- Replicates state to followers via **Raft** consensus.

Brokers do *not* share storage. Each broker has local disk. This is what enables horizontal scaling — no shared DB, no shared FS, no contention.

### 2.3 Gateway

The **gateway** is stateless. It:

- Terminates client gRPC connections (port `26500` external).
- Routes requests to the correct partition's leader broker.
- Aggregates responses for fan-out commands (e.g., topology).
- Can be scaled independently of brokers.

In small clusters you can run gateway *embedded* in the broker (same JVM). In production you run **standalone gateways** behind a load balancer.

### 2.4 Partitions

A **partition** is the unit of parallelism in Zeebe. Think Kafka partitions:

- Each partition is an independent event log + state.
- Each partition has **one leader** at a time (handles writes).
- Followers replicate the log via Raft.
- Process instances are pinned to a partition by hash of their key.

**Why partitions matter**: throughput scales linearly with partition count *up to* the number of brokers. With 4 brokers and 8 partitions, each broker leads 2 partitions. The replication factor defines how many copies exist.

**Sizing rule of thumb**:

```
partitions = expected peak throughput / per-partition throughput
           ≈ target PI/sec / ~150
replication factor = 3 (production minimum)
brokers ≥ replication factor
```

You **cannot change partition count after cluster creation** — choose wisely upfront.

### 2.5 Replication (Raft)

Each partition is a Raft group. A write is acknowledged when a **quorum** of replicas have persisted it.

```
Replication factor 3, quorum = 2
Tolerates 1 broker failure with no data loss.

Replication factor 5, quorum = 3
Tolerates 2 broker failures with no data loss.
```

### 2.6 State Storage

Zeebe stores two things:

1. **Event log** — append-only file per partition. The source of truth.
2. **RocksDB snapshot** — materialized state (current variables, active jobs, timers) for fast lookups.

The state in RocksDB is rebuildable from the log. Zeebe takes periodic snapshots and prunes the log up to the last snapshot (configurable retention).

### 2.7 Job Workers

A **job worker** is your code. It calls `ActivateJobs` over gRPC and gets a batch of jobs to process. Crucially:

- Workers **pull** jobs (long-polling, then gRPC streaming in 8.5+).
- A job is **leased** to one worker for a configurable timeout.
- If the worker doesn't complete/fail it within the timeout, the job is re-activated for another worker.
- Workers complete jobs with variables, fail them (decrements retry count), or throw a BPMN error.

```
+--------+   activateJobs(type="payment", maxJobs=10)   +--------+
| Worker |  ----------------------------------------->  | Zeebe  |
|        |  <-----------------------------------------  |        |
+--------+      [Job{key,vars,deadline}, ...]           +--------+
   |
   | (process)
   v
+--------+   completeJob(key, vars={result:"ok"})       +--------+
| Worker |  ----------------------------------------->  | Zeebe  |
+--------+                                              +--------+
```

### 2.8 Message Correlation

Zeebe correlates incoming messages to waiting process instances by **correlation key**:

```
Publish message:
   name = "OrderPaid"
   correlationKey = "ORDER-12345"
   variables = { paymentId: "PAY-99" }

Zeebe finds the unique PI subscribed to message name "OrderPaid"
with correlation key value "ORDER-12345" and resumes it.
```

If no subscriber exists yet, the message is buffered for its TTL.

### 2.9 Timers and Retries

- **Timers** are first-class events stored in RocksDB. The stream processor checks for due timers each tick.
- **Job retries** decrement on `failJob`. When retries reach 0, an **incident** is raised — visible in Operate, requires human action.
- **Backoff**: from 8.1+, `failJob` accepts `retryBackOff` for exponential retry delays.

### 2.10 Fault Tolerance & Scaling

- Lose a broker → followers elect a new leader, ~5-10s blip.
- Lose the gateway → clients reconnect.
- Lose Elasticsearch → engine keeps running; Operate gets stale; back-pressure if log can't be exported.
- Disk fills → engine refuses new writes (back-pressure), incidents created.

### 2.11 gRPC Communication

Zeebe's protocol is gRPC. Why:

- HTTP/2 multiplexing — many concurrent jobs over one connection.
- Streaming (`ActivateJobs` is server-streaming in 8.5+).
- Binary protobuf — small, fast.

Since 8.5, a **REST API** is also available at port `8080` of the gateway, primarily for orchestration calls (not high-throughput worker traffic).

---

## 3. How to Start Zeebe Locally

### 3.1 Prerequisites

- **Java 21+** (for the Spring app, not Zeebe itself).
- **Docker 20+** and **Docker Compose v2**.
- **Maven 3.9+** or **Gradle 8+**.
- 8 GB RAM minimum, 16 GB recommended.
- Ports free: `26500`, `8080`, `8081`, `8082`, `9200`, `5432`, `18080`.

### 3.2 Option A: Camunda Platform Docker Compose (recommended for local)

This is the official "everything-in-one" stack — Zeebe, Operate, Tasklist, Optimize, Identity, Connectors, Elasticsearch, Keycloak.

```bash
git clone https://github.com/camunda/camunda-platform.git
cd camunda-platform
docker compose up -d
```

This takes 2–5 minutes to fully start. Verify:

```bash
# Topology check
docker run --rm --network camunda-platform_camunda-platform \
  camunda/zeebe:8.7.0 zbctl status --address zeebe:26500 --insecure
```

Access:
- Operate: http://localhost:8081 (demo / demo)
- Tasklist: http://localhost:8082 (demo / demo)
- Optimize: http://localhost:8083 (demo / demo)
- Identity: http://localhost:8084 (demo / demo)
- Keycloak: http://localhost:18080 (admin / admin)
- Zeebe gRPC: localhost:26500

### 3.3 Option B: Minimal Docker Compose (Zeebe + Operate only)

For day-to-day dev work you rarely need the full stack. This is faster and uses less RAM:

```yaml
# docker-compose-minimal.yml
version: "3.8"

services:
  zeebe:
    image: camunda/zeebe:8.7.0
    container_name: zeebe
    ports:
      - "26500:26500"   # gRPC for clients/workers
      - "9600:9600"     # monitoring (metrics, ready, health)
      - "8080:8080"     # REST API (8.5+)
    environment:
      - ZEEBE_BROKER_CLUSTER_PARTITIONSCOUNT=4
      - ZEEBE_BROKER_CLUSTER_REPLICATIONFACTOR=1
      - ZEEBE_BROKER_CLUSTER_CLUSTERSIZE=1
      - ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_CLASSNAME=io.camunda.zeebe.exporter.ElasticsearchExporter
      - ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_URL=http://elasticsearch:9200
      - ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_BULK_SIZE=1
      - ZEEBE_BROKER_GATEWAY_SECURITY_AUTHENTICATION_MODE=none
    volumes:
      - zeebe-data:/usr/local/zeebe/data
    depends_on:
      - elasticsearch
    networks: [camunda]

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.13.4
    container_name: elasticsearch
    ports: ["9200:9200"]
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - ES_JAVA_OPTS=-Xms1g -Xmx1g
    volumes:
      - es-data:/usr/share/elasticsearch/data
    networks: [camunda]

  operate:
    image: camunda/operate:8.7.0
    container_name: operate
    ports: ["8081:8080"]
    environment:
      - CAMUNDA_OPERATE_ZEEBE_GATEWAYADDRESS=zeebe:26500
      - CAMUNDA_OPERATE_ELASTICSEARCH_URL=http://elasticsearch:9200
      - CAMUNDA_OPERATE_ZEEBEELASTICSEARCH_URL=http://elasticsearch:9200
      - SPRING_PROFILES_ACTIVE=auth
      - CAMUNDA_OPERATE_CSRFPREVENTIONENABLED=false
    depends_on: [zeebe, elasticsearch]
    networks: [camunda]

  tasklist:
    image: camunda/tasklist:8.7.0
    container_name: tasklist
    ports: ["8082:8080"]
    environment:
      - CAMUNDA_TASKLIST_ZEEBE_GATEWAYADDRESS=zeebe:26500
      - CAMUNDA_TASKLIST_ELASTICSEARCH_URL=http://elasticsearch:9200
      - CAMUNDA_TASKLIST_ZEEBEELASTICSEARCH_URL=http://elasticsearch:9200
      - SPRING_PROFILES_ACTIVE=auth
    depends_on: [zeebe, elasticsearch]
    networks: [camunda]

volumes:
  zeebe-data:
  es-data:

networks:
  camunda:
    driver: bridge
```

Start:

```bash
docker compose -f docker-compose-minimal.yml up -d
docker compose -f docker-compose-minimal.yml logs -f zeebe
```

### 3.4 Port Cheat Sheet

| Port | Service | Purpose |
|------|---------|---------|
| 26500 | Zeebe Gateway | gRPC — clients, workers |
| 26501 | Zeebe Broker | Command API (internal) |
| 26502 | Zeebe Broker | Replication / cluster |
| 9600 | Zeebe Broker | Monitoring: `/actuator/health`, `/metrics` |
| 8080 | Zeebe Gateway | REST API |
| 9200 | Elasticsearch | HTTP API |
| 8081 | Operate | Web UI |
| 8082 | Tasklist | Web UI |
| 8083 | Optimize | Web UI |
| 8084 | Identity | Web UI |

### 3.5 Option C: Standalone Broker (no Docker)

```bash
curl -L https://github.com/camunda/zeebe/releases/download/8.7.0/zeebe-distribution-8.7.0.tar.gz \
  | tar -xz
cd zeebe-broker-8.7.0
./bin/broker
```

This gives you a single-node embedded-gateway broker on `localhost:26500`. No Operate, no exporter — just the engine.

### 3.6 Option D: Camunda SaaS

1. Sign up at https://camunda.io.
2. Create a **cluster** (Dev plans are free with limits).
3. In the cluster, create an **API client** with scopes `Zeebe`, `Operate`, `Tasklist`.
4. Download client credentials (`.txt` file).

You'll get:
```
ZEEBE_ADDRESS=<UUID>.bru-2.zeebe.camunda.io:443
ZEEBE_CLIENT_ID=<id>
ZEEBE_CLIENT_SECRET=<secret>
ZEEBE_AUTHORIZATION_SERVER_URL=https://login.cloud.camunda.io/oauth/token
ZEEBE_TOKEN_AUDIENCE=zeebe.camunda.io
```

### 3.7 Option E: Kubernetes (overview)

Camunda ships an official Helm chart:

```bash
helm repo add camunda https://helm.camunda.io
helm repo update

helm install camunda camunda/camunda-platform \
  --namespace camunda --create-namespace \
  --version 11.x.x \
  -f values.yaml
```

See section 9 for production values.

### 3.8 Verifying Zeebe is Healthy

```bash
# Health endpoint
curl http://localhost:9600/actuator/health
# {"status":"UP",...}

# Topology via zbctl
docker run --rm --network host camunda/zeebe:8.7.0 \
  zbctl status --address localhost:26500 --insecure

# Expected output:
# Cluster size: 1
# Partitions count: 4
# Replication factor: 1
# Gateway version: 8.7.0
# Brokers:
#   Broker 0 - 0.0.0.0:26501
#     Version: 8.7.0
#     Partition 1 : Leader, Healthy
#     ...
```

### 3.9 Common Startup Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `bind: address already in use` on 26500 | Old Zeebe running | `docker ps`, stop conflicting container |
| Operate shows "Data not loaded" forever | ES not reachable from Operate | Check both `CAMUNDA_OPERATE_ELASTICSEARCH_URL` and `ZEEBEELASTICSEARCH_URL` point to ES |
| Zeebe restarts repeatedly | Out of disk (snapshot) | `docker system prune`, increase volume |
| `UNAVAILABLE: io exception` from client | Gateway not ready | Wait — Zeebe takes 30-60s to elect leaders |
| Identity not starting | Keycloak slow | Wait 2+ minutes, check `docker logs keycloak` |
| ES "max virtual memory areas vm.max_map_count" | Host kernel setting | `sudo sysctl -w vm.max_map_count=262144` |

---

## 4. Create Your First BPMN Process

We'll model a simple order process:

```
        +-------+    +-----------+    +-----------+    +--------+
   ◯ -->|Validate|-->|  Charge   |--> |  Ship     |--> ◉
        | Order |    |  Payment  |    |  Order    |
        +-------+    +-----------+    +-----------+
```

### 4.1 Using Desktop Modeler

1. Download Camunda Modeler: https://camunda.com/download/modeler/
2. File → New File → BPMN Diagram.
3. Click the start event → change to "Plain". Click the wrench icon → set "ID" to `StartEvent_1`.
4. Append a **Task** (use the rocket icon to append). Change its type to **Service Task** (wrench icon in the toolbar of the task).
5. Set:
   - **Name**: `Validate Order`
   - **Task Definition → Type**: `validate-order`
6. Repeat for `Charge Payment` (type `charge-payment`) and `Ship Order` (type `ship-order`).
7. Add an end event.
8. Click the empty diagram area, set **Process ID** to `order-process` and **Name** to `Order Process`. Mark it as **Executable**.
9. Save as `order-process.bpmn`.

### 4.2 The Resulting BPMN XML

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  id="Definitions_1"
                  targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="order-process" name="Order Process" isExecutable="true">

    <bpmn:startEvent id="StartEvent_1" name="Order received">
      <bpmn:outgoing>Flow_1</bpmn:outgoing>
    </bpmn:startEvent>

    <bpmn:serviceTask id="Task_Validate" name="Validate Order">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="validate-order" retries="3"/>
      </bpmn:extensionElements>
      <bpmn:incoming>Flow_1</bpmn:incoming>
      <bpmn:outgoing>Flow_2</bpmn:outgoing>
    </bpmn:serviceTask>

    <bpmn:serviceTask id="Task_Charge" name="Charge Payment">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="charge-payment" retries="3"/>
      </bpmn:extensionElements>
      <bpmn:incoming>Flow_2</bpmn:incoming>
      <bpmn:outgoing>Flow_3</bpmn:outgoing>
    </bpmn:serviceTask>

    <bpmn:serviceTask id="Task_Ship" name="Ship Order">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="ship-order" retries="3"/>
      </bpmn:extensionElements>
      <bpmn:incoming>Flow_3</bpmn:incoming>
      <bpmn:outgoing>Flow_4</bpmn:outgoing>
    </bpmn:serviceTask>

    <bpmn:endEvent id="EndEvent_1" name="Order completed">
      <bpmn:incoming>Flow_4</bpmn:incoming>
    </bpmn:endEvent>

    <bpmn:sequenceFlow id="Flow_1" sourceRef="StartEvent_1" targetRef="Task_Validate"/>
    <bpmn:sequenceFlow id="Flow_2" sourceRef="Task_Validate" targetRef="Task_Charge"/>
    <bpmn:sequenceFlow id="Flow_3" sourceRef="Task_Charge" targetRef="Task_Ship"/>
    <bpmn:sequenceFlow id="Flow_4" sourceRef="Task_Ship" targetRef="EndEvent_1"/>
  </bpmn:process>
</bpmn:definitions>
```

### 4.3 Key BPMN Element Details

- **`zeebe:taskDefinition type="validate-order"`** — This is the **job type**. Workers subscribe by job type, not by task ID. Multiple service tasks can share a type if they do the same work.
- **`retries="3"`** — How many times Zeebe will re-activate the job after `failJob`. Hitting 0 raises an **incident**.
- **`isExecutable="true"`** — Without this, Zeebe refuses to deploy.
- **`id="order-process"`** — This is the **BPMN process ID**. You'll use it to start instances: `createProcessInstance("order-process", ...)`.

### 4.4 Deploy via zbctl

```bash
zbctl deploy resource order-process.bpmn --address localhost:26500 --insecure
```

### 4.5 Start an Instance via zbctl

```bash
zbctl create instance order-process \
  --variables '{"orderId":"ORD-001","amount":99.99,"customerId":"C-42"}' \
  --address localhost:26500 --insecure
```

### 4.6 View in Operate

Open http://localhost:8081 → "Processes" → click "Order Process". You'll see:

- All running instances (with token positions highlighted).
- An **incident** marker if no worker is connected (after retries exhausted).
- Click any instance → see variables, history, incident detail.

---

## 5. Connect Camunda 8 with Spring Boot

### 5.1 Required Dependencies

Use the **Camunda Spring SDK** (formerly "spring-zeebe"). It auto-configures the client, scans for `@JobWorker`s, and integrates with Spring Boot's actuator/metrics.

### 5.2 Maven (`pom.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.example</groupId>
  <artifactId>order-service</artifactId>
  <version>1.0.0</version>
  <packaging>jar</packaging>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.3.4</version>
    <relativePath/>
  </parent>

  <properties>
    <java.version>21</java.version>
    <camunda.version>8.7.0</camunda.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-validation</artifactId>
    </dependency>

    <!-- Camunda Spring SDK -->
    <dependency>
      <groupId>io.camunda</groupId>
      <artifactId>spring-boot-starter-camunda-sdk</artifactId>
      <version>${camunda.version}</version>
    </dependency>

    <!-- Observability -->
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-prometheus</artifactId>
    </dependency>

    <!-- Testing -->
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>io.camunda</groupId>
      <artifactId>spring-boot-starter-camunda-test</artifactId>
      <version>${camunda.version}</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

### 5.3 Gradle (`build.gradle.kts`)

```kotlin
plugins {
    java
    id("org.springframework.boot") version "3.3.4"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "com.example"
version = "1.0.0"

java {
    toolchain { languageVersion = JavaLanguageVersion.of(21) }
}

repositories { mavenCentral() }

extra["camundaVersion"] = "8.7.0"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("io.camunda:spring-boot-starter-camunda-sdk:${property("camundaVersion")}")
    implementation("io.micrometer:micrometer-registry-prometheus")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.camunda:spring-boot-starter-camunda-test:${property("camundaVersion")}")
}
```

### 5.4 `application.yml` — Local Self-Managed Zeebe

```yaml
spring:
  application:
    name: order-service

camunda:
  client:
    mode: self-managed
    zeebe:
      grpc-address: http://localhost:26500
      rest-address: http://localhost:8080
      audience: zeebe-api
      prefer-rest-over-grpc: false
    worker:
      defaults:
        timeout: PT5M
        max-jobs-active: 32
        poll-interval: PT0.1S
        request-timeout: PT10S
        stream-enabled: true
        stream-timeout: PT8H
    deployment:
      enabled: true
      resources:
        - "classpath:bpmn/*.bpmn"
        - "classpath:dmn/*.dmn"

management:
  endpoints.web.exposure.include: health,info,metrics,prometheus
  endpoint.health.show-details: always
  metrics.tags.application: ${spring.application.name}

logging:
  level:
    io.camunda.zeebe.client: INFO
    io.camunda.zeebe.spring: INFO
```

### 5.5 `application.yml` — Camunda SaaS

```yaml
camunda:
  client:
    mode: saas
    auth:
      client-id: ${ZEEBE_CLIENT_ID}
      client-secret: ${ZEEBE_CLIENT_SECRET}
    cloud:
      cluster-id: ${CAMUNDA_CLUSTER_ID}
      region: ${CAMUNDA_REGION:bru-2}
    worker:
      defaults:
        timeout: PT5M
        max-jobs-active: 32
```

### 5.6 `application.yml` — Self-Managed with OAuth (Keycloak/Identity)

```yaml
camunda:
  client:
    mode: self-managed
    auth:
      issuer: http://localhost:18080/auth/realms/camunda-platform/protocol/openid-connect/token
      client-id: ${ZEEBE_CLIENT_ID}
      client-secret: ${ZEEBE_CLIENT_SECRET}
    zeebe:
      grpc-address: https://zeebe.example.com:26500
      audience: zeebe-api
```

### 5.7 Enabling the Client — Main Class

```java
package com.example.orderservice;

import io.camunda.zeebe.spring.client.annotation.Deployment;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Deployment(resources = "classpath:bpmn/*.bpmn")
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
```

The `@Deployment` annotation triggers automatic deployment of BPMN/DMN files on startup. You can omit it if you set `camunda.client.deployment.resources` in YAML.

### 5.8 Smoke Test

```java
@RestController
@RequiredArgsConstructor
public class HealthController {
    private final io.camunda.zeebe.client.ZeebeClient zeebe;

    @GetMapping("/zeebe/topology")
    public Object topology() {
        return zeebe.newTopologyRequest().send().join();
    }
}
```

Hit `GET /zeebe/topology` — you should see broker info, partitions, leader assignments.

---

## 6. Spring Boot + Zeebe Full Integration

We'll build a complete working example end-to-end: BPMN, controller, workers, error handling, retries, transactions.

### 6.1 Project Structure

```
order-service/
├── pom.xml
├── src/main/java/com/example/orderservice/
│   ├── OrderServiceApplication.java
│   ├── api/
│   │   ├── OrderController.java
│   │   └── dto/StartOrderRequest.java
│   ├── workers/
│   │   ├── OrderValidationWorker.java
│   │   ├── PaymentWorker.java
│   │   └── ShippingWorker.java
│   ├── domain/
│   │   ├── Order.java
│   │   └── OrderRepository.java
│   ├── config/
│   │   └── ZeebeConfig.java
│   └── exceptions/
│       └── ApiExceptionHandler.java
├── src/main/resources/
│   ├── application.yml
│   ├── bpmn/order-process.bpmn
│   └── logback-spring.xml
└── src/test/java/com/example/orderservice/
    └── OrderProcessIntegrationTest.java
```

### 6.2 REST Controller — Starting a Process

```java
package com.example.orderservice.api;

import com.example.orderservice.api.dto.StartOrderRequest;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final ZeebeClient zeebe;

    @PostMapping
    public ResponseEntity<Map<String, Object>> startOrder(@RequestBody StartOrderRequest req) {
        String orderId = "ORD-" + UUID.randomUUID();

        ProcessInstanceEvent event = zeebe.newCreateInstanceCommand()
                .bpmnProcessId("order-process")
                .latestVersion()
                .variables(Map.of(
                        "orderId", orderId,
                        "customerId", req.customerId(),
                        "items", req.items(),
                        "amount", req.amount()
                ))
                // Idempotency: pass a business key so retries don't duplicate
                .withResult()                 // optional: block until end event
                .requestTimeout(java.time.Duration.ofSeconds(30))
                .send()
                .join();

        log.info("Started orderId={} processInstanceKey={}", orderId, event.getProcessInstanceKey());

        return ResponseEntity.accepted().body(Map.of(
                "orderId", orderId,
                "processInstanceKey", event.getProcessInstanceKey(),
                "processDefinitionKey", event.getProcessDefinitionKey()
        ));
    }

    /**
     * Fire-and-forget variant for high throughput — does NOT wait for completion.
     */
    @PostMapping("/async")
    public ResponseEntity<Map<String, Object>> startOrderAsync(@RequestBody StartOrderRequest req) {
        String orderId = "ORD-" + UUID.randomUUID();

        ProcessInstanceEvent event = zeebe.newCreateInstanceCommand()
                .bpmnProcessId("order-process")
                .latestVersion()
                .variables(Map.of(
                        "orderId", orderId,
                        "customerId", req.customerId(),
                        "amount", req.amount()
                ))
                .send()
                .join();

        return ResponseEntity.accepted().body(Map.of("orderId", orderId,
                "processInstanceKey", event.getProcessInstanceKey()));
    }
}
```

### 6.3 The DTO

```java
package com.example.orderservice.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.List;

public record StartOrderRequest(
        @NotBlank String customerId,
        @Positive BigDecimal amount,
        List<String> items
) {}
```

### 6.4 Job Workers

#### 6.4.1 Validation Worker

```java
package com.example.orderservice.workers;

import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;

@Component
@Slf4j
public class OrderValidationWorker {

    @JobWorker(type = "validate-order", autoComplete = true)
    public Map<String, Object> validate(
            final ActivatedJob job,
            @Variable String orderId,
            @Variable String customerId,
            @Variable BigDecimal amount
    ) {
        log.info("Validating orderId={} customer={} amount={}", orderId, customerId, amount);

        if (customerId == null || customerId.isBlank()) {
            // BPMN error -> goes to a BPMN error boundary event if defined
            throw new ZeebeBpmnError("INVALID_ORDER", "customerId is missing", Map.of());
        }
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new ZeebeBpmnError("INVALID_ORDER", "amount must be > 0", Map.of());
        }

        // Returned map becomes process variables
        return Map.of("validated", true, "validationTimestamp", System.currentTimeMillis());
    }
}
```

**Important distinction**:

| You throw / return | What Zeebe does |
|---|---|
| Return a `Map` (autoComplete=true) | `completeJob` with those variables |
| Throw `ZeebeBpmnError("CODE", msg)` | Throws a BPMN error → boundary event |
| Throw any other exception | `failJob` with retries-- |
| Throw and retries reach 0 | **Incident raised** — needs human action in Operate |

#### 6.4.2 Payment Worker (manual completion, idempotency)

```java
package com.example.orderservice.workers;

import io.camunda.zeebe.client.api.command.FailJobCommandStep1;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentWorker {

    private final PaymentGateway paymentGateway;   // your external service client

    @JobWorker(type = "charge-payment", autoComplete = false, timeout = 60_000)
    public void charge(
            final JobClient client,
            final ActivatedJob job,
            @Variable String orderId,
            @Variable BigDecimal amount,
            @Variable(name = "customerId") String customerId
    ) {
        // Idempotency key — same orderId always produces same paymentId
        String idempotencyKey = "PAY-" + orderId;

        try {
            PaymentResult result = paymentGateway.charge(customerId, amount, idempotencyKey);

            client.newCompleteCommand(job.getKey())
                    .variables(Map.of(
                            "paymentId", result.paymentId(),
                            "paymentStatus", result.status()
                    ))
                    .send()
                    .join();

            log.info("Payment OK orderId={} paymentId={}", orderId, result.paymentId());

        } catch (PaymentDeclinedException e) {
            // Business failure -> BPMN error, goes to compensation path
            client.newThrowErrorCommand(job.getKey())
                    .errorCode("PAYMENT_DECLINED")
                    .errorMessage(e.getMessage())
                    .send()
                    .join();

        } catch (TransientPaymentException e) {
            // Transient -> let Zeebe retry with backoff
            client.newFailCommand(job.getKey())
                    .retries(job.getRetries() - 1)
                    .retryBackoff(Duration.ofSeconds(30))
                    .errorMessage("Transient payment error: " + e.getMessage())
                    .send()
                    .join();

        } catch (Exception e) {
            // Unknown -> fail fast, no retry, create incident immediately
            client.newFailCommand(job.getKey())
                    .retries(0)
                    .errorMessage("Unexpected: " + e.getMessage())
                    .send()
                    .join();
        }
    }
}

record PaymentResult(String paymentId, String status) {}

class PaymentDeclinedException extends RuntimeException {
    public PaymentDeclinedException(String msg) { super(msg); }
}
class TransientPaymentException extends RuntimeException {
    public TransientPaymentException(String msg) { super(msg); }
}

interface PaymentGateway {
    PaymentResult charge(String customerId, BigDecimal amount, String idempotencyKey);
}
```

#### 6.4.3 Shipping Worker (with DB transaction)

```java
package com.example.orderservice.workers;

import com.example.orderservice.domain.Order;
import com.example.orderservice.domain.OrderRepository;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.Variable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class ShippingWorker {

    private final OrderRepository orderRepository;
    private final CarrierClient carrier;

    /**
     * IMPORTANT: @Transactional only covers our local DB.
     * The job completion to Zeebe is NOT inside the transaction.
     * If the DB commits but the Zeebe completion fails, the job will
     * be re-activated and we'll run again → must be idempotent.
     */
    @JobWorker(type = "ship-order", autoComplete = true)
    @Transactional
    public Map<String, Object> ship(@Variable String orderId,
                                    @Variable String paymentId) {

        // Idempotency check: maybe we already shipped this order
        Order order = orderRepository.findByOrderId(orderId)
                .orElseGet(() -> orderRepository.save(new Order(orderId, paymentId, "NEW")));

        if ("SHIPPED".equals(order.getStatus())) {
            log.info("Order {} already shipped, returning cached tracking", orderId);
            return Map.of("trackingNumber", order.getTrackingNumber(), "idempotent", true);
        }

        String trackingNumber = carrier.createShipment(orderId);
        order.markShipped(trackingNumber);
        orderRepository.save(order);

        log.info("Shipped orderId={} tracking={}", orderId, trackingNumber);

        return Map.of("trackingNumber", trackingNumber);
    }
}

interface CarrierClient {
    String createShipment(String orderId);
}
```

### 6.5 Transaction Boundary — the Golden Rule

```
       +-----------+   +-----------+    Network
       | Spring TX |   | Job Logic | ---boundary--->  Zeebe
       +-----------+   +-----------+
            ^                                          |
            |        completeJob (separate call)       |
            +------------------------------------------+
```

**You cannot wrap the Zeebe completion in your Spring transaction.** The two-phase commit doesn't exist here. Consequences:

1. **Workers must be idempotent**. The same job may run twice if a worker completes-locally-but-loses-the-network on the way back to Zeebe.
2. **Side effects (DB writes, API calls) must commit before completeJob**. With `autoComplete=true`, the SDK calls `completeJob` *after* your method returns. If your DB tx commits inside the method but completeJob fails, you've already written the DB — that's correct ordering.
3. **Never write to DB *after* completeJob succeeds**. Move it before.
4. **Use natural idempotency keys** (orderId, paymentId) on every external API call.

### 6.6 Configuration — `ZeebeConfig`

Most things are auto-configured. Use a `@Configuration` class only for ObjectMapper tweaks or worker overrides:

```java
package com.example.orderservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.camunda.zeebe.client.api.JsonMapper;
import io.camunda.zeebe.client.impl.ZeebeObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZeebeConfig {

    @Bean
    public JsonMapper zeebeJsonMapper() {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return new ZeebeObjectMapper(mapper);
    }
}
```

### 6.7 Logback Configuration with Correlation IDs

```xml
<!-- src/main/resources/logback-spring.xml -->
<configuration>
  <include resource="org/springframework/boot/logging/logback/defaults.xml"/>
  <property name="LOG_PATTERN"
      value="%d{ISO8601} %-5level [%thread] [orderId=%X{orderId:-},piKey=%X{processInstanceKey:-}] %logger{36} - %msg%n"/>
  <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
    <encoder><pattern>${LOG_PATTERN}</pattern></encoder>
  </appender>
  <root level="INFO"><appender-ref ref="CONSOLE"/></root>
</configuration>
```

Inject MDC in your workers:

```java
import org.slf4j.MDC;

@JobWorker(type = "validate-order")
public Map<String, Object> validate(final ActivatedJob job, @Variable String orderId) {
    MDC.put("orderId", orderId);
    MDC.put("processInstanceKey", String.valueOf(job.getProcessInstanceKey()));
    try {
        // ... business logic
    } finally {
        MDC.clear();
    }
}
```

You can cleanly extract this into an `@Around` AOP advice if you have many workers.

---

## 7. Zeebe Job Workers in Detail

### 7.1 `@JobWorker` Annotation Reference

```java
@JobWorker(
    type        = "validate-order",        // REQUIRED: job type from BPMN
    name        = "order-validator",       // worker name (defaults to method name)
    timeout     = 60_000,                  // ms — lease duration
    pollInterval= 100,                     // ms — how often to poll if streaming off
    maxJobsActive = 32,                    // max jobs held by THIS worker concurrently
    requestTimeout = 10_000,               // ms — server-side long-poll timeout
    autoComplete = true,                   // call completeJob after method returns
    enabled = true,                        // toggleable via property
    fetchVariables = {"orderId","amount"}, // limit payload size — VERY IMPORTANT
    streamEnabled = true,                  // use gRPC streaming (8.5+)
    streamTimeout = 28_800_000,            // 8h — stream lifetime
    tenantIds = {"tenant-a"}               // multi-tenancy
)
public void onJob(...) { ... }
```

### 7.2 Worker Lifecycle

```
[App startup]
     |
     v
[Spring scans @JobWorker beans]
     |
     v
[SDK creates JobWorker instances]
     |
     v
[Worker opens stream / starts polling]
     |
     v
+----> [Receive batch of N jobs] -----+
|                                     |
|                                     v
|                              [Thread pool executes each]
|                                     |
|                                     v
|                          [completeJob / failJob / throwError]
|                                     |
+-------------------------------------+

[App shutdown]
     |
     v
[Worker closes stream → in-flight jobs lease until timeout]
```

### 7.3 Polling vs Streaming

**Polling (pre-8.5)**: client periodically calls `ActivateJobs`. The gateway long-polls (holds the connection for `requestTimeout`) until jobs are available or timeout.

**Streaming (8.5+)**: client opens a long-lived gRPC stream. Zeebe pushes jobs as soon as they're created. **Latency drops from ~100-500ms to single-digit ms**.

Enable globally:
```yaml
camunda.client.worker.defaults.stream-enabled: true
camunda.client.worker.defaults.stream-timeout: PT8H
```

Or per-worker via `@JobWorker(streamEnabled = true)`.

**Caveat**: streaming + polling co-exist. The SDK always also polls (in case stream missed something).

### 7.4 Worker Threads & Concurrency

The Spring SDK uses a Zeebe-managed executor. The relevant knob:

```yaml
camunda:
  client:
    worker:
      threads: 4                  # core thread count of executor
      defaults:
        max-jobs-active: 32       # max concurrent jobs IN FLIGHT for one worker
```

Math:
- **Total concurrent jobs across all workers in this process** ≤ `threads × number_of_workers` (bounded by `max-jobs-active` per worker).
- **Per-worker limit** controls memory/payload pressure for that specific job type.

For **CPU-bound** workers: keep `maxJobsActive` ≤ CPU cores.
For **I/O-bound** workers (HTTP, DB): can go much higher (32, 64, 128).

### 7.5 Backpressure

Zeebe protects itself with **backpressure**: it returns `RESOURCE_EXHAUSTED` when overloaded. The SDK retries automatically with exponential backoff. You'll see:

```
DEBUG io.camunda.zeebe.client - Backpressure, retry in 50ms
```

If you see this constantly, your cluster is under-provisioned (more partitions, more brokers, faster disk).

### 7.6 Job Timeouts

When Zeebe activates a job, it leases it for `timeout` ms. If the worker doesn't complete/fail within that window, Zeebe **re-activates** the job — possibly on a different worker.

**Pick timeouts to be ~2-3× the P99 of your worker latency.**

```
Worker P50 = 200ms, P99 = 2s  →  timeout = 6-10 seconds
Worker P99 = 30s              →  timeout = 90 seconds
Workers calling slow vendor   →  timeout = 5-10 minutes
```

Too short: jobs re-execute while still running → duplicates.
Too long: failed workers leave jobs stuck for the full duration.

### 7.7 Failure Handling Patterns

```java
@JobWorker(type = "charge-payment", autoComplete = false)
public void charge(JobClient client, ActivatedJob job, @Variable String orderId) {
    try {
        doWork();
        client.newCompleteCommand(job.getKey()).send().join();

    } catch (BusinessValidationException e) {
        // Domain rule violation → BPMN error
        client.newThrowErrorCommand(job.getKey())
              .errorCode("VALIDATION_FAILED")
              .errorMessage(e.getMessage())
              .send().join();

    } catch (TransientException e) {
        // Network blip, 503 from vendor → retry with backoff
        client.newFailCommand(job.getKey())
              .retries(job.getRetries() - 1)
              .retryBackoff(java.time.Duration.ofSeconds(
                  (long) Math.pow(2, 3 - job.getRetries()) * 10
              ))
              .errorMessage("Transient: " + e.getMessage())
              .send().join();

    } catch (Throwable e) {
        // Unknown → don't retry, surface as incident immediately
        client.newFailCommand(job.getKey())
              .retries(0)
              .errorMessage("Unexpected: " + e.getMessage())
              .variables(Map.of("lastError", e.getClass().getSimpleName()))
              .send().join();
    }
}
```

### 7.8 Worker Best Practices

1. **Always set `fetchVariables`**. Don't pull the entire process payload — it costs network + memory.
2. **Workers must be idempotent**. Period.
3. **Don't do heavy work in workers**. Workers should be thin orchestrators that call your domain services. Easier to test.
4. **Separate workers by concern**, not by performance. One class = one job type is usually right.
5. **Tune timeouts per worker**, not globally.
6. **Set retries in BPMN, not in code**. Lets ops people tune without redeploy.
7. **Use `@Variable` for binding** — clearer than reading from `job.getVariablesAsMap()`.
8. **Log job key on every entry/exit**. Makes incident debugging trivial.
9. **Never block indefinitely**. Always set HTTP/DB timeouts in your downstream calls.
10. **Don't catch and swallow `InterruptedException`**. Re-throw or set interrupt flag.

---

## 8. Advanced Workflow Features

### 8.1 Service Task (recap)

Standard work delegation. We've covered this exhaustively above.

### 8.2 User Task

For human work via Tasklist:

```xml
<bpmn:userTask id="ReviewOrder" name="Review Order">
  <bpmn:extensionElements>
    <zeebe:userTask />
    <zeebe:assignmentDefinition assignee="alice" candidateGroups="reviewers"/>
    <zeebe:formDefinition formId="order-review-form"/>
  </bpmn:extensionElements>
</bpmn:userTask>
```

In Tasklist, the user sees a form, fills it in, completes → variables go back to the process.

You can also drive user tasks programmatically via the Tasklist API or Zeebe's user task commands (8.5+):

```java
zeebe.newUserTaskCompleteCommand(userTaskKey)
     .variables(Map.of("approved", true))
     .send().join();
```

### 8.3 External Tasks vs Job Workers

In Camunda 8, **everything** is a job worker — there's no separate "external task" concept like C7. The `zeebe:taskDefinition` element makes the task externally fulfillable.

### 8.4 Timer Events

```xml
<!-- Wait 30 minutes -->
<bpmn:intermediateCatchEvent id="Wait">
  <bpmn:timerEventDefinition>
    <bpmn:timeDuration>PT30M</bpmn:timeDuration>
  </bpmn:timerEventDefinition>
</bpmn:intermediateCatchEvent>

<!-- Wait until specific date -->
<bpmn:timerEventDefinition>
  <bpmn:timeDate>2026-12-31T23:59:00Z</bpmn:timeDate>
</bpmn:timerEventDefinition>

<!-- Cron: every Monday 9am -->
<bpmn:timerEventDefinition>
  <bpmn:timeCycle>0 0 9 ? * MON</bpmn:timeCycle>
</bpmn:timerEventDefinition>
```

Timer durations follow **ISO 8601** (`PT30M`, `P1D`, `PT2H30M`).

### 8.5 Message Events — Async Correlation

Pattern: process waits for an external event (Kafka, webhook).

```xml
<bpmn:intermediateCatchEvent id="WaitForPayment">
  <bpmn:messageEventDefinition messageRef="Message_PaymentReceived"/>
</bpmn:intermediateCatchEvent>

<bpmn:message id="Message_PaymentReceived" name="PaymentReceived">
  <bpmn:extensionElements>
    <zeebe:subscription correlationKey="=orderId"/>
  </bpmn:extensionElements>
</bpmn:message>
```

Publish from Spring Boot:

```java
zeebe.newPublishMessageCommand()
     .messageName("PaymentReceived")
     .correlationKey("ORD-001")
     .timeToLive(Duration.ofMinutes(30))
     .variables(Map.of("paymentId", "PAY-99"))
     .messageId("payment-event-uuid-123")   // dedup key
     .send()
     .join();
```

`messageId` deduplicates: if you publish twice with the same id within TTL, the second is ignored. Critical for Kafka at-least-once consumers.

### 8.6 Compensation (Saga)

Compensation reverses completed steps when something fails downstream:

```xml
<bpmn:serviceTask id="ReserveInventory" name="Reserve Inventory">
  <bpmn:extensionElements>
    <zeebe:taskDefinition type="reserve-inventory"/>
  </bpmn:extensionElements>
</bpmn:serviceTask>

<!-- Compensation handler attached as boundary event -->
<bpmn:boundaryEvent id="CompReserveInventory" attachedToRef="ReserveInventory">
  <bpmn:compensateEventDefinition/>
</bpmn:boundaryEvent>

<bpmn:serviceTask id="ReleaseInventory" name="Release Inventory" isForCompensation="true">
  <bpmn:extensionElements>
    <zeebe:taskDefinition type="release-inventory"/>
  </bpmn:extensionElements>
</bpmn:serviceTask>

<bpmn:association associationDirection="One"
                  sourceRef="CompReserveInventory"
                  targetRef="ReleaseInventory"/>

<!-- Trigger compensation later in the flow -->
<bpmn:endEvent id="CompensateEnd">
  <bpmn:compensateEventDefinition/>
</bpmn:endEvent>
```

When the `CompensateEnd` event fires, Zeebe walks back and invokes the compensation handler for every completed activity that has one.

### 8.7 Multi-Instance Activities

Process N items in parallel:

```xml
<bpmn:serviceTask id="ProcessLineItem" name="Process Line Item">
  <bpmn:extensionElements>
    <zeebe:taskDefinition type="process-line-item"/>
    <zeebe:ioMapping>
      <zeebe:input source="=item" target="item"/>
    </zeebe:ioMapping>
  </bpmn:extensionElements>
  <bpmn:multiInstanceLoopCharacteristics isSequential="false">
    <bpmn:extensionElements>
      <zeebe:loopCharacteristics inputCollection="=items"
                                 inputElement="item"
                                 outputCollection="results"
                                 outputElement="=result"/>
    </bpmn:extensionElements>
  </bpmn:multiInstanceLoopCharacteristics>
</bpmn:serviceTask>
```

`isSequential="false"` → all in parallel. `true` → one after another.

### 8.8 Call Activity

Invoke another BPMN process as a sub-process:

```xml
<bpmn:callActivity id="HandlePayment" name="Handle Payment">
  <bpmn:extensionElements>
    <zeebe:calledElement processId="payment-process" propagateAllChildVariables="false"/>
    <zeebe:ioMapping>
      <zeebe:input source="=orderId" target="orderId"/>
      <zeebe:input source="=amount" target="amount"/>
      <zeebe:output source="=paymentId" target="paymentId"/>
    </zeebe:ioMapping>
  </bpmn:extensionElements>
</bpmn:callActivity>
```

Use call activities to:
- Reuse sub-processes across products.
- Enforce separation of concerns (e.g., `payment-process` owned by the payments team).
- Keep diagrams readable.

### 8.9 DMN Integration

DMN models business rules as decision tables. Deploy a `.dmn` file and reference from a Business Rule Task:

```xml
<bpmn:businessRuleTask id="DetermineShippingMethod" name="Determine Shipping">
  <bpmn:extensionElements>
    <zeebe:calledDecision decisionId="shipping-method" resultVariable="shipping"/>
  </bpmn:extensionElements>
</bpmn:businessRuleTask>
```

DMN tables are versioned independently of BPMN — great for letting business analysts tweak rules without code changes.

### 8.10 Connectors

Connectors are pre-built workers maintained by Camunda. They install as a separate runtime (`camunda/connectors` Docker image) and register their job types automatically. Examples:

- **HTTP Connector** (`io.camunda:http-json:1`) — generic REST calls
- **Kafka Producer / Consumer**
- **SendGrid, AWS SQS/SNS/Lambda, Slack, Microsoft Teams**

```xml
<bpmn:serviceTask id="CallRiskApi" name="Call Risk API">
  <bpmn:extensionElements>
    <zeebe:taskDefinition type="io.camunda:http-json:1"/>
    <zeebe:ioMapping>
      <zeebe:input source="POST" target="method"/>
      <zeebe:input source="https://risk-api/check" target="url"/>
      <zeebe:input source="={orderId: orderId, amount: amount}" target="body"/>
      <zeebe:output source="=response.body" target="riskResult"/>
    </zeebe:ioMapping>
  </bpmn:extensionElements>
</bpmn:serviceTask>
```

For 80% of "call this HTTP endpoint" tasks, the HTTP connector replaces a custom worker entirely.

---

## 9. Production Deployment

### 9.1 Kubernetes with Helm

Use the official chart:

```bash
helm repo add camunda https://helm.camunda.io
helm repo update
helm search repo camunda
```

A **production-leaning `values.yaml`** (single-region, 3 brokers, 2 gateways):

```yaml
global:
  identity:
    auth:
      enabled: true
  ingress:
    enabled: true
    className: nginx
    tls: { enabled: true, secretName: camunda-tls }

zeebe:
  enabled: true
  clusterSize: 3
  partitionCount: 6
  replicationFactor: 3
  cpuThreadCount: 4
  ioThreadCount: 4
  pvcSize: 100Gi
  pvcStorageClassName: fast-ssd      # provision NVMe-backed storage class
  resources:
    requests: { cpu: 2,   memory: 4Gi }
    limits:   { cpu: 4,   memory: 8Gi }
  env:
    - name: ZEEBE_BROKER_DATA_LOGSEGMENTSIZE
      value: "128MB"
    - name: ZEEBE_BROKER_DATA_DISKUSAGECOMMANDWATERMARK
      value: "0.85"
    - name: ZEEBE_BROKER_DATA_DISKUSAGEREPLICATIONWATERMARK
      value: "0.87"

zeebeGateway:
  enabled: true
  replicas: 2
  service:
    type: ClusterIP
  ingress:
    grpc: { enabled: true, host: zeebe.example.com }
    rest: { enabled: true, host: zeebe-rest.example.com }
  resources:
    requests: { cpu: 1,   memory: 1Gi }
    limits:   { cpu: 2,   memory: 2Gi }

elasticsearch:
  enabled: true
  replicas: 3
  minimumMasterNodes: 2
  volumeClaimTemplate:
    resources: { requests: { storage: 200Gi } }
    storageClassName: fast-ssd
  esJavaOpts: "-Xms4g -Xmx4g"

operate:
  enabled: true
  replicas: 2
  ingress:
    enabled: true
    host: operate.example.com

tasklist:
  enabled: true
  replicas: 2

optimize:
  enabled: false   # Enterprise

identity:
  enabled: true
  replicas: 2

keycloak:
  enabled: true
  postgresql:
    persistence: { enabled: true, size: 20Gi }

prometheus-servicemonitor:
  enabled: true   # if using kube-prometheus-stack
```

Install:

```bash
helm install camunda camunda/camunda-platform \
  -n camunda --create-namespace \
  -f values.yaml --version 11.x.x
```

### 9.2 Scaling Brokers

**Adding a broker post-deployment is non-trivial** — you cannot change `partitionCount` or `replicationFactor` of an existing cluster. To add capacity:

1. **Scale up the StatefulSet replicas** (`zeebe.clusterSize`). Partitions are redistributed automatically.
2. New brokers receive partition replicas via Raft snapshot transfer (can take time on large clusters).
3. To **change partition count**, you must create a new cluster and migrate.

Plan partition count for a **3-year horizon**.

### 9.3 Resource Tuning

| Config | Default | Production guideline |
|---|---|---|
| `zeebe.broker.threads.cpu` | 2 | = #vCPUs / 2 |
| `zeebe.broker.threads.io` | 2 | = #vCPUs / 2 |
| `zeebe.broker.data.logSegmentSize` | 128MB | 256MB-512MB for high throughput |
| JVM heap | 1G | 4-8G typical; **disk** is more important than heap |
| Disk IOPS | — | **NVMe SSD mandatory**, 10k+ sustained IOPS |
| Disk size | — | log retention × throughput × replication factor (budget 100-500GB) |

**Zeebe is disk-bound**. The single biggest perf factor is disk latency. Spinning disks or network storage (EBS gp2, EFS, NFS) will cripple throughput. Use **local NVMe** or **gp3/io2 with provisioned IOPS**.

### 9.4 Monitoring with Prometheus & Grafana

Zeebe exposes metrics on port `9600` at `/actuator/prometheus`. Scrape config:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: zeebe
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ['zeebe-0:9600','zeebe-1:9600','zeebe-2:9600']
```

Key metrics to alert on:

| Metric | Alert when |
|---|---|
| `zeebe_health` | != 1 |
| `zeebe_partition_role{role="leader"}` | sum != partition_count |
| `zeebe_broker_disk_usage_ratio` | > 0.80 |
| `zeebe_stream_processor_processing_latency` | P99 > 1s |
| `zeebe_backpressure_rejected_total` rate | > 0 sustained |
| `zeebe_election_latency` | > 30s |
| `zeebe_exporter_last_exported_position` lag | > 100k events behind log head |
| `zeebe_job_activated_total` rate | == 0 for an active job type → workers down |

Camunda provides **official Grafana dashboards** at https://github.com/camunda/camunda-platform-helm/tree/main/charts/camunda-platform/test/integration/grafana — import them as a starting point.

### 9.5 Security & TLS

#### gRPC TLS for clients
```yaml
camunda:
  client:
    zeebe:
      grpc-address: https://zeebe.example.com:443
      ca-certificate-path: /etc/zeebe/ca.crt
```

#### Gateway TLS
```yaml
ZEEBE_GATEWAY_SECURITY_ENABLED: "true"
ZEEBE_GATEWAY_SECURITY_CERTIFICATECHAINPATH: /certs/tls.crt
ZEEBE_GATEWAY_SECURITY_PRIVATEKEYPATH: /certs/tls.key
```

#### Authentication via Identity (OIDC)
```yaml
ZEEBE_GATEWAY_SECURITY_AUTHENTICATION_MODE: identity
ZEEBE_GATEWAY_SECURITY_AUTHENTICATION_IDENTITY_ISSUERBACKENDURL: http://keycloak/auth/realms/camunda-platform
ZEEBE_GATEWAY_SECURITY_AUTHENTICATION_IDENTITY_AUDIENCE: zeebe-api
```

Clients then use OAuth client-credentials to obtain bearer tokens.

#### Multi-tenancy
Set `ZEEBE_BROKER_GATEWAY_MULTITENANCY_ENABLED=true`. Each deploy and command then requires a `tenantId`. Identity manages tenant → user mappings.

### 9.6 Backup Strategy

Zeebe 8.5+ has a **built-in backup API** that takes consistent snapshots to S3/GCS/Azure Blob:

```yaml
ZEEBE_BROKER_DATA_BACKUP_STORE: S3
ZEEBE_BROKER_DATA_BACKUP_S3_BUCKETNAME: zeebe-backups-prod
ZEEBE_BROKER_DATA_BACKUP_S3_REGION: eu-central-1
ZEEBE_BROKER_DATA_BACKUP_S3_APIKEY: ...
ZEEBE_BROKER_DATA_BACKUP_S3_APISECRET: ...
```

Trigger a backup via management endpoint:

```bash
curl -X POST http://zeebe:9600/actuator/backups -H "Content-Type: application/json" \
     -d '{"backupId": 20260525120000}'

# Monitor
curl http://zeebe:9600/actuator/backups/20260525120000
```

Backups must include:
- **Zeebe partitions** (via backup API).
- **Elasticsearch** indices (snapshot to same/different bucket).
- **Identity database** (Postgres pg_dump).
- **Keycloak database** (Postgres pg_dump).

### 9.7 Disaster Recovery

**RTO/RPO planning**:

| Scenario | Mitigation |
|---|---|
| Single broker fails | Automatic — Raft re-elects, <30s |
| Whole cluster lost (datacenter outage) | Restore from latest backup → new cluster |
| Region failure | Multi-region active/passive with backup replication |
| Data corruption | Restore to last known-good backup |
| Accidental delete of process | Process definitions are versioned in Zeebe; redeploy |

**Recovery procedure (simplified)**:
1. Spin up new cluster with same `partitionCount` / `replicationFactor`.
2. Stop the new cluster.
3. Restore Zeebe backup to broker disks.
4. Restore Elasticsearch snapshot.
5. Restart cluster. Operate/Tasklist will see the restored state.

**Test your DR plan**. Set up a quarterly fire drill — there is no other way to know it works.

---

## 10. Observability and Troubleshooting

### 10.1 Operate — Your Single Pane of Glass

Use Operate to:
- See running PIs with their token positions.
- View **incidents** — workflows blocked due to errors.
- See variables, decision evaluations, call activity hierarchy.
- **Retry** a failed job (re-activates with `retries = 1`).
- **Resolve** an incident (after fixing root cause).
- **Cancel** a stuck instance.
- **Modify** an instance (move token, add/remove variables) — emergency only.

### 10.2 Log Analysis

Set log levels:

```yaml
logging:
  level:
    io.camunda.zeebe.client: DEBUG       # gRPC details
    io.camunda.zeebe.spring: DEBUG       # SDK lifecycle
    io.grpc.netty.shaded.io.grpc: WARN   # noisy by default
```

What to grep for:

```
"BACKPRESSURE"           — Zeebe rejecting commands, cluster under load
"PARTITION_LEADER_MISMATCH" — temporary, leader changed
"UNAVAILABLE"            — gateway not reachable
"RESOURCE_EXHAUSTED"     — backpressure on this command
"JobWorker.*activated"   — worker is alive and pulling
"Incident raised"        — workflow is stuck
"Failed to process command"  — engine-level error
```

### 10.3 Common Failures

#### "No worker found for job type X"
- Cause: no worker registered with `@JobWorker(type="X")`, OR job type typo in BPMN.
- Fix: Check Operate → Instance → click the stuck task → see the configured type. Match exactly.

#### Workflow stuck on a service task
- Open Operate → click instance → click the task → see retries left.
- If retries = 0: there's an incident. Click "Resolve" after fixing the worker, then "Retry" the job.

#### Worker logs "UNAVAILABLE: io exception"
- Gateway unreachable. Check `kubectl get pods`, network policies, TLS.
- Verify `zbctl status --address ...` from inside the cluster.

#### Workflow completes but variables are wrong
- Add `client.io_format` logging. Use Operate's history view to see the exact `completeJob` payload received.

#### Massive lag in Operate
- Elasticsearch can't keep up. Check ES heap, disk, refresh interval.
- `zeebe_exporter_last_exported_position` tells you how far behind.

#### Partition becomes unhealthy
- Disk pressure: check `zeebe_broker_disk_usage_ratio`. Free space or expand PVC.
- Snapshot stuck: check broker logs for "snapshot directory size".
- Raft leader election storm: check network between brokers.

### 10.4 Debugging Stuck Workflows — Decision Tree

```
Workflow stuck?
│
├── Is there an INCIDENT? (Operate shows red icon)
│   ├── YES → click incident → read message
│   │   ├── "Failed to evaluate expression" → variable missing/wrong type
│   │   ├── "No worker for job type X" → start the worker
│   │   ├── "Failed at task X with error: ..." → application bug, fix and retry
│   │   └── "Retry exhausted" → fix, click Retry in Operate
│   │
│   └── NO → check next
│
├── Is a job ACTIVATED but never completed?
│   ├── Check worker pod logs around activation timestamp
│   ├── Worker might be blocked on a downstream call
│   └── Check timeout — job will be re-activated when lease expires
│
├── Is a MESSAGE waiting?
│   ├── Check the subscription: Operate → instance → message subscriptions
│   ├── Verify correlationKey in published msg matches subscription
│   └── Check TTL didn't expire
│
└── Is a TIMER pending?
    └── Operate shows timer details (next fire time). Check broker clock skew.
```

### 10.5 Useful zbctl Commands

```bash
# Topology
zbctl status --insecure

# Deploy
zbctl deploy resource process.bpmn --insecure

# Create instance
zbctl create instance order-process --variables '{"orderId":"X"}' --insecure

# Publish message
zbctl publish message PaymentReceived --correlationKey "ORD-1" \
    --variables '{"paymentId":"P1"}' --insecure

# Cancel
zbctl cancel instance 2251799813685250 --insecure

# Watch jobs (manual worker for debugging)
zbctl activate jobs payment --maxJobsToActivate 1 --insecure
```

---

## 11. Performance Optimization

### 11.1 Worker Tuning

**Throughput per worker** ≈ `(jobs / sec) = maxJobsActive × (1 / per-job latency)`.

If per-job is 100ms and you want 1000 jobs/s: need `100` concurrent (`maxJobsActive=100`) on a fast network. Distribute across replicas.

**Cardinal rules**:
1. Always set `fetchVariables` — never pull the full process payload.
2. Set worker timeout = 2-3× P99 of the work.
3. For high-throughput types: enable streaming, increase `maxJobsActive`, scale worker replicas horizontally.
4. Keep workers stateless — no in-memory state that breaks horizontal scaling.

### 11.2 Partition Strategy

**Partition count drives throughput.** Each partition handles roughly 100-200 PIs/s for moderate complexity.

| Target throughput | Partitions | Brokers (RF=3) |
|---|---|---|
| < 100 PI/s | 4 | 3 |
| 100-500 PI/s | 8 | 3 |
| 500-2000 PI/s | 16 | 5 |
| 2000-5000 PI/s | 32 | 7-9 |
| > 5000 PI/s | 64+ | 9+ |

**Test your number with a benchmark before going live.**

### 11.3 Throughput Optimization Checklist

- [ ] NVMe disk on Zeebe brokers
- [ ] `partitionCount` matches throughput target
- [ ] Workers stream-enabled (8.5+)
- [ ] `fetchVariables` set on every worker
- [ ] Worker replicas ≥ ceil(target_throughput / per-worker-throughput)
- [ ] Backpressure metric clean
- [ ] No serialization of large variables (keep payloads small)
- [ ] Idempotency layer doesn't hit a single DB row (no hotspots)
- [ ] Elasticsearch keeping up (`exporter_last_exported_position` lag < 10s)

### 11.4 Parallel Processing

For embarrassingly parallel work, use **multi-instance** with `isSequential=false`. Each iteration becomes a separate job, picked up by N workers in parallel. Subject to:
- All variables visible to the parallel branches.
- `outputCollection` aggregates results.

### 11.5 Payload Size

**Zeebe is not a database.** Process variables should be < 1 MB per instance, ideally < 100 KB.

Anti-patterns:
- ❌ Storing PDFs, images, file blobs as variables.
- ❌ Putting full product catalogs in `items`.
- ❌ Keeping 10k-row arrays.

Patterns:
- ✅ Variables store **IDs and references**.
- ✅ Heavy data lives in S3/DB; the process holds the key.
- ✅ Workers fetch what they need by ID.

### 11.6 Benchmarking

Camunda provides `zeebe-benchmark`:

```bash
docker run --rm --network host \
  ghcr.io/camunda-community-hub/zeebe-benchmark:latest \
  --address localhost:26500 \
  --bpmn /workdir/process.bpmn \
  --start-rate 100 \
  --duration 5m
```

Measure:
- **Process instances started per second** (achievable).
- **End-to-end latency** (start → end event).
- **Backpressure rate** (must be 0 or near 0 for sustainable throughput).

---

## 12. Real-World Architecture Example

A microservices-based order processing system using Camunda 8 as orchestrator, Spring Boot for services, Kafka for events, PostgreSQL for service-local state.

### 12.1 System Architecture

```
                       +----------------+
                       |  API Gateway   |
                       +-------+--------+
                               |
                +--------------+----------------+
                |                               |
        +-------v--------+              +-------v-------+
        | Order Service  |              | Customer Svc  |
        | (orchestrator) |              | (REST)        |
        | + Camunda 8 SDK|              +---------------+
        +-------+--------+
                |                                  +--------------+
                |  gRPC                           +-> Inventory   |
                v                                 |  Service     |
   +------------+-------------+                   +--------------+
   |       Camunda 8          |                          ^
   |    (Zeebe cluster)       |   activate jobs   +------+-------+
   +------------+-------------+   <-------------+ | Payment Svc  |
                                                  +--------------+
                |                                         ^
                |  publish events via Kafka exporter      |
                v                                 +-------+-------+
        +-------+---------+                       |  Shipping     |
        |  Apache Kafka   |  <-- async events --+ |  Service      |
        +-----------------+                       +---------------+
                |
                v
        +---------------+
        | Analytics svc |
        +---------------+

Each service has its own PostgreSQL.
```

### 12.2 BPMN: Order Saga

```
○─►[Validate Order]─►[Reserve Inventory]─►[Charge Payment]─►[Ship]─►◉
                            │                    │
                       (boundary)            (boundary)
                            │                    │
                            ▼                    ▼
                    [Release Inventory]◄─[Refund] (compensation chain)
                                    │
                                    ▼
                                   ◉ (terminate end)
```

### 12.3 Saga Pattern with Compensation

The BPMN diagram **is** the saga. When `Charge Payment` throws a BPMN error `PAYMENT_DECLINED`:

1. Boundary error event catches it.
2. Triggers a compensation throw event.
3. Zeebe walks back the completed activities.
4. For each, if a compensation handler is attached, it's invoked.
5. → `Release Inventory` runs.
6. Process ends in error end event.

This is **declarative compensation** — no manual saga orchestrator code.

### 12.4 Distributed Transaction Handling

There is no XA across services. Patterns:

- **Idempotency keys** on every external call: `Idempotency-Key: ORD-001-charge`.
- **Outbox pattern** for "write DB + publish Kafka event atomically": worker writes to local DB + outbox table in same tx; separate publisher reads outbox and publishes to Kafka.
- **Eventual consistency**: Workflow waits for "inventory.reserved" event; Inventory Service publishes after committing locally.

### 12.5 Event-Driven Communication via Kafka

Configure the **Zeebe Kafka exporter** (community) or **Kafka connectors** for outbound events. For inbound: a Spring Boot consumer correlates Kafka messages to Zeebe:

```java
@KafkaListener(topics = "payment-events")
public void onPaymentEvent(PaymentEvent event) {
    zeebe.newPublishMessageCommand()
         .messageName("PaymentSettled")
         .correlationKey(event.orderId())
         .messageId(event.eventId())   // dedup
         .timeToLive(Duration.ofMinutes(10))
         .variables(Map.of("paymentId", event.paymentId(),
                           "status", event.status()))
         .send()
         .join();
}
```

### 12.6 Worker Skeleton — Inventory Service

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryReservationWorker {

    private final InventoryService inventoryService;
    private final KafkaTemplate<String, Object> kafka;

    @JobWorker(type = "reserve-inventory",
               fetchVariables = {"orderId","items"},
               timeout = 30_000)
    @Transactional
    public Map<String, Object> reserve(@Variable String orderId,
                                       @Variable List<LineItem> items) {

        String reservationId = "RES-" + orderId;

        // Idempotent: same orderId always produces same reservation
        Reservation reservation = inventoryService.reserveOrGet(reservationId, items);

        // Outbox event (committed with the transaction)
        kafka.send("inventory-events",
                new InventoryReservedEvent(orderId, reservationId, items));

        return Map.of("reservationId", reservationId,
                      "reservedAt", Instant.now().toString());
    }

    @JobWorker(type = "release-inventory",
               fetchVariables = {"orderId","reservationId"})
    @Transactional
    public void release(@Variable String orderId,
                        @Variable String reservationId) {
        inventoryService.release(reservationId);
        kafka.send("inventory-events", new InventoryReleasedEvent(orderId, reservationId));
    }
}
```

---

## 13. Best Practices

### 13.1 BPMN Modeling Standards

- **One process = one business outcome**. Don't pack multiple end-to-end flows in one diagram.
- **Name tasks as verbs**: "Charge Payment", not "Payment".
- **Use call activities for reuse and team boundaries**.
- **Avoid deep nesting** of sub-processes; flat is readable.
- **Document non-obvious decisions** with text annotations.
- **Always model error paths explicitly** — error boundary events, escalation events.
- **Process IDs are namespaces**: `payments.charge-customer`, `orders.fulfillment`.

### 13.2 Workflow Versioning

Zeebe versions process definitions automatically. When you redeploy with the same `bpmnProcessId`, a new version is created. Strategies:

- **In-flight instances keep their version**. Already-running PIs don't migrate to v2.
- **New instances use latest by default** (`.latestVersion()`). Pin to a specific version with `.version(3)` if needed.
- For breaking changes: **bump the process ID** (`order-process-v2`) instead — keeps semantics explicit.
- Use Operate's "Migration" feature (8.6+) to move running PIs between versions.

### 13.3 Error Handling Patterns

| Error type | Mechanism |
|---|---|
| Transient (network, 5xx) | `failJob` with retries + backoff |
| Permanent business rule violation | `throwError` → BPMN error boundary |
| Bug / unexpected | `failJob` retries=0 → incident → human |
| Process-wide cancel | `cancelProcessInstance` |

**Rule**: differentiate between *retryable* technical failures and *non-retryable* business failures. Use BPMN errors for the latter so the diagram shows the alternative path.

### 13.4 Idempotency

```
Every external side effect MUST be idempotent.
```

Techniques:
- HTTP: `Idempotency-Key` header (Stripe-style).
- DB: `INSERT ... ON CONFLICT DO NOTHING` with natural key.
- Kafka: dedup by `messageId`.
- File ops: deterministic file names, `if not exists` write.

### 13.5 Worker Design

- One method per job type.
- Workers are thin; delegate to `@Service` beans for testability.
- Always log entry/exit with job key + business id.
- Always set `fetchVariables`.
- Always specify `timeout` (don't rely on global default).

### 13.6 Security Recommendations

- TLS everywhere — gateway, broker-to-broker, ES, Operate.
- OAuth2 client credentials for service-to-Zeebe auth.
- Identity-based RBAC: workers get only the scopes they need.
- Multi-tenancy enabled for shared platforms.
- Secrets in Kubernetes Secrets / Vault, never in `application.yml`.
- Image scanning, restricted PodSecurity.

### 13.7 Multi-Environment Setup

```
dev   → embedded Zeebe (testcontainers) for unit/integration tests
local → docker-compose (full stack, 1 partition)
ci    → docker-compose ephemeral
stage → K8s cluster, same shape as prod but smaller (1 broker, 3 partitions)
prod  → K8s, 3+ brokers, 8+ partitions, RF=3, full backups
```

Keep BPMN files **environment-agnostic**. Externalize URLs, secrets via process variables or input mappings driven by environment-specific worker config.

### 13.8 CI/CD Integration

A typical pipeline:

```yaml
# .github/workflows/ci.yml
name: ci
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: 21 }
      - name: Validate BPMN
        run: |
          # Lint BPMN files
          npm install -g bpmnlint
          bpmnlint src/main/resources/bpmn/*.bpmn
      - name: Build & test (uses Zeebe testcontainer)
        run: ./mvnw -B verify
      - name: Build image
        run: docker build -t orderservice:${{ github.sha }} .
      - name: Push & deploy
        run: |
          docker push orderservice:${{ github.sha }}
          # Deploy BPMN to Zeebe (e.g., via zbctl in a job)
          # Or rely on Spring SDK's auto-deployment on pod startup
```

**Process deployment strategy**:
- **Auto-deploy on app startup** (simple, works well for small teams).
- **Explicit deploy step** in CI/CD (more control, audit trail) — use `zbctl deploy resource` or the Zeebe REST API.

---

## 14. Full Sample Project Structure

```
order-service/
├── .github/workflows/ci.yml
├── docker/
│   ├── docker-compose.yml             # full local stack
│   └── docker-compose-minimal.yml     # zeebe+operate only
├── helm/
│   └── values-prod.yaml
├── pom.xml
├── README.md
├── src/
│   ├── main/
│   │   ├── java/com/example/orderservice/
│   │   │   ├── OrderServiceApplication.java
│   │   │   ├── api/
│   │   │   │   ├── OrderController.java
│   │   │   │   ├── MessageController.java     # publish messages from REST/Kafka
│   │   │   │   └── dto/
│   │   │   │       ├── StartOrderRequest.java
│   │   │   │       └── OrderResponse.java
│   │   │   ├── config/
│   │   │   │   ├── ZeebeConfig.java
│   │   │   │   ├── KafkaConfig.java
│   │   │   │   └── SecurityConfig.java
│   │   │   ├── domain/
│   │   │   │   ├── Order.java
│   │   │   │   ├── OrderRepository.java
│   │   │   │   └── OrderStatus.java
│   │   │   ├── workers/
│   │   │   │   ├── OrderValidationWorker.java
│   │   │   │   ├── InventoryReservationWorker.java
│   │   │   │   ├── PaymentWorker.java
│   │   │   │   └── ShippingWorker.java
│   │   │   ├── messaging/
│   │   │   │   └── PaymentEventListener.java   # Kafka → Zeebe message
│   │   │   ├── clients/
│   │   │   │   ├── PaymentGateway.java
│   │   │   │   └── CarrierClient.java
│   │   │   └── exceptions/
│   │   │       ├── ApiExceptionHandler.java
│   │   │       └── BusinessExceptions.java
│   │   └── resources/
│   │       ├── application.yml
│   │       ├── application-local.yml
│   │       ├── application-prod.yml
│   │       ├── logback-spring.xml
│   │       ├── bpmn/
│   │       │   ├── order-process.bpmn
│   │       │   └── payment-subprocess.bpmn
│   │       └── dmn/
│   │           └── shipping-method.dmn
│   └── test/
│       └── java/com/example/orderservice/
│           ├── OrderProcessIntegrationTest.java
│           ├── workers/PaymentWorkerTest.java
│           └── api/OrderControllerTest.java
└── Dockerfile
```

### 14.1 Dockerfile

```dockerfile
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/order-service-*.jar app.jar
EXPOSE 8080
ENV JAVA_OPTS="-XX:+UseG1GC -Xms512m -Xmx1g -XX:MaxRAMPercentage=75"
ENTRYPOINT exec java $JAVA_OPTS -jar /app/app.jar
```

### 14.2 `application-prod.yml`

```yaml
camunda:
  client:
    mode: self-managed
    auth:
      issuer: ${KEYCLOAK_URL}/auth/realms/camunda-platform/protocol/openid-connect/token
      client-id: ${CAMUNDA_CLIENT_ID}
      client-secret: ${CAMUNDA_CLIENT_SECRET}
    zeebe:
      grpc-address: ${ZEEBE_GRPC_URL}
      audience: zeebe-api
    worker:
      threads: 8
      defaults:
        timeout: PT2M
        max-jobs-active: 64
        stream-enabled: true
        stream-timeout: PT8H
        request-timeout: PT10S

spring:
  datasource:
    url: jdbc:postgresql://${DB_HOST}:5432/orders
    username: ${DB_USER}
    password: ${DB_PASSWORD}
  jpa:
    hibernate.ddl-auto: validate
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS}

management:
  endpoints.web.exposure.include: health,prometheus,info
  endpoint.health.probes.enabled: true
  health.livenessstate.enabled: true
  health.readinessstate.enabled: true

logging:
  level:
    com.example: INFO
    io.camunda.zeebe.client: INFO
```

---

## 15. Testing, CI/CD, and Learning Roadmap

### 15.1 Integration Testing with Zeebe Testcontainer

The Camunda test starter spins up an in-memory Zeebe (no Elasticsearch, no Operate) for fast tests:

```java
package com.example.orderservice;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.process.test.api.ZeebeTestEngine;
import io.camunda.zeebe.spring.test.ZeebeSpringTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.Map;

import static io.camunda.zeebe.process.test.assertions.BpmnAssert.assertThat;
import static io.camunda.zeebe.spring.test.ZeebeTestThreadSupport.waitForProcessInstanceCompleted;

@SpringBootTest
@ZeebeSpringTest
class OrderProcessIntegrationTest {

    @Autowired ZeebeClient zeebe;
    @Autowired ZeebeTestEngine engine;

    @Test
    void happyPath_completesOrderProcess() throws Exception {
        ProcessInstanceEvent pi = zeebe.newCreateInstanceCommand()
                .bpmnProcessId("order-process")
                .latestVersion()
                .variables(Map.of(
                        "orderId", "ORD-T1",
                        "customerId", "C-1",
                        "amount", 50.0
                ))
                .send().join();

        // Drive the engine (in-memory) — simulates time passing, workers running
        waitForProcessInstanceCompleted(pi, Duration.ofSeconds(30));

        assertThat(pi).isCompleted()
                      .hasPassedElement("Task_Validate")
                      .hasPassedElement("Task_Charge")
                      .hasPassedElement("Task_Ship")
                      .hasVariableWithValue("paymentStatus", "OK");
    }

    @Test
    void invalidOrder_triggersBpmnError() throws Exception {
        ProcessInstanceEvent pi = zeebe.newCreateInstanceCommand()
                .bpmnProcessId("order-process")
                .latestVersion()
                .variables(Map.of(
                        "orderId", "ORD-T2",
                        "customerId", "",   // invalid
                        "amount", 50.0
                ))
                .send().join();

        engine.waitForIdleState(Duration.ofSeconds(5));

        assertThat(pi).hasPassedElement("Task_Validate")
                      .hasNotPassedElement("Task_Charge");
    }
}
```

### 15.2 Unit Test for a Worker

```java
@ExtendWith(MockitoExtension.class)
class PaymentWorkerTest {

    @Mock PaymentGateway gateway;
    @Mock JobClient jobClient;
    @Mock ActivatedJob job;
    @Mock CompleteJobCommandStep1 completeStep1;
    @Mock FinalCommandStep<Void> finalStep;

    @InjectMocks PaymentWorker worker;

    @Test
    void chargesSuccessfullyAndCompletes() {
        when(job.getKey()).thenReturn(123L);
        when(gateway.charge(eq("C-1"), eq(BigDecimal.TEN), eq("PAY-ORD-1")))
                .thenReturn(new PaymentResult("PAY-99", "OK"));
        when(jobClient.newCompleteCommand(123L)).thenReturn(completeStep1);
        when(completeStep1.variables(anyMap())).thenReturn((CompleteJobCommandStep1) finalStep);
        when(finalStep.send()).thenReturn(CompletableFuture.completedFuture(null).thenApply(x -> null));

        worker.charge(jobClient, job, "ORD-1", BigDecimal.TEN, "C-1");

        verify(jobClient).newCompleteCommand(123L);
        verify(completeStep1).variables(Map.of("paymentId","PAY-99","paymentStatus","OK"));
    }
}
```

### 15.3 Curl Test Sequence

```bash
# 1) Start a workflow
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"C-42","amount":99.99,"items":["A","B"]}'
# {"orderId":"ORD-xxx","processInstanceKey":...}

# 2) Topology check
curl http://localhost:8080/zeebe/topology

# 3) Health
curl http://localhost:8080/actuator/health

# 4) Prometheus metrics
curl http://localhost:8080/actuator/prometheus | grep job_

# 5) Publish a message (using zbctl)
zbctl publish message PaymentReceived \
   --correlationKey ORD-xxx \
   --variables '{"paymentId":"P1","status":"OK"}' \
   --insecure
```

### 15.4 Testing Approach Summary

| Layer | Test type | Tool |
|---|---|---|
| Worker logic | Unit | JUnit 5 + Mockito |
| BPMN flow | Integration | `@ZeebeSpringTest` (in-memory engine) |
| End-to-end | E2E | Real Zeebe via Testcontainers + RestAssured |
| Performance | Benchmark | `zeebe-benchmark` against staging |
| BPMN syntax | Static | `bpmnlint` in CI |
| Contract | Pact / WireMock | Worker ↔ downstream services |

### 15.5 Recommended Learning Roadmap

**Week 1 — Foundations**
- [ ] BPMN basics: events, gateways, tasks. Practice in Modeler.
- [ ] Run the full Docker Compose stack locally.
- [ ] Deploy a 3-task happy-path process with zbctl, no workers.
- [ ] Connect Operate, observe the incident when no workers are running.

**Week 2 — Spring Boot Integration**
- [ ] Wire Camunda Spring SDK into a Spring Boot app.
- [ ] Build one `@JobWorker` end-to-end.
- [ ] REST endpoint to start a PI.
- [ ] Auto-deploy BPMN from classpath.

**Week 3 — Error Handling**
- [ ] BPMN error events.
- [ ] Compensation handlers.
- [ ] Boundary events: timer, error, message.
- [ ] Idempotency in workers with a real DB.

**Week 4 — Advanced BPMN**
- [ ] Multi-instance.
- [ ] Call activity / sub-process.
- [ ] DMN business rules.
- [ ] User tasks via Tasklist.

**Week 5 — Production Concerns**
- [ ] Helm deploy to a real K8s cluster (minikube counts).
- [ ] TLS, OAuth, Identity.
- [ ] Prometheus metrics + Grafana dashboards.
- [ ] Backups & restore drill.

**Week 6 — Real-World Architecture**
- [ ] Build the saga from section 12.
- [ ] Connect Kafka ↔ Zeebe via messages.
- [ ] Run a load test with `zeebe-benchmark`.
- [ ] Tune partitions, workers, observe backpressure.

**Ongoing**
- Read the Zeebe source code for the components you care about.
- Follow the Camunda Forum (forum.camunda.io) — most production issues have been seen.
- Subscribe to the Camunda Community Hub for connectors and tooling.

---

## Appendix A: Quick Reference Cheat Sheet

### Zeebe Client Java API
```java
// Deploy
zeebe.newDeployResourceCommand().addResourceFromClasspath("bpmn/x.bpmn").send().join();

// Create instance
zeebe.newCreateInstanceCommand().bpmnProcessId("x").latestVersion()
     .variables(vars).send().join();

// Publish message
zeebe.newPublishMessageCommand().messageName("M").correlationKey("K")
     .variables(v).messageId("dedup").timeToLive(Duration.ofMinutes(10))
     .send().join();

// Cancel
zeebe.newCancelInstanceCommand(piKey).send().join();

// Set variables
zeebe.newSetVariablesCommand(elementInstanceKey).variables(v).local(false).send().join();

// Resolve incident
zeebe.newResolveIncidentCommand(incidentKey).send().join();

// Update retries
zeebe.newUpdateRetriesCommand(jobKey).retries(3).send().join();
```

### BPMN FEEL Expressions
```
=orderId                              # variable reference
=amount > 100                         # comparison
=if amount > 100 then "HIGH" else "LOW"
={ id: orderId, total: amount * 1.1 } # object construction
=items[type = "BOOK"]                 # filter
=count(items)                         # function
```

### Useful Annotations
```java
@JobWorker(type="x", fetchVariables={"a"}, timeout=60000, autoComplete=true)
@Variable                       // bind variable to parameter
@VariablesAsType                // bind whole payload to POJO
@CustomHeaders                  // bind BPMN task headers
@Deployment(resources="classpath:bpmn/*.bpmn")
```

---

## Appendix B: Glossary

- **Broker** — Zeebe node holding partition replicas and running the stream processor.
- **Gateway** — stateless proxy translating client requests to broker commands.
- **Partition** — independent stream of commands/events; unit of parallelism.
- **Process Definition** — BPMN model; identified by `bpmnProcessId` + version.
- **Process Instance (PI)** — a single execution; identified by `processInstanceKey`.
- **Job** — a unit of work created when a process token reaches a service task; pulled by a worker.
- **Incident** — a workflow that cannot proceed; requires human resolution.
- **Stream Processor** — Zeebe's deterministic command-to-event processor on each partition.
- **Exporter** — pluggable component that streams events to external systems (e.g., Elasticsearch).
- **Variables** — JSON payload attached to a process instance / scope.
- **Correlation Key** — value used to route messages to a specific waiting instance.

---

*End of guide. Build something, break it, fix it, repeat. That's the only way you'll really learn Camunda 8.*
