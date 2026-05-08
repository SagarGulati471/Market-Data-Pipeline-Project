# Kafka Connectivity and Architecture (Docker + KRaft Mode)

## 1. Overview

This project uses Apache Kafka deployed via Docker in KRaft (Kafka Raft) mode.
The system involves multiple clients operating in different network contexts:

* A Python-based producer running on the host machine
* A Kafka UI service running inside a Docker container

To support both clients, Kafka is configured with multiple listeners and advertised endpoints.

---

## 2. Kafka Connection Model

Kafka clients do not maintain communication using only the initial connection endpoint. Instead, connection establishment occurs in two phases:

### 2.1 Connection Flow

```
Client → Bootstrap Server → Metadata Response → Broker Address → Reconnect
```

### 2.2 Explanation

1. The client connects to a bootstrap server
2. Kafka returns metadata containing broker addresses
3. The client reconnects using the advertised address

This makes correct configuration of `advertised.listeners` critical.

---

## 3. Multi-Client Setup

This project involves two distinct Kafka clients:

### 3.1 Python Producer (Host Machine)

* Runs outside Docker
* Connects using:

```python
bootstrap_servers = "localhost:9093"
```

---

### 3.2 Kafka UI (Docker Container)

* Runs inside Docker network
* Connects using:

```yaml
kafka:9092
```

---

### 3.3 Network Context Difference

| Environment      | Hostname Resolution    | Correct Endpoint |
| ---------------- | ---------------------- | ---------------- |
| Host Machine     | Cannot resolve `kafka` | `localhost:9093` |
| Docker Container | Can resolve `kafka`    | `kafka:9092`     |

---

## 4. Kafka Listener Configuration

### 4.1 Listeners

```
INTERNAL://0.0.0.0:9092
EXTERNAL://0.0.0.0:9093
CONTROLLER://0.0.0.0:29093
```

These define the interfaces Kafka binds to for incoming connections.

---

### 4.2 Advertised Listeners

```
INTERNAL://kafka:9092
EXTERNAL://localhost:9093
```

These define the addresses Kafka returns to clients during metadata exchange.

---

### 4.3 Listener Roles

| Listener   | Purpose                        | Used By             |
| ---------- | ------------------------------ | ------------------- |
| INTERNAL   | Container-to-container traffic | Kafka UI, services  |
| EXTERNAL   | Host-to-Kafka traffic          | Python producer     |
| CONTROLLER | Kafka internal coordination    | Broker ↔ Controller |

---

## 5. Security Protocol

```
PLAINTEXT
```

All listeners use the PLAINTEXT protocol:

* No encryption
* No authentication
* Suitable for local development environments

---

## 6. KRaft Mode (Kafka without Zookeeper)

This project uses KRaft mode, where Kafka manages metadata internally.

### 6.1 Components

* **Broker**: Handles data read/write operations
* **Controller**: Manages metadata, partitions, and leadership

---

### 6.2 Controller Configuration

```
KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:29093
```

This specifies:

* Node ID: 1
* Controller endpoint: `kafka:29093`

---

## 7. Internal Communication

### 7.1 Inter-broker Communication

```
KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
```

Brokers communicate using the INTERNAL listener (`kafka:9092`).

---

### 7.2 Controller Communication

Broker-to-controller communication occurs via:

```
kafka:29093
```

---

## 8. System Architecture

```
                +------------------------+
                |      Kafka Broker      |
                |                        |
                | INTERNAL : 9092        | ← Docker clients
                | EXTERNAL : 9093        | ← Host applications
                | CONTROLLER: 29093      | ← Internal coordination
                +------------------------+
```

---

## 9. Common Misconfigurations

### 9.1 Incorrect Bootstrap Server

```
localhost:9092  (Incorrect for host clients)
```

---

### 9.2 Using Docker Hostname from Host

```
kafka:9092  (Not resolvable outside Docker)
```

---

### 9.3 Incorrect Controller Port

```
1@kafka:9093  (Incorrect)
1@kafka:29093 (Correct)
```

---

### 9.4 Mixing Listener Contexts

* INTERNAL → Docker-only
* EXTERNAL → Host-only

---

## 10. Summary

| Component       | Endpoint       |
| --------------- | -------------- |
| Python Producer | localhost:9093 |
| Kafka UI        | kafka:9092     |
| Controller      | kafka:29093    |

---

## 5. Final advice (important)

* Yes, use this in your README
* But **read it once and tweak wording slightly**
* That makes it genuinely yours

If you want, I can next:

* convert this into **Mermaid diagrams (GitHub-rendered visuals)**
* or review your full README as if I were an interviewer

That’s where you’ll really level up.
