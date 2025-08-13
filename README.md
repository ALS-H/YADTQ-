# YADTQ: Yet Another Distributed Task Queue

YADTQ is a Python-based **distributed task queue system** with multiple worker nodes, Redis backend, and Kafka broker for message passing. It supports client-side task submission, worker-side execution, fault-tolerant task reassignment, and monitoring.

---

## **Project Overview**

### **Features**
- **Client-side Support**
  - Submit tasks with unique IDs.
  - Query task status and results.
  - Task types: `add`, `sub`, `mul`, `sleep`, `slow_task`.

- **Worker-side Support**
  - Multiple workers in a Kafka consumer group for load balancing.
  - Redis-based result backend storing task status (`queued`, `processing`, `success`, `failed`) and result.
  - Fault-tolerant task claiming: if a worker fails, stale tasks are re-claimed by other workers.
  - Heartbeat mechanism to track active workers.
  - Atomic task claiming using Redis Lua script.

- **Monitoring**
  - `monitor.py` displays:
    - Active/failed workers.
    - Task counts by status.
    - Last seen heartbeat per worker.

---

## **Architecture**
<img width="1662" height="1262" alt="image" src="https://github.com/user-attachments/assets/7ceb26aa-b634-4b45-867f-e5116cc5eeb6" />

## **Running the System**

### 1. Start the Containers
```bash
docker-compose up -d

## *How to run workers (in 3 different terminals)*
export WORKER_ID=worker1
python worker.py

export WORKER_ID=worker2
python worker.py

export WORKER_ID=worker3
python worker.py


# How to run the client tasks
# Add
python client.py --submit add "[1,2,3]"

# Subtract
python client.py --submit sub "[10,4]"

# Multiply
python client.py --submit mul "[2,3,4]"

# Sleep
python client.py --submit sleep "[5]"

# Slow task
python client.py --submit slow_task "[20]"


# Query Task Status
python client.py --query <task_id>

# Example Output
Task ID: 123e4567-e89b-12d3-a456-426614174000
Status: success
Result: 6

# To monitor Worker and Tasks
python monitor.py

# Output 
=== WORKERS ===
worker:worker1 state online last_seen 1755103784 tasks_executed 3
worker:worker2 state online last_seen 1755103783 tasks_executed 2
=== TASKS === {'queued': 0, 'processing': 1, 'success': 4, 'failed': 0}

# *Fault Tolerance / Reclaiming Tasks*
If a worker executing a long task dies, the task remains in processing for CLAIM_STALE_SECONDS.
Other workers can automatically reclaim and execute the task after the timeout.

# Task Backend Structure (Redis)
Example Stored task
{
  "d5750c0e-ed82": {
    "status": "success",
    "result": "3"
  }
}
status: queued, processing, success, or failed.

result: JSON-serialized result.

worker_id: ID of worker processing the task.

started_at / finished_at: timestamps.
