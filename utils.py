import redis, json, time, os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Redis key helpers
def task_key(task_id): return f"task:{task_id}"
def worker_key(worker_id): return f"worker:{worker_id}"

# Save initial queued state
def init_task(task_id, payload):
    now = time.time()
    r.hset(task_key(task_id), mapping={
        "status": "queued",
        "result": json.dumps(None),
        "task_payload": json.dumps(payload),
        "created_at": now,
        "retries": 0
    })

def read_task(task_id):
    data = r.hgetall(task_key(task_id))
    if not data:
        return None
    return {
        "status": data.get("status"),
        "result": json.loads(data.get("result")) if data.get("result") else None,
        "payload": json.loads(data.get("task_payload")) if data.get("task_payload") else None,
        "created_at": float(data.get("created_at")) if data.get("created_at") else None,
        "started_at": float(data.get("started_at")) if data.get("started_at") else None,
        "finished_at": float(data.get("finished_at")) if data.get("finished_at") else None,
        "worker_id": data.get("worker_id"),
        "retries": int(data.get("retries") or 0)
    }

def heartbeat(worker_id):
    now = time.time()
    r.hset(worker_key(worker_id), mapping={"last_seen": now})
    r.hincrby(worker_key(worker_id), "tasks_executed", 0)
