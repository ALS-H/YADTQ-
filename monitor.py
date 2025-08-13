# monitor.py
import time, json
from utils import r, task_key

def list_workers(threshold_seconds=15):
    keys = r.keys("worker:*")
    now = time.time()
    workers = []
    for k in keys:
        info = r.hgetall(k)
        last = float(info.get("last_seen") or 0)
        state = "online" if now - last <= threshold_seconds else "offline"
        workers.append((k, info, state))
    return workers

def count_tasks_by_status():
    # naive scan: acceptable for demo. For large scale use Redis sets or streams.
    keys = r.keys("task:*")
    counts = {"queued":0,"processing":0,"success":0,"failed":0}
    for k in keys:
        st = r.hget(k, "status")
        if st in counts:
            counts[st] += 1
    return counts

if __name__ == "__main__":
    while True:
        w = list_workers()
        stats = count_tasks_by_status()
        print("=== WORKERS ===")
        for key, info, state in w:
            print(key, "state", state, "last_seen", info.get("last_seen"), "tasks_executed", info.get("tasks_executed"))
        print("=== TASKS ===", stats)
        time.sleep(3)
