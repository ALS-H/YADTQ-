# worker.py
import threading, time, json, os, uuid
from kafka import KafkaConsumer, KafkaProducer
from utils import r, task_key, heartbeat, init_task

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
WORKER_ID = os.getenv("WORKER_ID", f"worker-{uuid.uuid4().hex[:6]}")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "yadtq-workers")
CLAIM_STALE_SECONDS = 10

# Kafka consumer (shared group for load balancing)
consumer = KafkaConsumer(
    'tasks',
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=CONSUMER_GROUP,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Kafka producer (for re-queue if needed)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Lua script for atomic claim in Redis
CLAIM_LUA = """
local tkey = KEYS[1]
local now = tonumber(ARGV[1])
local timeout = tonumber(ARGV[2])
local wid = ARGV[3]

local status = redis.call('HGET', tkey, 'status')
if not status then return -1 end
if status == 'queued' then
    redis.call('HSET', tkey,'status','processing')
    redis.call('HSET', tkey,'started_at',now)
    redis.call('HSET', tkey,'worker_id',wid)
    return 1
elseif status=='processing' then
    local started = tonumber(redis.call('HGET', tkey,'started_at') or '0')
    if (now-started) > timeout then
        redis.call('HSET', tkey,'worker_id',wid)
        redis.call('HSET', tkey,'started_at',now)
        redis.call('HSET', tkey,'status','processing')
        redis.call('HINCRBY', tkey,'retries',1)
        return 2
    else
        return 0
    end
else
    return 0
end
"""
claim_script = r.register_script(CLAIM_LUA)

def claim_task(task_id):
    now = int(time.time())
    res = claim_script(
        keys=[task_key(task_id)],
        args=[now, CLAIM_STALE_SECONDS, WORKER_ID]   # <- timeout = CLAIM_STALE_SECONDS
    )
    return int(res)


# Define tasks
def execute_task(payload):
    t = payload.get("task")
    args = payload.get("args", [])
    
    if t=="add":
        return sum(args)
    if t=="sub":
        return args[0]-args[1]
    if t=="mul": 
        prod = 1
        for a in args: prod*=a
        return prod
    if t=="sleep": 
        time.sleep(int(args[0]))
        return f"slept {args[0]}s"
    if t=="slow_task":
        seconds = int(args[0])
        print(f"[{WORKER_ID}] Starting slow task for {seconds}s...")
        time.sleep(seconds)
        return f"Finished after {seconds}s"
    
    raise ValueError(f"Unknown task {t}")



# Heartbeat thread
def heartbeat_loop():
    while True:
        heartbeat(WORKER_ID)
        time.sleep(5)

# Main worker loop
def main_loop():
    print(f"Starting worker {WORKER_ID} in group {CONSUMER_GROUP}")
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    for msg in consumer:
        payload = msg.value
        tid = payload.get("task_id")
        if not tid:
            consumer.commit()
            continue

        res = claim_task(tid)
        if res==0:
            print(f"[{WORKER_ID}] Task {tid} not claimable now.")
            consumer.commit()
            continue
        if res==-1:
            r.hset(task_key(tid), mapping={"status":"failed","result":json.dumps("missing metadata")})
            consumer.commit()
            continue

        try:
            result = execute_task(payload)
            r.hset(task_key(tid), mapping={"status":"success","result":json.dumps(result),
                                          "worker_id":WORKER_ID,"finished_at":time.time()})
            print(f"[{WORKER_ID}] Completed {tid} -> {result}")
        except Exception as e:
            r.hset(task_key(tid), mapping={"status":"failed","result":json.dumps(str(e))})
        consumer.commit()

if __name__=="__main__":
    main_loop()
