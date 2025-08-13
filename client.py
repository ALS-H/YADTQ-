# client.py
import uuid, json, time, argparse, os
from kafka import KafkaProducer
from utils import r, init_task, read_task, task_key

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","localhost:9092")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def submit_task(task_name, args, broadcast=False, run_at=None, max_retries=3):
    task_id = str(uuid.uuid4())
    payload = {"task_id": task_id, "task": task_name, "args": args, 
               "meta":{"created_at": time.time()}, "run_at": run_at, "max_retries": max_retries}
    init_task(task_id, payload)
    topic = "broadcast" if broadcast else "tasks"
    producer.send(topic, key=task_id.encode(), value=payload)
    producer.flush()
    print(f"Submitted {'BROADCAST' if broadcast else 'task'} {task_id} -> {topic}")
    return task_id

def query_task(task_id):
    data = read_task(task_id)
    if not data: print("Task not found."); return
    print("Task ID:", task_id)
    print("Status:", data['status'])
    if data['status'] in ("success","failed"):
        print("Result:", data['result'])
    else:
        print("Started at:", data.get('started_at'))
        print("Created at:", data.get('created_at'))

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--submit', nargs=2, metavar=('TASKNAME','ARGS_JSON'))
    parser.add_argument('--broadcast', action='store_true')
    parser.add_argument('--query', metavar='TASK_ID')
    parser.add_argument('--run_at', help='schedule future time in ISO format')
    args = parser.parse_args()

    if args.submit:
        name = args.submit[0]
        args_json = json.loads(args.submit[1])
        submit_task(name, args_json, broadcast=args.broadcast, run_at=args.run_at)
    elif args.query:
        query_task(args.query)
    else:
        parser.print_help()
