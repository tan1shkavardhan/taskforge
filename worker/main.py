import json
import time
import logging
import os
import signal 
from core.redis_client import get_redis
from core.config import (
    QUEUE_NAME,
    FAILED_QUEUE,
    BASE_RETRY_DELAY
)

from db.database import SessionLocal
from db.models import TaskRecord
from worker.handlers import TASK_HANDLERS

logging.basicConfig(level=logging.INFO)

redis = get_redis()

WORKER_ID = os.getpid()
RUNNING = True

def process_task(task, record, db):

    payload = task.get("payload", {})

    task_type = task["type"]

    logging.info(
        f"[Worker {WORKER_ID}] "
        f"Processing {task['display_id']} "
        f"({task_type})"
    )

    handler = TASK_HANDLERS.get(task_type)

    if not handler:
        raise ValueError(
            f"Unknown task type: {task_type}"
        )

    return handler(
        payload,
        lambda: update_heartbeat(record, db)
    )

def update_heartbeat(record, db):

    record.last_heartbeat = time.time()

    db.commit()

    

    logging.info(
    f"Heartbeat updated for {record.display_id}"
    )

def shutdown_worker(signum, frame):

    global RUNNING

    logging.info(
        f"[Worker {WORKER_ID}] "
        f"Shutdown requested"
    )

    RUNNING = False


signal.signal(
    signal.SIGINT,
    shutdown_worker
)

signal.signal(
    signal.SIGTERM,
    shutdown_worker
)

def worker_loop():

    logging.info(
        f"[Worker {WORKER_ID}] Worker started"
    )

    while RUNNING:

        db = None

        try:

            
            result = redis.brpop(
                QUEUE_NAME,
                timeout=5
            )

            if result is None:
                continue

            _, task_data = result

            task = json.loads(task_data)

            task_id = task["id"]

            display_id = task["display_id"]

            db = SessionLocal()

            record = db.query(TaskRecord).filter(
                TaskRecord.id == task_id
            ).first()

            if not record:
                raise ValueError(
                    f"Missing DB record for {display_id}"
                )

            started_at = time.time()

            # status -> processing
            record.status = "processing"
            record.started_at = started_at
            record.last_heartbeat= started_at

            db.commit()

            redis.set(
                f"task:{task_id}",
                "processing"
            )

            # execute task
            result = process_task(
                task,
                record,
                db)

            completed_at = time.time()

            duration_ms = round(
                (completed_at - started_at) * 1000,
                2
            )

            # update db
            record.status = "done"
            record.result = result
            record.duration_ms = duration_ms
            record.completed_at = completed_at

            db.commit()

            # redis metadata
            redis.set(
                f"task:{task_id}",
                "done"
            )

            redis.set(
                f"task:{task_id}:result",
                json.dumps(result)
            )

            logging.info(
                f"[Worker {WORKER_ID}] "
                f"Completed {display_id} "
                f"in {duration_ms} ms"
            )

        except Exception as e:

            logging.exception(
                f"[Worker {WORKER_ID}] Worker error"
            )

            if 'task' in locals():

                retry_count = task.get(
                    "retry_count",
                    0
                )

                max_retries = task.get(
                    "max_retries",
                    3
                )

                if db is None:
                    db = SessionLocal()

                record = db.query(TaskRecord).filter(
                    TaskRecord.id == task_id
                ).first()

                if record:

                    record.error = str(e)

                    # retry allowed
                    if retry_count < max_retries - 1:

                        task["retry_count"] += 1

                        record.status = "retrying"
                        record.retry_count = task[
                            "retry_count"
                        ]

                        db.commit()

                        redis.set(
                            f"task:{task_id}",
                            "retrying"
                        )

                        backoff_seconds = BASE_RETRY_DELAY * (2 ** retry_count)

                        logging.warning(
                            f"[Worker {WORKER_ID}] "
                            f"Retrying {display_id} "
                            f"in {backoff_seconds} s"
                            f"({task['retry_count']}/"
                            f"{max_retries})"
                        )

                        time.sleep(backoff_seconds)

                        redis.lpush(
                            QUEUE_NAME,
                            json.dumps(task)
                        )

                    else:

                        record.status = "failed"
                        record.retry_count = retry_count

                        db.commit()

                        redis.set(
                            f"task:{task_id}",
                            "failed"
                        )

                        redis.set(
                            f"task:{task_id}:error",
                            str(e)
                        )

                        redis.lpush(
                            FAILED_QUEUE,
                            task_data
                        )

                        logging.error(
                            f"[Worker {WORKER_ID}] "
                            f"{display_id} "
                            f"failed permanently"
                        )
                
                       

        finally:

            if db:
                db.close()

    logging.info(
        f"[Worker {WORKER_ID}] "
        f"Worker stopped gracefully"
    )


if __name__ == "__main__":
    worker_loop()