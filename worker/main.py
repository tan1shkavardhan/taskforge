import json
import time
import logging
import os

from core.redis_client import get_redis
from core.config import (
    QUEUE_NAME,
    FAILED_QUEUE
)

from db.database import SessionLocal
from db.models import TaskRecord

logging.basicConfig(level=logging.INFO)

redis = get_redis()

WORKER_ID = os.getpid()


def process_task(task,record,db):

    payload = task.get("payload", {})

    logging.info(
        f"[Worker {WORKER_ID}] "
        f"Processing {task['display_id']} "
        f"({task['type']})"
    )

    if task["type"] == "print":

        return {
            "output": payload
        }

    elif task["type"] == "sleep":

        seconds = payload.get("seconds", 1)

        if not isinstance(seconds, (int, float)):
            raise ValueError(
                "seconds must be numeric"
            )

        remaining = seconds

        while remaining > 0:
            sleep_time = min(5,remaining)
            time.sleep(sleep_time)
            remaining -= sleep_time
            update_heartbeat(record,db)

        return {
            "slept_for": seconds
        }

    else:
        raise ValueError(
            f"Unknown task type: {task['type']}"
        )

def update_heartbeat(record, db):

    record.last_heartbeat = time.time()

    db.commit()

    logging.debug(
        f"Heartbeat updates for"
        f"{record.display_id}"
    )

    logging.info(
    f"Heartbeat updated for {record.display_id}"
    )

def worker_loop():

    logging.info(
        f"[Worker {WORKER_ID}] Worker started"
    )

    while True:

        db = None

        try:

            _, task_data = redis.brpop(
                QUEUE_NAME
            )

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

                        logging.warning(
                            f"[Worker {WORKER_ID}] "
                            f"Retrying {display_id} "
                            f"({task['retry_count']}/"
                            f"{max_retries})"
                        )

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


if __name__ == "__main__":
    worker_loop()