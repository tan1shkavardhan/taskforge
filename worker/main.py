import json
import time
import logging
import os

from core.redis_client import get_redis
from db.database import SessionLocal
from db.models import TaskRecord

logging.basicConfig(level=logging.INFO)

redis = get_redis()

QUEUE_NAME = "task_queue"
FAILED_QUEUE = "failed_queue"

WORKER_ID = os.getpid()


def process_task(task):
    """
    Execute task logic based on task type.
    """

    logging.info(
        f"[Worker {WORKER_ID}] "
        f"Processing {task['display_id']} ({task['type']})"
    )

    payload = task.get("payload", {})

    if task["type"] == "print":
        return {
            "output": payload
        }

    elif task["type"] == "sleep":

        seconds = payload.get("seconds", 1)

        if not isinstance(seconds, (int, float)):
            raise ValueError("seconds must be numeric")

        time.sleep(seconds)

        return {
            "slept_for": seconds
        }

    else:
        raise ValueError(
            f"Unknown task type: {task['type']}"
        )


def worker_loop():

    logging.info(
        f"[Worker {WORKER_ID}] "
        f"Worker started (Memurai connected)"
    )

    while True:

        db = None

        try:
            # Wait for next task
            _, task_data = redis.brpop(QUEUE_NAME)

            # Deserialize task
            task = json.loads(task_data)

            task_id = task["id"]
            display_id = task["display_id"]

            # Create DB session
            db = SessionLocal()

            # Fetch DB record
            record = db.query(TaskRecord).filter(
                TaskRecord.id == task_id
            ).first()

            if not record:
                raise ValueError(
                    f"Task record not found for {display_id}"
                )

            # Update status -> processing
            started_at = time.time()

            redis.set(f"task:{task_id}", "processing")

            record.status = "processing"
            record.started_at = started_at

            db.commit()

            

            # Execute task
            result = process_task(task)

            # End timing
            completed_at = time.time()

            duration_ms = round(
                (completed_at - started_at) * 1000,
                2
            )

            # Store Redis metadata
            redis.set(
                f"task:{task_id}:result",
                json.dumps(result)
            )

            redis.set(
                f"task:{task_id}:duration_ms",
                duration_ms
            )

            redis.set(
                f"task:{task_id}:completed_at",
                completed_at
            )

            # Update final Redis status
            redis.set(
                f"task:{task_id}",
                "done"
            )

            # Update PostgreSQL record
            record.status = "done"
            record.result = result
            record.duration_ms = duration_ms
            record.completed_at = completed_at

            db.commit()

            logging.info(
                f"[Worker {WORKER_ID}] "
                f"Completed {display_id} "
                f"in {duration_ms} ms"
            )

        except Exception as e:

            if 'task' in locals():

                task_id = task["id"]
                display_id = task["display_id"]

                retry_count = task.get("retry_count", 0)
                max_retries = task.get("max_retries", 3)

                # Store Redis error
                redis.set(
                    f"task:{task_id}:error",
                    str(e)
                )

                # Ensure DB session exists
                if db is None:
                    db = SessionLocal()

                # Fetch DB record
                record = db.query(TaskRecord).filter(
                    TaskRecord.id == task_id
                ).first()

                if record:

                    record.error = str(e)

                    logging.error(
                        f"[Worker {WORKER_ID}] "
                        f"Task {display_id} failed "
                        f"(attempt {retry_count + 1}/{max_retries}): {e}"
                    )

                    # Retry logic
                    if retry_count < max_retries - 1:

                        # Increment retry count
                        task["retry_count"] = retry_count + 1

                        # Redis state
                        redis.set(
                            f"task:{task_id}",
                            "retrying"
                        )

                        # PostgreSQL state
                        record.status = "retrying"
                        record.retry_count = task["retry_count"]

                        db.commit()

                        logging.info(
                            f"[Worker {WORKER_ID}] "
                            f"Retrying {display_id} "
                            f"({task['retry_count']}/{max_retries})"
                        )

                        # Requeue task
                        redis.lpush(
                            QUEUE_NAME,
                            json.dumps(task)
                        )

                    else:
                        # Permanent failure

                        redis.set(
                            f"task:{task_id}",
                            "failed"
                        )

                        redis.lpush(
                            FAILED_QUEUE,
                            task_data
                        )

                        # PostgreSQL state
                        record.status = "failed"
                        record.retry_count = retry_count

                        db.commit()

                        logging.error(
                            f"[Worker {WORKER_ID}] "
                            f"Task {display_id} failed permanently"
                        )

            else:
                logging.exception(
                    f"[Worker {WORKER_ID}] "
                    f"Unhandled worker exception"
                )

        finally:
            if db:
                db.close()


if __name__ == "__main__":
    worker_loop()