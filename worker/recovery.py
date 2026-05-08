import json
import time
import logging

from core.redis_client import get_redis
from db.database import SessionLocal
from db.models import TaskRecord

logging.basicConfig(level=logging.INFO)

redis = get_redis()

QUEUE_NAME = "task_queue"

PROCESSING_TIMEOUT = 30  # seconds


def recover_stuck_tasks():

    while True:

        db = SessionLocal()

        try:
            now = time.time()

            processing_tasks = db.query(TaskRecord).filter(
                TaskRecord.status == "processing"
            ).all()

            for task in processing_tasks:

                if not task.started_at:
                    continue

                elapsed = now - task.started_at

                if elapsed > PROCESSING_TIMEOUT:

                    logging.warning(
                        f"Recovering stuck task "
                        f"{task.display_id}"
                    )

                    # rebuild task payload
                    task_data = {
                        "id": task.id,
                        "display_id": task.display_id,
                        "type": task.type,
                        "payload": {},
                        "retry_count": task.retry_count,
                        "max_retries": task.max_retries
                    }

                    # update state
                    task.status = "queued"

                    db.commit()

                    # requeue
                    redis.lpush(
                        QUEUE_NAME,
                        json.dumps(task_data)
                    )

            time.sleep(10)

        finally:
            db.close()


if __name__ == "__main__":
    logging.info("Recovery worker started")

    recover_stuck_tasks()