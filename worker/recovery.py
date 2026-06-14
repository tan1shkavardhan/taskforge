import json
import time
import logging

from core.redis_client import get_redis
from core.config import (
    QUEUE_NAME,
    PROCESSING_TIMEOUT
)

from db.database import SessionLocal
from db.models import TaskRecord

logging.basicConfig(level=logging.INFO)

redis = get_redis()


def recover_stuck_tasks():

    logging.info(
        "Recovery worker started"
    )

    while True:

        db = SessionLocal()

        try:

            now = time.time()

            processing_tasks = db.query(
                TaskRecord
            ).filter(
                TaskRecord.status == "processing"
            ).all()

            for task in processing_tasks:

                if not task.started_at:
                    continue

                elapsed = now - task.started_at

                if elapsed > PROCESSING_TIMEOUT:

                    logging.warning(
                        f"Recovering "
                        f"{task.display_id}"
                    )

                    task_data = {
                        "id": task.id,
                        "display_id": task.display_id,
                        "type": task.type,
                        "payload": task.payload,
                        "retry_count": task.retry_count,
                        "max_retries": task.max_retries,
                        "created_at": task.created_at
                    }

                    # update DB state
                    task.status = "recovering"

                    db.commit()

                    # update redis state
                    redis.set(
                        f"task:{task.id}",
                        "recovering"
                    )

                    # requeue task
                    redis.lpush(
                        QUEUE_NAME,
                        json.dumps(task_data)
                    )

            time.sleep(10)

        except Exception:

            logging.exception(
                "Recovery worker failure"
            )

        finally:

            db.close()


if __name__ == "__main__":
    recover_stuck_tasks()