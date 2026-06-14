from fastapi import FastAPI
from pydantic import BaseModel
import json
import ulid
import logging
import time

from core.redis_client import get_redis
from core.config import (
    QUEUE_NAME,
    MAX_RETRIES
)

from db.database import SessionLocal
from db.models import TaskRecord

logging.basicConfig(level=logging.INFO)

app = FastAPI()

redis = get_redis()


class Task(BaseModel):
    type: str
    payload: dict


@app.get("/")
def root():

    return {
        "message": "TaskForge API running"
    }


@app.get("/health")
def health():

    return {
        "status": "healthy"
    }


@app.post("/task")
def create_task(task: Task):

    task_id = str(ulid.new())

    created_at = time.time()

    display_number = redis.incr(
        "task_display_counter"
    )

    display_id = f"TASK-{display_number:04d}"

    task_data = {
        "id": task_id,
        "display_id": display_id,
        "type": task.type,
        "payload": task.payload,
        "retry_count": 0,
        "max_retries": MAX_RETRIES,
        "created_at": created_at
    }

    db = SessionLocal()

    try:

        record = TaskRecord(
            id=task_id,
            display_id=display_id,
            type=task.type,
            payload=task.payload,
            status="queued",
            retry_count=0,
            max_retries=MAX_RETRIES,
            created_at=created_at
        )

        db.add(record)

        db.commit()

    finally:
        db.close()

    redis.lpush(
        QUEUE_NAME,
        json.dumps(task_data)
    )

    redis.set(
        f"task:{task_id}",
        "queued"
    )

    return {
        "task_id": task_id,
        "display_id": display_id,
        "status": "queued"
    }


@app.get("/task/{task_id}")
def get_task_status(task_id: str):

    db = SessionLocal()

    try:

        record = db.query(TaskRecord).filter(
            TaskRecord.id == task_id
        ).first()

        if not record:

            return {
                "error": "task not found"
            }

        return {
            "task_id": record.id,
            "display_id": record.display_id,
            "type": record.type,
            "status": record.status,
            "payload": record.payload,
            "retry_count": record.retry_count,
            "max_retries": record.max_retries,
            "duration_ms": record.duration_ms,
            "result": record.result,
            "error": record.error,
            "created_at": record.created_at,
            "started_at": record.started_at,
            "completed_at": record.completed_at
        }

    finally:
        db.close()


@app.get("/tasks")
def list_tasks():

    db = SessionLocal()

    try:

        tasks = db.query(TaskRecord).all()

        return [
            {
                "task_id": task.id,
                "display_id": task.display_id,
                "type": task.type,
                "status": task.status,
                "payload": task.payload,
                "retry_count": task.retry_count,
                "max_retries": task.max_retries,
                "duration_ms": task.duration_ms,
                "result": task.result,
                "error": task.error,
                "created_at": task.created_at,
                "started_at": task.started_at,
                "completed_at": task.completed_at
            }
            for task in tasks
        ]

    finally:
        db.close()