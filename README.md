TaskForge

A distributed task execution system built with Python, Redis, and PostgreSQL.

TaskForge simulates how modern backend systems handle asynchronous job processing using a producer-worker architecture with queue-based task distribution and persistent task tracking.

Architecture
Producer -> Redis Queue -> Workers -> PostgreSQL

Features
Asynchronous task execution
Redis-backed message queue
Parallel worker processing
PostgreSQL persistence
Modular worker architecture
Structured logging
Scalable queue-based design
Tech Stack
Technology	Purpose
Python	Core backend logic
Redis	Task queue broker
PostgreSQL	Persistent storage
SQLAlchemy	Database ORM
Asyncio / Threading	Concurrency

Run Locally
# start redis
redis-server

# run worker
python workers/worker.py

# run producer
python producer/producer.py
Concepts Explored
Distributed systems
Queue-based architectures
Concurrent task execution
Worker orchestration
Persistent job tracking
Backend scalability fundamentals
Future Improvements
Docker deployment
Retry & dead-letter queues
FastAPI integration
Monitoring dashboard
Priority scheduling