import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
import json
from typing import Optional
import uvicorn
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncpg
from fastapi import FastAPI
from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

SLA_WINDOWS_MINUTES = {"P1": 60, "P2": 240, "P3": 1440}
WARNING_THRESHOLD = 0.75

# ─────────────────────────────────────────────────────
# Connection settings (Phase 0 credentials, docker-compose network)
# ─────────────────────────────────────────────────────

import os

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    f"postgresql://{os.environ.get('POSTGRES_USER', 'crmuser')}:"
    f"{os.environ.get('POSTGRES_PASSWORD', 'crmpassword')}@"
    f"{os.environ.get('POSTGRES_HOST', 'postgres')}:"
    f"{os.environ.get('POSTGRES_PORT', '5432')}/"
    f"{os.environ.get('POSTGRES_DB', 'crmplatform')}"
)

JOBSTORE_URL = POSTGRES_DSN.replace("postgresql://", "postgresql+psycopg2://")

STARTUP_RETRY_ATTEMPTS = int(os.environ.get("STARTUP_RETRY_ATTEMPTS", "10"))
STARTUP_RETRY_DELAY_SECONDS = int(os.environ.get("STARTUP_RETRY_DELAY_SECONDS", "3"))
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


pg_pool: asyncpg.Pool = None
kafka_producer: AIOKafkaProducer = None

def calculate_sla_times(priority: str, created_at: datetime):
    window_minutes = SLA_WINDOWS_MINUTES[priority]

    breach_time = created_at + timedelta(minutes=window_minutes)
    warning_time = created_at + timedelta(minutes=window_minutes * WARNING_THRESHOLD)

    return warning_time, breach_time

def schedule_sla_jobs(scheduler, ticket_id: int, priority: str, created_at: datetime):
    warning_time, breach_time = calculate_sla_times(priority, created_at)

    scheduler.add_job(
        send_warning,
        trigger="date",
        run_date=warning_time,
        args=[ticket_id, priority],
        id=f"sla_warning_{ticket_id}",
        replace_existing=True,
    )
    scheduler.add_job(
        send_breach,
        trigger="date",
        run_date=breach_time,
        args=[ticket_id, priority],
        id=f"sla_breach_{ticket_id}",
        replace_existing=True,
    )

def cancel_sla_jobs(scheduler, ticket_id: int):
    for job_id in (f"sla_warning_{ticket_id}", f"sla_breach_{ticket_id}"):
        try:
            scheduler.remove_job(job_id)
        except JobLookupError:
            pass 

async def consume_tickets(consumer, scheduler):
    async for message in consumer:
        event = json.loads(message.value.decode())
        ticket_id = event["id"]
        status = event["status"]

        if status in ("open", "assigned"):
            created_at = datetime.fromisoformat(event["created_at"])
            schedule_sla_jobs(scheduler, ticket_id, event["priority"], created_at)

        elif status == "resolved":
            cancel_sla_jobs(scheduler, ticket_id)


async def _with_retry(connect_fn, name: str):
    # Dependencies report healthy via Docker healthcheck slightly before
    # they're actually accepting connections (e.g. RabbitMQ bounces its
    # broker app right after boot to apply default user/perms, which can
    # take 15-20s end to end), so the first attempt right after container
    # start can be refused.
    for attempt in range(1, STARTUP_RETRY_ATTEMPTS + 1):
        try:
            return await connect_fn()
        except (OSError, ConnectionError) as exc:
            if attempt == STARTUP_RETRY_ATTEMPTS:
                raise
            print(f"[startup] {name} connect failed ({exc}), retrying "
                  f"({attempt}/{STARTUP_RETRY_ATTEMPTS})...", flush=True)
            await asyncio.sleep(STARTUP_RETRY_DELAY_SECONDS)

async def _start_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    return producer

async def _start_kafka_consumer(*topics: str, group_id: str):
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id
    )
    await consumer.start()
    return consumer

async def _start_scheduler_job_store():
    scheduler = AsyncIOScheduler(jobstores={'default': SQLAlchemyJobStore(url=JOBSTORE_URL)})
    scheduler.start()
    return scheduler

async def send_warning(ticket_id: int, priority: str):
    async with pg_pool.acquire() as conn:
        ticket = await conn.fetchrow(
            "SELECT status FROM tickets WHERE id=$1", ticket_id
        )
    if not ticket or ticket["status"] == "resolved":
        print(f"[SLA WARNING] ticket {ticket_id} already resolved, skipping", flush=True)
        return
    await kafka_producer.send_and_wait(
        "sla-events",
        json.dumps({
            "ticket_id": ticket_id,
            "priority": priority,
            "type": "warning",
            "timestamp": datetime.now(timezone.utc).isoformat()
    }).encode()
    )
    print(f"[SLA WARNING] published for ticket {ticket_id} ({priority})", flush=True)


async def send_breach(ticket_id: int, priority: str):
    async with pg_pool.acquire() as conn:
        ticket = await conn.fetchrow(
            "SELECT status FROM tickets WHERE id=$1", ticket_id
        )
    if not ticket or ticket["status"] == "resolved":
        print(f"[SLA BREACH] ticket {ticket_id} already resolved, skipping", flush=True)
        return
    await kafka_producer.send_and_wait(
        "sla-events",
        json.dumps({
            "ticket_id": ticket_id,
            "priority": priority,
            "type": "breach",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }).encode()
    )
    print(f"[SLA BREACH] published for ticket {ticket_id} ({priority})", flush=True)
    

@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, kafka_producer
    app.state.pg_pool = await _with_retry(
        lambda: asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=10), "postgres"
    )
    app.state.kafka_producer = await _with_retry(
        lambda: _start_kafka_producer(), 
        "kafka"
    )
    pg_pool = app.state.pg_pool
    kafka_producer = app.state.kafka_producer
    app.state.kafka_consumer = await _with_retry(
        lambda: _start_kafka_consumer("tickets", group_id="sla-tracker"), 
        "kafka"
    )
    app.state.scheduler = await _with_retry(
        lambda: _start_scheduler_job_store(), 
        "scheduler"
    )
    app.state.consumer_task = asyncio.create_task(
        consume_tickets(app.state.kafka_consumer, app.state.scheduler)
    )
    yield
    await app.state.pg_pool.close()
    await app.state.kafka_producer.stop()
    await app.state.kafka_consumer.stop()
    app.state.scheduler.shutdown()
    app.state.consumer_task.cancel()

app = FastAPI(title="sla-service", lifespan=lifespan)

@app.get("/health")
def health():
    return {
        "service": "sla-service",
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)