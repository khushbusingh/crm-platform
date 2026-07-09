import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from typing import Optional
import uvicorn
import aio_pika
import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

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
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://crmuser:crmpassword@rabbitmq:5672/")

STARTUP_RETRY_ATTEMPTS = int(os.environ.get("STARTUP_RETRY_ATTEMPTS", "10"))
STARTUP_RETRY_DELAY_SECONDS = int(os.environ.get("STARTUP_RETRY_DELAY_SECONDS", "3"))


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pg_pool = await _with_retry(
        lambda: asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=10), "postgres"
    )
    app.state.redis = redis.from_url(REDIS_URL, decode_responses=True)
    app.state.rabbit_conn = await _with_retry(
        lambda: aio_pika.connect_robust(RABBITMQ_URL), "rabbitmq"
    )
    app.state.rabbit_channel = await app.state.rabbit_conn.channel()
    yield
    await app.state.pg_pool.close()
    await app.state.redis.aclose()
    await app.state.rabbit_channel.close()
    await app.state.rabbit_conn.close()


app = FastAPI(title="assignment-service", lifespan=lifespan)

# ─────────────────────────────────────────────────────
# Phase 0: health check.
# Phase 1: ticket creation + synchronous assignment.
# NOTE — scope boundary: no Kafka, no ack-timeout/DLQ, no
# priority queue here yet. Straight-line happy path only.
# See docs/CRM_Execution_Plan.md Phase 1 for what's deferred.
# ─────────────────────────────────────────────────────


@app.get("/health")
def health():
    return {
        "service": "assignment-service",
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


class TicketCreate(BaseModel):
    customer_id: int
    channel: str = Field(pattern="^(chat|phone)$")
    type: str  # matched against agents.skills, e.g. "billing"
    priority: str = Field(default="P3", pattern="^(P1|P2|P3)$")


@app.post("/tickets", status_code=201)
async def create_ticket(payload: TicketCreate):
    pool: asyncpg.Pool = app.state.pg_pool
    r: redis.Redis = app.state.redis

    async with pool.acquire() as conn:
        # Step 1 — INSERT the ticket. It exists and is durable
        # even if nothing below succeeds.
        ticket = await conn.fetchrow(
            """
            INSERT INTO tickets (customer_id, channel, type, priority, status)
            VALUES ($1, $2, $3, $4, 'open')
            RETURNING id, customer_id, channel, type, priority, status,
                      assigned_agent_id, created_at
            """,
            payload.customer_id, payload.channel, payload.type, payload.priority,
        )

        # Step 2 — find agents with the matching skill (Postgres is the
        # only place skills live; Redis only knows availability/load).
        candidates = await conn.fetch(
            "SELECT id FROM agents WHERE $1 = ANY(skills) ORDER BY id",
            payload.type,
        )

    agent_id: Optional[int] = None
    for row in candidates:
        candidate_id = row["id"]
        status = await r.get(f"agent:{candidate_id}:status")
        load = await r.get(f"agent:{candidate_id}:load")
        load = int(load) if load is not None else 0

        # TODO(Phase 1 known gap, see HLD §11.1): this is a read-then-act
        # check, not atomic. Fine for a single-instance Phase 1 demo;
        # revisit with a Lua script before Phase 3 horizontal scaling.
        if status == "available" and load < 5:
            agent_id = candidate_id
            break

    if agent_id is None:
        # No eligible agent right now. Phase 1 scope: leave it open and
        # tell the caller. Queuing/ETA for this case is Phase 3.
        return dict(ticket)

    async with pool.acquire() as conn:
        # Step 3 — the real, durable decision.
        ticket = await conn.fetchrow(
            """
            UPDATE tickets SET assigned_agent_id = $1, status = 'assigned'
            WHERE id = $2
            RETURNING id, customer_id, channel, type, priority, status,
                      assigned_agent_id, created_at
            """,
            agent_id, ticket["id"],
        )

    # Step 4 — atomic increment of the fast-read copy.
    await r.incr(f"agent:{agent_id}:load")

    # Step 5 — hand the task to the agent's queue. Plain queue, no
    # TTL/priority args yet (that's Phase 3's ack-timeout + DLQ work).
    queue_name = f"agent.{agent_id}.queue"
    await app.state.rabbit_channel.declare_queue(queue_name, durable=True)
    await app.state.rabbit_channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(dict(ticket),default=str).encode(),
            content_type="application/json",
        ),
        routing_key=queue_name,
    )

    return dict(ticket)


@app.get("/tickets/{ticket_id}")
async def get_ticket(ticket_id: int):
    pool: asyncpg.Pool = app.state.pg_pool
    async with pool.acquire() as conn:
        ticket = await conn.fetchrow(
            "SELECT id, customer_id, channel, type, priority, status, "
            "assigned_agent_id, created_at, resolved_at FROM tickets WHERE id = $1",
            ticket_id,
        )
    if ticket is None:
        raise HTTPException(status_code=404, detail="ticket not found")
    return dict(ticket)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)