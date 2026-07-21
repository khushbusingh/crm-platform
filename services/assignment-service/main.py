import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from typing import Optional
import uvicorn
import aio_pika
import aiokafka
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
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

STARTUP_RETRY_ATTEMPTS = int(os.environ.get("STARTUP_RETRY_ATTEMPTS", "10"))
STARTUP_RETRY_DELAY_SECONDS = int(os.environ.get("STARTUP_RETRY_DELAY_SECONDS", "3"))

# Priority-based TTL in milliseconds
AGENT_QUEUE_TTL_MS = 30_000

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
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    return producer

async def consume_dead_queue(channel, pg_pool,r):
    dead_queue = await channel.declare_queue(
        "dead.letter.queue", durable=True
    )
    async with dead_queue.iterator() as queue_iter:    
        async for message in queue_iter:
            async with message.process():
                ticket = json.loads(message.body.decode())
                priority = ticket["priority"]
                skill = ticket["type"]
                retry_count = int(message.headers.get("x-retry-count", "0")) + 1
                if retry_count >= 3:
                    print(f"[DLQ] ticket {ticket['id']} exhausted retries, dropping")
                    # message.process() acks it — won't loop back
                    continue
                async with pg_pool.acquire() as conn:
                    candidates = await conn.fetch(
                        "SELECT id FROM agents WHERE $1 = ANY(skills) ORDER BY id",
                        skill,
                    )
                new_agent_id = None
                for row in candidates:
                    candidate_id = row["id"]
                    if candidate_id == ticket["assigned_agent_id"]:
                        continue
                    status = await r.get(f"agent:{candidate_id}:status")
                    load = int(await r.get(f"agent:{candidate_id}:load") or 0)
                    if status == "available" and load < 5:
                        new_agent_id = candidate_id
                        break
                if new_agent_id is None:
                    print(f"[DLQ] No eligible agent for ticket {ticket['id']}, dropping")
                    continue

                await r.decr(f"agent:{ticket['assigned_agent_id']}:load")
                async with pg_pool.acquire() as conn:
                    updated = await conn.fetchrow(
                        """
                        UPDATE tickets SET assigned_agent_id = $1
                        WHERE id = $2
                        RETURNING id, customer_id, channel, type, priority, status,
                                  assigned_agent_id, created_at
                        """,
                        new_agent_id, ticket["id"],
                    )
                await r.incr(f"agent:{new_agent_id}:load")
                queue_name = f"agent.{new_agent_id}.queue"
                await channel.declare_queue(
                    queue_name, 
                    durable=True, 
                    arguments={
                        "x-message-ttl": AGENT_QUEUE_TTL_MS,
                        "x-dead-letter-exchange": "",
                        "x-dead-letter-routing-key": "dead.letter.queue"
                    }
                )
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(dict(updated), default=str).encode(),
                        headers={"x-retry-count": retry_count},
                        content_type="application/json",
                    ),
                    routing_key=queue_name,
                )
                print(f"[DLQ] ticket {ticket['id']} reassigned to agent {new_agent_id} (retry {retry_count})")

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
    app.state.kafka_producer = await _with_retry(
        lambda:_start_kafka_producer(), "kafka"
    )
    dead_letter_task = asyncio.create_task(consume_dead_queue(app.state.rabbit_channel, app.state.pg_pool, app.state.redis) )
    yield
    await app.state.pg_pool.close()
    await app.state.redis.aclose()
    await app.state.rabbit_channel.close()
    await app.state.rabbit_conn.close()
    await app.state.kafka_producer.stop()
    dead_letter_task.cancel()

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


async def get_agent_status(pool, redis_client, agent_id: int):
    try:
        status = await redis_client.get(f"agent:{agent_id}:status")
        load = await redis_client.get(f"agent:{agent_id}:load")

        # Redis responded — trust it completely regardless of value
        # None means key expired (agent disconnected), not Redis down
        # Don't fall through to Postgres in this case
        return status, int(load) if load else 0

    except Exception as e:
        print(f"[fallback] Redis unreachable for agent {agent_id}: {e}", flush=True)

    # Only reaches here if Redis threw an exception (genuinely unreachable)
    async with pool.acquire() as conn:
        agent = await conn.fetchrow(
            "SELECT status, current_load FROM agents WHERE id = $1",
            agent_id,
        )

    if agent is None:
        return None, 0

    return agent["status"], agent["current_load"]

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
    # Step 2 — also publish to Kafka for analytics/monitoring.
    await app.state.kafka_producer.send_and_wait(
        "tickets",
        json.dumps(dict(ticket), default=str).encode(),
    )
    agent_id: Optional[int] = None
    for row in candidates:
        candidate_id = row["id"]
        status, load = await get_agent_status(pool, r, candidate_id)
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
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE agents SET current_load = current_load + 1 WHERE id = $1", agent_id
        )
    try:
        await r.incr(f"agent:{agent_id}:load")
    except Exception as e:
        print(f"Redis incr failed (agent {agent_id}), Postgres is source of truth: {e}")

    # Step 5 — hand the task to the agent's queue. Plain queue, no
    # TTL/priority args yet (that's Phase 3's ack-timeout + DLQ work).
    queue_name = f"agent.{agent_id}.queue"
    await app.state.rabbit_channel.declare_queue(
        queue_name, 
        durable=True, 
        arguments= {
            "x-message-ttl": AGENT_QUEUE_TTL_MS,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": "dead.letter.queue"
        }
    )
    await app.state.rabbit_channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(dict(ticket),default=str).encode(),
            content_type="application/json",
            headers={"x-retry-count": 0},
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