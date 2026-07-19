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
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status, HTTPException
from pydantic import BaseModel, Field
import httpx

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
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Redis TTL for agent availability (Option 1: no sweep/polling needed).
# SET ... EX writes the key and starts this countdown in one call;
# a heartbeat refreshes it, a missing heartbeat lets it expire on its own.
AGENT_STATUS_TTL_SECONDS = int(os.environ.get("AGENT_STATUS_TTL_SECONDS", "90"))
BOT_SERVICE_URL = os.environ.get("BOT_SERVICE_URL", "http://bot-service:8000")
AWAITING_CONFIRMATION_TIMEOUT_MINUTES = 10


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
    app.state.http_client = httpx.AsyncClient(base_url=BOT_SERVICE_URL, timeout=10.0)
    app.state.kafka_producer = await _with_retry(
        lambda: _start_kafka_producer(), 
        "kafka"
    )
    yield
    await app.state.pg_pool.close()
    await app.state.redis.aclose()
    await app.state.rabbit_channel.close()
    await app.state.rabbit_conn.close()
    await app.state.kafka_producer.stop()

app = FastAPI(title="agent-service", lifespan=lifespan)

@app.get("/health")
def health():
    return {
        "service": "agent-service",
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

async def consume_agent_queue(websocket: WebSocket, agent_id: str):
    queue_name = f"agent.{agent_id}.queue"
    queue = await app.state.rabbit_channel.declare_queue(queue_name, durable=True)
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                ticket = json.loads(message.body.decode())
                await websocket.send_json({"event": "ticket_assigned", "ticket": ticket})


async def heartbeat_loop(agent_id: str, r: redis.Redis):
    while True:
        await asyncio.sleep(AGENT_STATUS_TTL_SECONDS / 2);
        await r.set(f"agent:{agent_id}:status", "available", ex=AGENT_STATUS_TTL_SECONDS)

@app.websocket("/ws/agent/{agent_id}")
async def websocket_endpoint(websocket: WebSocket, agent_id: int):
    await websocket.accept()
    r: redis.Redis = app.state.redis
    pool : asyncpg.Pool = app.state.pg_pool
    # Option 1: TTL instead of a separate last_seen key + sweep.
    # This single call sets status AND starts a 90s countdown.
    async with pool.acquire() as conn:
        agent = await conn.fetchrow(
            "SELECT id, name, status FROM agents WHERE id = $1", agent_id
        )
        if agent is None:
            await websocket.send_json({"error": "Agent not found"})
            await websocket.close()
            return
        if agent["status"] == "offline":
            await conn.execute(
                "UPDATE agents SET status = 'active' WHERE id = $1", agent_id
            )
        
    await r.set(f"agent:{agent_id}:status", "available", ex=AGENT_STATUS_TTL_SECONDS)
    await app.state.kafka_producer.send_and_wait(
        "agent-availability",
        json.dumps({"agent_id": agent_id, "event": "connected", "status": "available", "timestamp": datetime.now(timezone.utc).isoformat()}).encode()
    )
    consumer_task = asyncio.create_task(consume_agent_queue(websocket, agent_id))
    heartbeat_loop_task = asyncio.create_task(heartbeat_loop(agent_id, r))
    try:
        while True:
            message = await websocket.receive_text()
            # Any message counts as a heartbeat — refresh the TTL.
            # Client should send at an interval comfortably under
            # AGENT_STATUS_TTL_SECONDS (e.g. ~30s).
            await r.set(f"agent:{agent_id}:status", "available", ex=AGENT_STATUS_TTL_SECONDS)
    except WebSocketDisconnect:
        print(f"WebSocket connection closed for agent {agent_id}")
        heartbeat_loop_task.cancel()
    except Exception as e:
        print(f"Unexpected error for agent {agent_id}: {e}")
    finally:
        consumer_task.cancel()
        heartbeat_loop_task.cancel()
        # Clean disconnect: delete immediately rather than waiting up
        # to 90s for the TTL to expire naturally — we already know
        # for certain, right now, that this agent is gone.
        await app.state.kafka_producer.send_and_wait(
            "agent-availability",
            json.dumps({"agent_id": agent_id, "event": "disconnected", "status": "unavailable", "timestamp": datetime.now(timezone.utc).isoformat()}).encode()
        )
        await r.delete(f"agent:{agent_id}:status")
        await websocket.close()

@app.post("/agents/{agent_id}/logout")
async def agent_logout(agent_id: int):
    r : redis.Redis = app.state.redis
    pool : asyncpg.Pool = app.state.pg_pool

    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE agents SET status = 'offline' WHERE id = $1", agent_id
        )
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="agent not found")
    await r.delete(f"agent:{agent_id}:status")
    return {"agent_id": agent_id, "status": "offline"} 
    
@app.post("/tickets/{ticket_id}/resolve")
async def resolve_ticket(ticket_id: int):
    pool: asyncpg.Pool = app.state.pg_pool
    r: redis.Redis = app.state.redis

    async with pool.acquire() as conn:
        ticket = await conn.fetchrow(
            """
            UPDATE  tickets set status='resolved' , resolved_at=NOW() WHERE id = $1
            RETURNING id, customer_id, channel, type, priority, status,
                      assigned_agent_id, created_at, resolved_at
            
            """,
            ticket_id,
        )
        if ticket is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ticket not found")
        # Notify the assigned agent if any
        if ticket["assigned_agent_id"] is not None:
            agent_id = ticket["assigned_agent_id"]
            await conn.execute(
                "UPDATE agents SET current_load = GREATEST(current_load - 1, 0) WHERE id = $1", agent_id
            )
            await r.decr(f"agent:{agent_id}:load")
        await app.state.http_client.post(f"/conversations/{ticket_id}/close")
        await app.state.kafka_producer.send_and_wait(
                "tickets",
                json.dumps(dict(ticket), default=str).encode(),
        )
        return dict(ticket)  
if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)