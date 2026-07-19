import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from openai import AsyncOpenAI
import httpx
import motor.motor_asyncio
import uvicorn
import json
import aiokafka
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import ReturnDocument

import os

# ─────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "crmplatform")

STARTUP_RETRY_ATTEMPTS = int(os.environ.get("STARTUP_RETRY_ATTEMPTS", "10"))
STARTUP_RETRY_DELAY_SECONDS = int(os.environ.get("STARTUP_RETRY_DELAY_SECONDS", "3"))
ASSIGNMENT_SERVICE_URL = os.environ.get("ASSIGNMENT_SERVICE_URL", "http://assignment-service:8000")
API_KEY = os.environ.get("AZURE_OPENAI_API_KEY")
AZURE_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT_NAME = os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
AWAITING_CONFIRMATION_TIMEOUT_MINUTES = 10
MAX_BOT_ATTEMPTS = 3

# ─────────────────────────────────────────────────────
# Domain vocabulary — single source of truth, no bare strings
# scattered through the logic below
# ─────────────────────────────────────────────────────

class ConversationStatus(str, Enum):
    OPEN = "open"
    AWAITING_CONFIRMATION = "awaiting_confirmation"
    BOT_RESOLVED = "bot_resolved"
    ESCALATED = "escalated"
    CLOSED = "closed"


class TicketStatus(str, Enum):
    OPEN = "open"
    ASSIGNED = "assigned"
    RESOLVED = "resolved"


class Sender(str, Enum):
    CUSTOMER = "customer"
    BOT = "bot"


# ─────────────────────────────────────────────────────
# Startup / connections
# ─────────────────────────────────────────────────────

async def _with_retry(connect_fn, name: str):
    # Dependencies report healthy via Docker healthcheck slightly before
    # they're actually accepting connections, so the first attempt right
    # after container start can be refused.
    for attempt in range(1, STARTUP_RETRY_ATTEMPTS + 1):
        try:
            return await connect_fn()
        except (OSError, ConnectionError) as exc:
            if attempt == STARTUP_RETRY_ATTEMPTS:
                raise
            print(f"[startup] {name} connect failed ({exc}), retrying "
                  f"({attempt}/{STARTUP_RETRY_ATTEMPTS})...", flush=True)
            await asyncio.sleep(STARTUP_RETRY_DELAY_SECONDS)


async def _connect_mongo():
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # AsyncIOMotorClient doesn't open a connection on construction —
    # force one now so _with_retry has something real to catch.
    await client.admin.command("ping")
    return client

async def _start_kafka_producer():
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    return producer

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.mongo_client = await _with_retry(lambda: _connect_mongo(), "mongodb")
    app.state.mongo_db = app.state.mongo_client[MONGO_DB_NAME]
    app.state.http_client = httpx.AsyncClient(base_url=ASSIGNMENT_SERVICE_URL, timeout=10.0)
    app.state.kafka_producer = await _with_retry(
        lambda:_start_kafka_producer(), "kafka"
    )
    app.state.azure_open_ai = AsyncOpenAI(
        api_key=API_KEY,
        base_url=AZURE_ENDPOINT
    )
    yield
    app.state.mongo_client.close()
    await app.state.http_client.aclose()
    await app.state.azure_open_ai.close()
    await app.state.kafka_producer.stop()

app = FastAPI(title="Bot Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "bot-service",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────
# Bot logic (Phase 2A stub — see try_resolve)
# ─────────────────────────────────────────────────────

def try_resolve_text(message: str) -> Optional[str]:
    """Phase 2A keyword-match stub. Returns an answer if matched, else
    None. Phase 2B replaces the body of this function only — every
    caller keeps working unchanged, since the contract (str -> answer
    or None) doesn't change."""
    text = message.lower()
    if "track" in text or "where is my order" in text:
        return "You can track your order at track.example.com using your order ID."
    if "refund" in text:
        return "Refunds are processed within 5-7 business days after we receive the returned item."
    return None

SYSTEM_PROMPT = """You are a customer support assistant. You can only answer questions about order tracking and refunds, using exactly these facts:

- Order tracking: customers can track orders at track.example.com using their order ID.
- Refunds: processed within 5-7 business days after the returned item is received.

If the question matches one of these two topics, answer using only the facts above and set resolved=true.
Otherwise set resolved=false and leave answer empty.

Respond ONLY with JSON: {"resolved": true or false, "answer": "..."}"""

CLASSIFICATION_PROMPT = """...

Choose ONLY one of these five categories: billing, refund, technical, delivery, general.

Use "general" ONLY if the conversation genuinely doesn't relate to any of
the other four — don't force billing/refund/technical/delivery onto
unrelated or nonsensical input.

Respond ONLY with JSON: {"category": "billing" or "refund" or "technical" or "delivery" or "general"}"""

async def classify_ticket_type(client: AsyncOpenAI , messages: list) -> str:
    conversation_text = "\n".join(f"{m['sender']}: {m['text']}" for m in messages)
    response = await client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT_NAME,
        messages=[
            {"role": "system", "content": CLASSIFICATION_PROMPT},
            {"role": "user", "content": conversation_text},
        ],
        response_format={"type": "json_object"},
    )
    try:
        result = json.loads(response.choices[0].message.content)
        print(f"[classify_ticket_type] result: {result}")
    except json.JSONDecodeError:
        return None
    return result.get("category")

async def try_resolve(client: AsyncOpenAI, message: str) -> Optional[str]:
    response = await client.chat.completions.create(
        model=AZURE_OPENAI_DEPLOYMENT_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": message},
        ],
        response_format={"type": "json_object"},
    )

    try:
        result = json.loads(response.choices[0].message.content)
    except json.JSONDecodeError:
        return None
    return result.get("answer") if result.get("resolved") else None

# ─────────────────────────────────────────────────────
# Mongo helpers — the repeated "append message" shape, in one place
# ─────────────────────────────────────────────────────

async def _append_message(db, customer_id: int, conversation_id: ObjectId, sender: Sender, text: str, now: datetime, extra_fields: Optional[dict] = None):
    """Pushes one message onto a conversation and always bumps
    updated_at. extra_fields lets a caller set additional fields
    (e.g. status, last_bot_reply_at) in the same atomic update."""
    fields_to_set = {"updated_at": now}
    if extra_fields:
        fields_to_set.update(extra_fields)

    await db.conversations.update_one(
        {"_id": conversation_id},
        {
            "$push": {"messages": {"sender": sender.value, "text": text, "timestamp": now}},
            "$set": fields_to_set,
        },
    )
    await app.state.kafka_producer.send_and_wait(
        "conversations",
        json.dumps({
            "customer_id": customer_id,
            "conversation_id": str(conversation_id),
            "sender": sender.value,
            "text": text,
            "timestamp": now.isoformat(),
        }).encode(),
    )


def _new_conversation_doc(customer_id: int, now: datetime) -> dict:
    return {
        "customer_id": customer_id,
        "channel": "chat",
        "status": ConversationStatus.OPEN.value,
        "ticket_id": None,
        "bot_attempts": 0,
        "last_bot_reply_at": None,
        "messages": [],
        "created_at": now,
        "updated_at": now,
    }


# ─────────────────────────────────────────────────────
# Conversation resolution — decides which document a message
# belongs to. Broken into one function per branch rather than one
# long nested block.
# ─────────────────────────────────────────────────────

class ResolutionAction(str, Enum):
    USE_EXISTING = "use_existing"
    CREATE_NEW = "create_new"
    ALREADY_HANDLED = "already_handled"


@dataclass
class ConversationResolution:
    action: ResolutionAction
    conversation_id: Optional[ObjectId] = None
    response: Optional[dict] = None


async def _find_relevant_conversation(db, customer_id: int, conversation_id: Optional[str]) -> Optional[dict]:
    """Looks up by explicit conversation_id first (if given and valid),
    else falls back to this customer's most recent non-terminal
    conversation."""
    if conversation_id:
        try:
            doc = await db.conversations.find_one({"_id": ObjectId(conversation_id)})
            if doc is not None:
                return doc
        except InvalidId:
            pass  # fall through to customer_id lookup

    return await db.conversations.find_one(
        {
            "customer_id": customer_id,
            "status": {"$in": [
                ConversationStatus.OPEN.value,
                ConversationStatus.ESCALATED.value,
                ConversationStatus.AWAITING_CONFIRMATION.value,
            ]},
        },
        sort=[("created_at", -1)],
    )


async def _handle_awaiting_confirmation(db, doc: dict, now: datetime) -> ConversationResolution:
    """A conversation sitting at awaiting_confirmation either times out
    (implicit satisfaction — no reply within the window, treated as
    bot_resolved) or the customer replied again (not actually solved,
    resume as open)."""
    elapsed_minutes = (now.replace(tzinfo=None) - doc["last_bot_reply_at"]).total_seconds() / 60

    if elapsed_minutes >= AWAITING_CONFIRMATION_TIMEOUT_MINUTES:
        await db.conversations.update_one(
            {"_id": doc["_id"]},
            {"$set": {"status": ConversationStatus.BOT_RESOLVED.value, "updated_at": now}},
        )
        return ConversationResolution(action=ResolutionAction.CREATE_NEW)

    await db.conversations.update_one(
        {"_id": doc["_id"]},
        {"$set": {"status": ConversationStatus.OPEN.value, "updated_at": now}},
    )
    return ConversationResolution(action=ResolutionAction.USE_EXISTING, conversation_id=doc["_id"])


async def _handle_escalated(db, http_client: httpx.AsyncClient, doc: dict, message: str, now: datetime) -> ConversationResolution:
    """A conversation already tied to a ticket. If the ticket is still
    live, don't create a second one — just record the customer's
    message for the agent to see. If the ticket somehow already
    resolved without Mongo hearing about it, self-heal and start
    fresh."""
    response = await http_client.get(f"/tickets/{doc['ticket_id']}")
    response.raise_for_status()
    ticket = response.json()

    if ticket["status"] == TicketStatus.ASSIGNED.value:
        await _append_message(db, doc['customer_id'], doc["_id"], Sender.CUSTOMER, message, now)
        return ConversationResolution(
            action=ResolutionAction.ALREADY_HANDLED,
            response={
                "conversation_id": str(doc["_id"]),
                "reply": f"Your message has been added to ticket #{ticket['id']}. An agent will respond shortly.",
                "status": ConversationStatus.ESCALATED.value,
            },
        )

    # ticket resolved, but Mongo's status was never synced — self-heal
    await db.conversations.update_one(
        {"_id": doc["_id"]},
        {"$set": {"status": ConversationStatus.CLOSED.value, "updated_at": now}},
    )
    return ConversationResolution(action=ResolutionAction.CREATE_NEW)


async def resolve_conversation(db, http_client: httpx.AsyncClient, customer_id: int, conversation_id: Optional[str], message: str, now: datetime) -> ConversationResolution:
    doc = await _find_relevant_conversation(db, customer_id, conversation_id)

    if doc is None:
        return ConversationResolution(action=ResolutionAction.CREATE_NEW)

    status_value = doc["status"]

    if status_value == ConversationStatus.OPEN.value:
        return ConversationResolution(action=ResolutionAction.USE_EXISTING, conversation_id=doc["_id"])

    if status_value == ConversationStatus.AWAITING_CONFIRMATION.value:
        return await _handle_awaiting_confirmation(db, doc, now)

    if status_value == ConversationStatus.ESCALATED.value:
        return await _handle_escalated(db, http_client, doc, message, now)

    # bot_resolved or closed — terminal states, nothing to resume
    return ConversationResolution(action=ResolutionAction.CREATE_NEW)


# ─────────────────────────────────────────────────────
# Escalation — separated out so chat() stays readable
# ─────────────────────────────────────────────────────

async def _escalate(db, http_client: httpx.AsyncClient, conversation_id: ObjectId, ticket_type: str, customer_id: int, now: datetime) -> dict: # For Phase 2B, this will be replaced with a call to the intent classifier to determine the type
    response = await http_client.post(
        "/tickets",
        json={
            "customer_id": customer_id,
            "channel": "chat",
            "type": ticket_type,
            "priority": "P3",
        },
    )
    response.raise_for_status()
    ticket = response.json()

    await db.conversations.update_one(
        {"_id": conversation_id},
        {"$set": {
            "status": ConversationStatus.ESCALATED.value,
            "ticket_id": ticket["id"],
            "updated_at": now,
        }},
    )

    return {
        "conversation_id": str(conversation_id),
        "reply": "Connecting you to an agent...",
        "status": ConversationStatus.ESCALATED.value,
    }


# ─────────────────────────────────────────────────────
# Endpoint — thin, delegates to the pieces above
# ─────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    customer_id: int
    message: str
    conversation_id: Optional[str] = None


@app.post("/chat")
async def chat(payload: ChatRequest):
    db = app.state.mongo_db
    http_client = app.state.http_client
    now = datetime.now(timezone.utc)

    resolution = await resolve_conversation(
        db, http_client, payload.customer_id, payload.conversation_id, payload.message, now
    )

    if resolution.action == ResolutionAction.ALREADY_HANDLED:
        return resolution.response

    if resolution.action == ResolutionAction.CREATE_NEW:
        result = await db.conversations.insert_one(_new_conversation_doc(payload.customer_id, now))
        conversation_id = result.inserted_id
    else:
        conversation_id = resolution.conversation_id

    await _append_message(db, payload.customer_id, conversation_id, Sender.CUSTOMER, payload.message, now)

    answer = await try_resolve(app.state.azure_open_ai, payload.message)

    if answer:
        await _append_message(
            db, payload.customer_id, conversation_id, Sender.BOT, answer, now,
            extra_fields={"status": ConversationStatus.AWAITING_CONFIRMATION.value, "last_bot_reply_at": now},
        )
        return {
            "conversation_id": str(conversation_id),
            "reply": answer,
            "status": ConversationStatus.AWAITING_CONFIRMATION.value,
        }

    updated = await db.conversations.find_one_and_update(
        {"_id": conversation_id},
        {"$inc": {"bot_attempts": 1}},
        return_document=ReturnDocument.AFTER,
    )

    if updated["bot_attempts"] >= MAX_BOT_ATTEMPTS or "agent" in payload.message.lower():
        ticket_type = await classify_ticket_type(app.state.azure_open_ai, updated.get("messages", []))
        return await _escalate(db, http_client, conversation_id, ticket_type, payload.customer_id, now)

    reply = "Could you rephrase that?"
    await _append_message(db, payload.customer_id, conversation_id, Sender.BOT, reply, now)
    return {"conversation_id": str(conversation_id), "reply": reply, "status": ConversationStatus.OPEN.value}

@app.patch("/conversations/{ticket_id}/close")
async def close_conversation(ticket_id: int):
    db = app.state.mongo_db
    now = datetime.now(timezone.utc)
    result = await db.conversations.update_one(
        {"ticket_id": ticket_id},
        {"$set": {"status": ConversationStatus.CLOSED.value, "updated_at": now}},
    )

    if result.matched_count == 0:
        return {"error": "details not found"}

    return {"ticket_id": ticket_id, "status": ConversationStatus.CLOSED.value}

def _serialize(doc: dict) -> dict:
    """Converts ObjectId to str for JSON serialization."""
    serialized = dict(doc)
    serialized["_id"] = str(serialized["_id"])
    return serialized

@app.get("/conversations/{ticket_id}")
async def get_conversation(ticket_id: int):
    db = app.state.mongo_db
    doc = await db.conversations.find_one({"ticket_id": ticket_id})

    if doc is None:
        return {"error": "details not found"}

    return _serialize(doc)
    
@app.get("/customers/{customer_id}/conversations")
async def get_customer_conversations(customer_id: int, exclude_ticket_id: Optional[int] = None, limit: int = 5):
    db = app.state.mongo_db
    query = {"customer_id": customer_id}
    if exclude_ticket_id is not None:
        query["ticket_id"] = {"$ne": exclude_ticket_id}
    conversations = await db.conversations.find(query).sort("created_at", -1).limit(limit).to_list(length=limit)

    return {"customer_id": customer_id, "conversations": [_serialize(c) for c in conversations]}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)