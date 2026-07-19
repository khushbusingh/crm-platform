from fastapi import FastAPI
import json
from contextlib import asynccontextmanager
import asyncio
import uvicorn
from aiokafka import AIOKafkaConsumer
from httpx import AsyncClient
import aio_pika
import os

STARTUP_RETRY_ATTEMPTS = int(os.environ.get("STARTUP_RETRY_ATTEMPTS", "10"))
STARTUP_RETRY_DELAY_SECONDS = int(os.environ.get("STARTUP_RETRY_DELAY_SECONDS", "3"))
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://crmuser:crmpassword@rabbitmq:5672/")
EMAIL_PROVIDER_URL = os.environ.get("EMAIL_PROVIDER_URL", "https://api.brevo.com") 
BREVO_API_KEY= os.environ.get("BREVO_API_KEY")
BREVO_SENDER_EMAIL= os.environ.get("BREVO_SENDER_EMAIL")
BREVO_SENDER_NAME= os.environ.get("BREVO_SENDER_NAME")
NOTIFICATION_TEST_EMAIL= os.environ.get("NOTIFICATION_TEST_EMAIL")


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


async def _start_kafka_consumer(*topics: str, group_id: str):
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id
    )
    await consumer.start()
    return consumer

async def consume_tickets(consumer, channel):
    async for message in consumer:
        event = json.loads(message.value.decode())
        if event.get("status") == "resolved":
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps({
                        "ticket_id": event["id"],
                        "customer_id": event["customer_id"],
                        "resolved_at": event.get("resolved_at"),
                    }).encode(),
                    content_type="application/json",
                ),
                routing_key="email.queue",
            )
            print(f"[notification] queued email for ticket {event['id']}", flush=True)

async def consume_email_queue(channel, http_client):
    queue = await channel.declare_queue("email.queue", durable=True)
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                event = json.loads(message.body.decode())
                await send_email(http_client, event["ticket_id"])

async def send_email(http_client, ticket_id: int):
    response = await http_client.post(
        "/v3/smtp/email",
        headers={"api-key": BREVO_API_KEY, "content-type": "application/json"},
        json={
            "sender": {"name": BREVO_SENDER_NAME, "email": BREVO_SENDER_EMAIL},
            "to": [{"email": NOTIFICATION_TEST_EMAIL}],
            "subject": f"Your support ticket #{ticket_id} has been resolved",
            "htmlContent": f"<p>Your support request (ticket #{ticket_id}) has been resolved. Thank you for contacting us.</p>",
        },
    )
    if response.status_code == 201:
        print(f"[notification] email sent for ticket {ticket_id}", flush=True)
    else:
        print(f"[notification] email failed for ticket {ticket_id}: {response.status_code} {response.text}", flush=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.kafka_consumer = await _with_retry(
        lambda: _start_kafka_consumer("tickets", group_id="notification-service"), 
        "kafka"
    )
    app.state.rabbit_conn = await _with_retry(
        lambda: aio_pika.connect_robust(RABBITMQ_URL), "rabbitmq"
    )
    app.state.http_client = AsyncClient(base_url=EMAIL_PROVIDER_URL, timeout=10.0)
    app.state.rabbit_channel = await app.state.rabbit_conn.channel()
    app.state.ticket_consumer_task = asyncio.create_task(
        consume_tickets(app.state.kafka_consumer, app.state.rabbit_channel)
    )
    app.state.email_worker_task = asyncio.create_task(
        consume_email_queue(app.state.rabbit_channel, app.state.http_client)
    )
    yield
    await app.state.kafka_consumer.stop()
    await app.state.rabbit_conn.close()
    await app.state.http_client.aclose()
    app.state.ticket_consumer_task.cancel()
    app.state.email_worker_task.cancel()


app = FastAPI(title="notification service", lifespan=lifespan)

@app.get('/health')
def health():
    return {'status': 'ok', 'service': 'notification-service'}

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
