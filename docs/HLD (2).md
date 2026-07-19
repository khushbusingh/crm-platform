# CRM Platform — High-Level Design (HLD)

**Author:** Khushbu Kumari
**Version:** 1.0
**Scope:** Omnichannel customer-support CRM (voice + chat), bot-first, agent-assisted, with downstream business-action integration.

---

## 1. Problem Statement

Build a customer-support platform where a customer reaches out over **phone or chat**, a **bot attempts resolution first**, and unresolved interactions are **routed to the right agent** based on skill, availability, and priority. Agents resolve tickets by taking **real business actions** (order status, refund, cancel, exchange) and the customer is **notified across channels** (email / WhatsApp / SMS). Start at **20 agents**, design so the **same architecture scales to 1000+**.

---

## 2. Functional Requirements (FR)

| ID | Requirement |
|----|-------------|
| FR-1 | Customer can initiate support via **phone call** or **chat** |
| FR-2 | A **bot responds first** on both channels and attempts resolution using a knowledge base |
| FR-3 | Bot **escalates to a human agent** after N failed attempts or on explicit customer request |
| FR-4 | System **auto-assigns** the ticket to an agent based on **skill + availability + priority** |
| FR-5 | Agents have **roles/profiles** (L1, L2, Supervisor, Admin) that gate which tickets they can handle |
| FR-6 | Agent sees assigned tickets and **live conversation** on a **real-time dashboard** |
| FR-7 | Agent can take **business actions**: check order, track, cancel, refund, return, exchange, view customer history |
| FR-8 | If an agent does not **acknowledge** within a timeout, the ticket is **reassigned** |
| FR-9 | If **no agent** is available, ticket is **queued** and customer is given an ETA |
| FR-10 | **P1 (urgent)** tickets are always handled before P2/P3 |
| FR-11 | On resolution, customer is **notified** via email, WhatsApp, and SMS |
| FR-12 | Every interaction and action is **audited** (who did what, when) |
| FR-13 | **SLA per priority** is tracked; warnings and breaches are raised |
| FR-14 | Supervisor can **monitor, reassign, and intervene** |
| FR-15 | Bot uses **RAG over a knowledge base** to answer from FAQs, SOPs, and past resolutions |
| FR-16 | System **classifies intent and detects sentiment** from customer messages to refine routing/priority |
| FR-17 | On escalation/resolution, an **AI summary** of the conversation is generated for the agent/supervisor |
| FR-18 | Agents receive **AI-suggested responses** (agent assist) drawn from the knowledge base |
| FR-19 | **Escalations, SLA breaches/warnings, sentiment drops, and agent-offline events notify supervisors** via **Slack + email** (internal ops channel, separate from customer notifications) |
| FR-20 | Supervisor alerts carry an **AI-generated summary + suggested action** (customer tier, history, churn risk) for fast decisions |
| FR-21 | *(Phase 2)* Supervisors can **act directly from Slack** (approve refund, reassign, join chat) via interactive components |

---

## 3. Non-Functional Requirements (NFR)

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-1 Latency (assignment) | Ticket visible on agent dashboard | < 5 sec |
| NFR-2 Latency (bot) | Bot first token to customer | < 2 sec |
| NFR-3 Scale | Concurrent agents supported | 20 → 1000+ (no re-architecture) |
| NFR-4 Throughput | Peak ticket intake (sale events) | 10K+ tickets/min |
| NFR-5 Availability | Core services uptime | 99.9%+ |
| NFR-6 Durability | No ticket/action lost, ever | Zero data loss |
| NFR-7 Consistency | Ticket state + business action integrity | Strong (ACID) on transactional data |
| NFR-8 Auditability | Full replayable event history | 7-day retention minimum |
| NFR-9 Reliability | Failed notifications recoverable | Retry + DLQ |
| NFR-10 Security | Auth, secrets, PII protection | JWT, Secret Manager, encryption in transit |
| NFR-11 Observability | Trace one ticket end-to-end | Correlation ID across all systems |
| NFR-12 Extensibility | Add channel / action without core change | Pluggable services |

---

## 4. Architecture Overview

```
                         CUSTOMER
              Phone (PSTN/VoIP)     Chat (Web/App)
                     |                    |
                     +---------+----------+
                               |
                        Cloudflare (DNS, WAF, DDoS)
                               |
                        Azure Front Door
                               |
                     Node.js API Gateway  (BFF: auth, rate-limit, route)
                               |
     +-------------------+-----+-------+---------------------+
     |                   |             |                     |
 Bot Service      Assignment      Agent Service       Notification
 (Python)          Service         (Python)             Service
  voice+chat        (Python)      WS + REST + biz       (Python+Celery)
     |                   |          actions                  |
     |                   |             |                     |
     +----------+--------+------+------+----------+----------+
                |               |                 |
             KAFKA          RabbitMQ           Redis
         (event backbone)  (task/queue)   (hot state/cache)
                |               |                 |
        +-------+-------+       |                 |
        |               |       |                 |
   AI Consumers   (analytics)   |                 |
   Summarizer /                 |                 |
   Sentiment /                  |                 |
   Intent                       |                 |
        |                       |                 |
        |  Kafka "supervisor-alerts"              |
        |        |                                |
        |   Supervisor Notification Service       |
        |   (enrich via LLM → Slack + email)      |
        |        |                                |
        |     Slack Webhook / Supervisor email    |
        |                                         |
        +-----------+-----------+-----------------+
                    |                             |
             DATA PLANE (polyglot)              Business API Layer
        PostgreSQL  MongoDB  Azure Synapse       Order/Payment/Return/
        (ACID:      (convos) (analytics)         Exchange/Customer
         tickets,                                       |
         agents,                                External/Internal Systems
         actions)                               (OMS, Payment GW, Warehouse)

   AI PLANE (Azure):  Azure OpenAI (LLM)  |  Azure Speech (STT/TTS)
                      Azure AI Search (vector DB / RAG knowledge base)
                      Agent Assist  |  Summarizer  |  Sentiment  |  Intent
   MEDIA PLANE:       LiveKit (WebRTC)  |  Telephony (Twilio/Exotel/Ubona)
```

---

## 5. Component Design & Technology Selection

Each component lists **responsibility**, **chosen tech**, and **why that tech over alternatives**.

### 5.1 API Gateway (BFF)
- **Responsibility:** Authentication, rate limiting, request validation, routing. No business logic, no DB, no queue access.
- **Tech:** **Node.js** (Express/Fastify) on **Azure Container Apps**.
- **Why:** Node's event loop excels at high-concurrency, I/O-bound edge work (thousands of open connections, thin pass-through). This is exactly the "gateway" role Node fills at scale — not heavy compute. Keeps business logic out of the edge.
- **Alternatives considered:** Kong/APIgee (heavier, more ops); Go (great but adds a language for marginal edge gain).

### 5.2 Bot Service
- **Responsibility:** First responder on voice + chat. Understands intent, answers from knowledge base, resolves simple queries, escalates the rest.
- **Tech:** **Python + FastAPI**; **Azure OpenAI** (LLM), **Azure Speech** (STT/TTS), **Azure AI Search** (RAG knowledge base), **LiveKit** (voice media).
- **Why Python/FastAPI:** LLM/AI ecosystem is Python-native; FastAPI is async (high I/O concurrency) and integrates cleanly with LiveKit's Python SDK. Directly reuses the existing MS Voice Bot stack.
- **Why Azure for AI:** Reuses existing production Voice Bot (data residency, Azure OpenAI, Azure AI Search ingestion pipeline already built). No need to re-platform AI.
- **Alternatives considered:** Node for bot (weak AI ecosystem); self-hosted LLM (ops-heavy, no benefit here).

### 5.3 Assignment Service
- **Responsibility:** The routing brain. Filter by skill → filter by availability → priority route → dispatch to agent queue → handle "no agent" and reassignment.
- **Tech:** **Python + FastAPI**, reads **Kafka** (ticket events), reads **Redis** (availability), writes **RabbitMQ** (dispatch).
- **Why:** Pure business logic + orchestration; Python keeps it consistent with the rest of the backend and easy to evolve routing rules.

### 5.4 Agent Service
- **Responsibility:** Real-time agent dashboard backend (WebSocket), REST for ticket/history, and the **Business Action** entry point (refund/cancel/etc.).
- **Tech:** **Python + FastAPI** (native WebSocket support) on Azure Container Apps.
- **Why:** FastAPI handles WebSocket + REST in one service; async model suits many concurrent agent sockets. Frontend dashboard in **React** (see 5.10).

### 5.5 Business API Layer
- **Responsibility:** Encapsulate all downstream actions — order status, tracking, cancel, refund, return, exchange, customer profile/loyalty. Single integration surface; logs every call.
- **Tech:** **Python + FastAPI** adapters calling internal/external systems (OMS, Payment Gateway). **Mock services** for the portfolio build.
- **Why:** Isolates volatile external contracts from core CRM; one place for retries, timeouts, **circuit breaking** (fail fast when OMS/Payment is down), and call logging. Adding a new action = adding an adapter, not touching core (NFR-12).

### 5.6 Notification Service
- **Responsibility:** Send email / WhatsApp / SMS on ticket events and agent actions. Retry on failure, dead-letter on repeated failure.
- **Tech:** **Python + Celery workers**, consuming **RabbitMQ**, sending via **Brevo** (email/SMS/WhatsApp in one API).
- **Why RabbitMQ (not Kafka) here:** Notifications are **tasks** needing per-message **retry with backoff, TTL, priority, and dead-letter** — RabbitMQ's native strengths. Kafka has none of these out of the box.
- **Why Brevo:** Single provider covers all three channels, generous free tier, simple REST — good enough for the build; swappable behind the service.

### 5.7 Event Backbone — Kafka
- **Responsibility:** Immutable event log for tickets, conversations, agent-availability, SLA events, analytics. Multiple independent consumers. Replay for audit/debug.
- **Tech:** **Kafka** (Azure Event Hubs, Kafka-compatible mode).
- **Why Kafka:** Needs **ordering (per partition), replay, retention, and fan-out to many consumers** (SLA tracker, analytics, audit) — Kafka's core design. Partition key = a stable entity id (e.g., ticket/customer) to preserve per-entity order.
- **Why managed (Event Hubs):** Fully managed, no broker/controller ops at all — Event Hubs speaks the Kafka protocol natively, so existing Kafka client code (bootstrap servers, topic names, consumer groups) connects with a config change, not a rewrite. Topic maps to Event Hub, partition and consumer group concepts carry over directly. Free tier sufficient for the build.

### 5.8 Task Queue — RabbitMQ
- **Responsibility:** Per-agent dispatch queues, unassigned queue, notification queues, DLQs. Priority + acknowledgment timeout + reassignment.
- **Tech:** **RabbitMQ** (CloudAMQP (vendor-neutral managed RabbitMQ, works identically on Azure)).
- **Why RabbitMQ:** Work distribution with **priority**, **message TTL** (ack timeout), **dead-letter exchange** (reassignment), and **routing** — all native. This is a task-broker problem, not an event-log problem.

> **Kafka vs RabbitMQ — the deliberate split:**
> Kafka = *"what happened"* (event history, replay, analytics, audit, SLA).
> RabbitMQ = *"what to do next"* (dispatch this ticket, send this message, retry, escalate).
> Using both is right-tool-for-the-job, not over-engineering.

### 5.9 Hot State / Cache — Redis
- **Responsibility:** Sub-millisecond agent availability lookup (status, load, skills), session data, rate-limit counters.
- **Tech:** **Redis** (Azure Cache for Redis).
- **Why:** Assignment runs on every ticket at high rate; hitting PostgreSQL per lookup is too slow/heavy. Redis gives <1ms reads. PostgreSQL remains the durable record; Redis is the fast projection.

### 5.10 Agent Dashboard (Frontend)
- **Responsibility:** Real-time queue, live chat/call panel, customer + order context, action buttons, SLA timers.
- **Tech:** **React** (your core strength), WebSocket client, on Azure Container Apps / static hosting + CDN.
- **Why:** Real-time UI with many live components suits React; leverages your 10-yr frontend depth and gives an instantly demoable interface — a differentiator in interviews.

### 5.11 Datastore — Polyglot Persistence
The store is chosen per **access pattern**, not one-size-fits-all.

- **PostgreSQL (Azure Database for PostgreSQL)** — **source of truth for transactional data**: tickets, agents, agent_actions, sla_tracking, notification_log, **customers** (Phase 6 — `id, email, phone, name`; enables notification-service to look up customer email from `customer_id` on resolved tickets, replacing the Phase 3 hardcoded test address).
  - *Why:* ACID for ticket state transitions and business actions where money/integrity matter (NFR-6, NFR-7); relational joins for history/reporting.
- **MongoDB (Atlas / self-managed)** — **conversation transcripts**.
  - *Why:* Conversations are document-shaped with variable structure (chat vs voice vs metadata, per-message sentiment), append-heavy, read by `ticket_id`/`customer_id`, no joins, and the highest-volume data in the system. Forcing them into relational rows adds no value and caps write scale. A document store fits naturally and scales writes better.
- **Redis (Azure Cache for Redis)** — **hot state**: agent availability, sessions, WebSocket→instance mapping, rate-limit counters. (Covered in 5.9 and §9.)
- **Azure AI Search (vector DB)** — **knowledge base** for RAG (bot + agent assist): embeddings of FAQs, SOPs, past resolutions. This is a NoSQL vector store, queried semantically.
- **Azure Synapse Analytics** — **analytics warehouse**: time-series event aggregation fed from Kafka (resolution times, agent performance, SLA compliance, complaint trends). Keeps heavy aggregation off the operational databases.

> **Polyglot rule:** relational for transactional integrity, document for high-volume conversations, key-value for hot ephemeral state, vector for semantic search, columnar warehouse for analytics.

### 5.12 Media & Telephony
- **Responsibility:** Bridge PSTN/VoIP calls into the platform; carry real-time audio for bot and agent.
- **Tech:** **Telephony provider (Twilio/Exotel/Ubona)** → **LiveKit (WebRTC)**.
- **Why:** Mirrors the proven MS Voice Bot pipeline; LiveKit handles WebRTC media, providers bridge PSTN.

### 5.13 AI Services (beyond the bot)
All AI consumers attach to the Kafka `conversations`/`tickets` streams, so they add intelligence **without touching the core flow** (Kafka fan-out). Real-time assist is served synchronously from the Agent Service.

- **Auto-Summarizer** — on escalation/resolution, an LLM condenses the transcript into a short context blob stored on the ticket (feeds the agent/supervisor). *Tech:* Azure OpenAI, Kafka consumer.
- **Agent Assist (RAG)** — while the agent reads a message, the system retrieves from the knowledge base (Azure AI Search) and an LLM proposes 2–3 suggested replies the agent can send or edit. *Tech:* Azure AI Search + Azure OpenAI, served via Agent Service.
- **Intent Classifier** — classifies each inbound message (intent, priority hint) to refine routing beyond keyword rules. *Tech:* LLM structured output or a lightweight classifier, Kafka consumer feeding Assignment Service.
- **Sentiment Analyzer** — tracks sentiment across the conversation; a downward trend (frustrated→angry) auto-raises priority and alerts a supervisor. *Tech:* sentiment model on the `conversations` stream.
- **(Phase 2) Ticket Categorization & SLA-risk prediction** — batch LLM categorization for analytics and an ML model predicting breach risk. *Tech:* batch jobs → Azure Synapse Analytics.

- **Why these, and why now:** each solves a concrete problem (faster resolution, better routing, proactive escalation, business insight) and reuses the existing Azure AI plane. They are additive by design — new Kafka consumers — so they don't destabilize the transactional path.

### 5.14 Supervisor Notification Service (internal ops alerts)
- **Responsibility:** Deliver **internal** alerts to supervisors — distinct from customer notifications in audience, urgency, and channel. Consumes a single **Kafka `supervisor-alerts`** topic that all sources publish to, then fans out to **Slack (primary)** and **email (backup)**. Handles formatting, deduplication (avoid alert spam), and retry.
- **Alert sources (all publish to `supervisor-alerts`):**
  - Escalation to supervisor (reassignment retry ≥ 3, or no eligible agent)
  - SLA 75% warning and 100% breach (from SLA consumer)
  - Sentiment drop to *angry* (from Sentiment Analyzer)
  - Agent offline mid-ticket (TTL expiry, §9.4)
  - Unassigned-queue backlog above threshold
- **AI enrichment:** each alert is enriched by the **Auto-Summarizer** (concise situation summary) and a **Suggested-Action** LLM call (recommended next step, informed by customer tier, prior contacts, churn risk) before it is sent — so a supervisor decides in seconds without opening the dashboard.
- **Tech:** **Python + FastAPI** consumer; **Slack Incoming Webhook** (HTTP POST) for real-time channel alerts; **Brevo email** as fallback when Slack is unavailable or the supervisor is offline; **Azure OpenAI** for summary + suggested action.
- **Why a dedicated service (Option B), not inline in each AI consumer:** centralizes all supervisor alerting in one place — consistent formatting, deduplication, a single retry path, and trivial addition of new channels (Teams/PagerDuty) later. Keeps Slack logic out of the AI consumers and the transactional services.
- **Why Slack (not Brevo) for supervisors:** supervisors live in Slack for ops; webhooks are a simple real-time HTTP POST with no OAuth for basic alerts. Customer-facing Brevo path (email/WhatsApp/SMS) stays entirely separate.

> **Phase 2 — Slack interactive actions:** upgrade alerts to **Slack interactive components** so a supervisor can **Approve Refund / Reassign / Join Chat** directly from the Slack message. Slack posts the button callback to a secured endpoint on the Supervisor Notification Service (or Gateway), which invokes the Agent Service / Business API Layer — the supervisor never leaves Slack. Deferred because it adds callback handling, request signing/verification, and action authorization; Phase 1 (enriched alerts + email backup) delivers ~80% of the value first.

---

## 6. Frontend Architecture — Microfrontends (Module Federation)

Three distinct UIs exist: **SOP onboarding** (admin), **agent dashboard** (agent/supervisor), **customer chat** (customer). Rather than three disconnected apps, they are built as a **microfrontend shell with independently deployable remotes** — the same Module Federation pattern as Myntra's Spectrum platform.

### 6.1 Shell (Host) + Remotes

```
Host/Shell App (Container)
  ├── Remote: sop-onboarding-mfe    → /admin/sop
  ├── Remote: agent-dashboard-mfe   → /agent
  └── Remote: customer-chat-mfe     → /support (also standalone-embeddable)
```

**Why Module Federation (over single-spa/iframes):** runtime integration — ship `agent-dashboard-mfe` independently without rebuilding the shell or the other two remotes; shared singleton dependencies (React, design system, auth context) load once instead of duplicating per remote; directly reuses proven production architecture rather than introducing a new pattern.

### 6.2 Shell Responsibilities

- Top-level routing (React Router, lazy-loaded remotes)
- Auth/session (JWT from Gateway) passed down to remotes via context/props
- **RBAC route guards**: Admin → `/admin/sop`; L1/L2/Supervisor → `/agent`; public → `/support`
- Shared layout, theme, design tokens; a page switcher for demo purposes (`SOP Admin | Agent Console | Customer View`)
- Owns the shared **WebSocket connection lifecycle** used by the agent remote

The shell does **not** own business logic or page-specific state — each remote owns its own.

### 6.3 Remote Responsibilities

| Remote | Pages | Role gate | Notes |
|--------|-------|-----------|-------|
| `sop-onboarding-mfe` | Upload SOP, ingestion status (indexed/failed/last-updated), list/manage SOPs, trigger re-index | Admin only | Feeds the Phase 5B ingestion pipeline (Blob → Skillset → Index → Indexer) |
| `agent-dashboard-mfe` | Ticket queue, live chat/call panel, customer + order context, action buttons, SLA timers | L1 / L2 / Supervisor | As designed in §5.4; consumes the shell-owned WebSocket |
| `customer-chat-mfe` | Chat widget UI (talks to Bot Service / Gateway) | Public/anonymous (session-based, not JWT-role-gated) | **Dual-mode:** renders as a "Customer View" page inside this shell for demo, **and** is standalone-embeddable via script tag on an external page — mirroring how a real customer-facing widget would sit on the storefront, outside any internal tool |

### 6.4 Shared Dependencies (Module Federation `shared` config)

- **Shared (singleton):** `react`, `react-dom`, design-system/UI-kit, auth-context, a thin websocket-client wrapper.
- **Not shared (each remote owns):** business logic, local component state, domain-specific API calls.

### 6.5 Cross-Remote Communication

No direct imports between remotes — avoids tight coupling. Host-mediated props/context handle shell→remote (auth, theme). If a remote-to-remote signal is ever needed, a small event bus (`window.CustomEvent`/pub-sub) is the escape hatch — not required for this project since each remote owns exactly one page and none currently need to talk to another.

### 6.6 Deployment Model

Four independently deployable units — shell, `sop-onboarding-mfe`, `agent-dashboard-mfe`, `customer-chat-mfe` — each its own repo/package/CI pipeline, served as static bundles behind a CDN (client bundles, not backend services). Remote versions are pinned in the shell's Module Federation config, with a fallback bundle if a `remoteEntry.js` fails to load.

### 6.7 Routing

```
/                    → landing / page switcher
/admin/sop           → sop-onboarding-mfe   (Admin only)
/agent               → agent-dashboard-mfe  (L1/L2/Supervisor)
/support             → customer-chat-mfe    (public)
```

---

## 7. Technology Selection Summary

| Concern | Technology | One-line justification |
|---------|-----------|------------------------|
| Edge / Gateway | Node.js (Azure Container Apps) | Best for high-concurrency I/O pass-through |
| Business services | Python + FastAPI | Async, AI-native, one language for backend |
| AI (LLM/STT/TTS/RAG) | Azure OpenAI / Speech / AI Search | Reuse existing production Voice Bot |
| AI assist / summarize / sentiment / intent | Azure OpenAI + Kafka consumers | Additive intelligence via fan-out, no core change |
| Voice media | LiveKit + Telephony | Proven WebRTC pipeline |
| Event log / audit / SLA / analytics | Kafka (Azure Event Hubs) | Ordering, replay, fan-out, retention |
| Task dispatch / customer notifications | RabbitMQ (CloudAMQP) | Priority, TTL, DLQ, routing |
| Supervisor / internal alerts | Slack Webhook + email (Brevo backup) | Real-time ops channel, separate from customer comms |
| Hot state / availability | Redis (Azure Cache for Redis) | Sub-ms reads at high rate |
| Transactional source of truth | PostgreSQL (Azure Database for PostgreSQL) | ACID, strong consistency |
| Conversation transcripts | MongoDB | Document-shaped, high-volume, no joins |
| Knowledge base | Azure AI Search (vector DB) | Semantic RAG retrieval |
| Analytics warehouse | Azure Synapse Analytics | Time-series aggregation off operational DBs |
| Frontend dashboard | React | Real-time UI, your strength, demoable |
| Notifications delivery | Brevo | Email+WhatsApp+SMS, one API |
| Compute platform | Azure Container Apps | Serverless auto-scale, single-vendor with Foundry (no cross-cloud networking) |
| Secrets | Azure Key Vault | Centralized secure config |
| Correlation/observability | request_id + logs/traces | End-to-end ticket tracing |

---

## 8. Core Data Flows

### 8.1 Chat, bot resolves
Customer → Gateway → Bot Service (Azure AI, RAG) → resolved → PostgreSQL (close) → Kafka `tickets`(resolved) → Notification Service → Brevo → customer.

### 8.2 Chat/voice, escalated to agent
Bot fails/opt-out → ticket persisted (PostgreSQL) + Kafka `tickets`(created) → Assignment Service reads event → Redis availability + skill/priority filter → RabbitMQ `agent.{id}.queue` (priority) → Agent Service consumes → WebSocket push → dashboard (<5s) → agent acknowledges.

### 8.3 Agent takes a business action
Agent clicks action → Agent Service → Business API Layer → external system (OMS/Payment) → result to agent + `agent_actions` (PostgreSQL) + Kafka `agent-actions` → Notification Service → customer notified.

### 8.4 No acknowledgment (reassignment)
`agent.{id}.queue` TTL expires → DLQ → Assignment Service reads DLQ → retry<3 → next agent; retry≥3 → supervisor.

### 8.5 No agent available
Assignment → `unassigned.queue` + customer ETA → periodic re-check → agent frees (Redis update) → dispatch.

### 8.6 SLA tracking
Kafka `tickets`(created) → SLA consumer starts per-priority timer → at 75% warn (WebSocket to agent + supervisor) → at 100% breach event → auto-escalate + analytics.

### 8.7 AI enrichment (additive, off the critical path)
Kafka `conversations` → **Sentiment Analyzer** (trend down → raise priority + alert supervisor) · **Intent Classifier** (refine routing hint to Assignment Service) · **Auto-Summarizer** (on escalation/resolution → summary written to ticket). **Agent Assist** runs synchronously: agent viewing a message → Agent Service → RAG over Azure AI Search + Azure OpenAI → suggested replies. Nightly batch → **categorization + SLA-risk** → Azure Synapse Analytics.

### 8.8 Analytics
Kafka `analytics`/`tickets`/`agent-actions` → sink → **Azure Synapse Analytics** → dashboards (resolution time, agent performance, SLA compliance, complaint trends). Operational DBs untouched.

### 8.9 Supervisor alert (internal)
Any source (escalation · SLA warn/breach · sentiment→angry · agent-offline · queue backlog) → Kafka `supervisor-alerts` → **Supervisor Notification Service** → enrich (Auto-Summarizer + Suggested-Action LLM) → **Slack webhook** (primary) + **email** (backup). *Phase 2:* supervisor taps a Slack button → signed callback → Agent Service / Business API Layer executes (approve refund / reassign / join) → confirmation back to Slack.

---

## 9. Agent Session & Conversation Transcript Management

### 9.1 Agent Session — split by access pattern

Session state is deliberately **not** kept in one place; each piece lives where its access pattern is cheapest.

| Session data | Store | Why here |
|--------------|-------|----------|
| Identity (who the agent is) | **JWT** (stateless) | No server lookup per request |
| Availability (online/busy/away, load, skills) | **Redis** | Assignment reads this on *every* ticket — needs <1ms |
| Live WebSocket handle | **In-memory** on the Agent Service instance | A socket object can't be serialized/shared |
| Which instance holds the socket | **Redis** (`agent:{id}:instance`) | Lets any instance locate any agent |
| Login/logout history | **PostgreSQL** | Durable audit + reporting |

### 9.2 The cross-instance WebSocket problem

When Agent Service auto-scales, an agent's live socket lives on **one** instance, but a ticket for that agent may arrive at a **different** instance.

**Solution — Redis Pub/Sub as the bridge:**
- On connect, the holding instance records `agent:{id}:instance = instance-2` in Redis.
- When another instance needs to reach that agent, it reads the mapping and **publishes to a Redis channel** named for that instance.
- The holding instance is subscribed to its own channel, receives the message, and pushes it down the live socket.

> RabbitMQ delivers the *ticket* to the agent's queue; Redis Pub/Sub answers *"which instance holds the socket right now."*

### 9.3 Session lifecycle

Login → JWT issued, PostgreSQL records login, Redis sets `status=available, load=0`.
Connect WS → Redis sets `agent:{id}:instance`.
Ticket assigned → Redis `load` incremented.
Break → Redis `status=away`; assignment skips the agent.
Disconnect → Redis `status=offline`, mapping removed; in-flight tickets reassigned via DLQ.

### 9.4 Liveness + availability: dual-layer graceful degradation

A crashed laptop or dropped network may not fire a clean WebSocket
`close`, leaving Redis falsely showing the agent as available. This is
handled by a **server-driven heartbeat** feeding a **two-layer
availability model** (implemented, not just designed):

- **Redis (primary, fast-read, self-healing):** `agent-service` runs a
  background task that re-`SET`s `agent:{id}:status available EX 90`
  every ~30s while the WebSocket is open — server-owned, not dependent
  on the client sending anything. If heartbeats stop (crash, dropped
  network), the key expires via TTL within 90s and the agent naturally
  falls out of availability with **no polling loop and no sweep**. This
  also makes Redis self-healing after a Redis *restart*: each live
  agent's next heartbeat repopulates its key within one cycle (≤30s).
- **Postgres (durable fallback):** `agents.status` and `current_load`
  are written on shift start/end and on assign/resolve — **not** on
  every heartbeat (that would be ~1.4M writes/day at 500 agents; the
  shift+ticket pattern is ~2000/day). If Redis is *unreachable* (not
  just cold), Assignment Service falls through to a direct Postgres read
  of status+load. A Redis restart is covered by heartbeat self-heal; a
  sustained Redis outage is covered by this Postgres fallback.

Write-path Redis operations (load `incr`/`decr`) are guarded too:
Postgres is updated first (durable), Redis second (best-effort), so
assignment stays correct even with Redis down. **Known gap:** Redis
load counters drift during a sustained outage and are not yet
reconciled from Postgres on recovery — a reconciliation-on-reconnect
step is the planned fix.

### 9.5 Conversation transcripts — dual write

Every message (customer, bot, agent) is written to **two** stores by access pattern:

| Store | Role | Retention |
|-------|------|-----------|
| Kafka `conversations` topic | Audit log, replay, analytics feed | ~7 days, then archived |
| MongoDB `conversations` collection | Source of truth, queryable by ticket/customer | Permanent |

Kafka answers *"replay everything that happened during this incident."* MongoDB answers *"show the transcript for ticket 123"* via an indexed document lookup — conversations are document-shaped and append-heavy, which is why they sit in a document store rather than relational rows (see 5.11).

### 9.6 Voice-call transcripts

Voice has no native text, so it is transcribed inline: **Azure Speech STT** converts customer speech to text; bot/agent responses (text → TTS for audio) are also stored as text. All of it lands in the same `conversations` collection with `channel = 'phone'`, making voice calls **searchable transcripts** using the identical query path as chat.

### 9.7 Full context on ticket open

When a ticket reaches an agent, the dashboard assembles context before the agent types a word:
1. **AI-generated summary** (from the Auto-Summarizer, stored on the ticket record).
2. **Current conversation** so far (MongoDB, by `ticket_id`).
3. **Customer's past conversations** (MongoDB, by `customer_id`) — recurring issue? VIP? angry?
4. **Order/business context** (Business API Layer).

New messages during the live session are appended in real time over the WebSocket; history is loaded once from MongoDB on open. (An optional Redis cache of the *active* conversation can speed appends at very high concurrency, written through to MongoDB.)

---

## 10. Scaling Strategy (20 → 1000+ agents)

- **Stateless services (Bot, Assignment, Agent, Notification):** horizontal auto-scale on Azure Container Apps; add instances, no code change.
- **Kafka:** increase partitions on high-volume topics; add consumer instances in the same group for parallelism (ordering preserved per partition).
- **RabbitMQ:** per-agent queues scale naturally with agent count; add consumer workers for notification queues.
- **PostgreSQL:** start single primary + read replicas for read-heavy history/reporting; introduce **sharding** (by customer/region) only if write volume demands it — the sharding pattern from the prior assignment applies here.
- **Redis:** cluster mode for availability state as agent count grows.
- **WebSocket fan-out:** shard agent connections across Agent Service instances; use a shared broker (RabbitMQ/Redis pub-sub) so any instance can reach any agent.

---

## 11. Reliability, Consistency & Failure Handling

| Risk | Mitigation |
|------|-----------|
| Duplicate processing on retry | **Idempotency key** (request_id) + `ON CONFLICT DO NOTHING` |
| Lost ticket on crash | PostgreSQL WAL (durability) + Kafka offset committed **after** DB commit |
| Agent offline mid-dispatch | RabbitMQ TTL → DLQ → reassign |
| Notification provider down | Celery retry w/ backoff → DLQ → ops alert |
| SLA breach | Kafka SLA consumer → supervisor alert + auto-escalate |
| Business API failure | Timeouts + retries in Business API Layer; action logged as failed, agent informed |
| Partial multi-step action | Wrap DB-side state changes in a transaction; external calls guarded with saga/compensation where needed |

### 11.1 Gaps closed after design review (dual-write, race conditions, timeout tuning)

Three concrete failure modes were surfaced by walking through the assignment flow end-to-end — each is a real gap in the design above, not a restatement of it.

**Dual-write problem (ticket creation, and separately, assignment dispatch).** Two systems are written to for one logical action — e.g., `INSERT INTO tickets` (Postgres) and publish "ticket created" (Kafka) are separate operations. If the DB write succeeds but the publish fails (brief Kafka unavailability), the ticket exists but no downstream service ever learns about it — it sits invisible, unassigned, indefinitely. The same problem recurs at Assignment Service: updating `tickets.assigned_agent_id` and publishing to the agent's RabbitMQ queue are two separate writes with the same failure mode.
- **Fix — Transactional Outbox pattern:** instead of publishing directly, write the event into an `outbox` table in the **same Postgres transaction** as the primary write (ticket insert, or assignment update). Since both are one transaction, they succeed or roll back together — atomic by construction. A separate poller (or Debezium CDC reading the outbox table) publishes to Kafka/RabbitMQ and marks the outbox row published. If the broker is briefly down, the outbox row simply waits; nothing is silently lost.

**Redis check-then-act race on agent load.** Two assignment-service instances, processing two different tickets concurrently, can both read `agent:5:load = 3`, both see room under `max_load`, and both increment — real load should be 5, Redis shows 4. Classic check-then-act race, same shape as the SQL race conditions in the companion sharding/locking assignment.
- **Fix:** replace read-then-write with an atomic Redis `INCR`/`DECR`, or where the check (available AND load<max) and the increment must happen as one indivisible step, a **Redis Lua script** (executes atomically server-side) — conceptually the same as the optimistic-locking pattern (version-checked update, retry on conflict) from the sharding assignment, applied to Redis instead of SQL.

**Ack-timeout is not priority-aware, and load isn't decremented on reassignment.** The RabbitMQ per-agent queue TTL (§7.4) currently uses one fixed timeout for every ticket. A P1 ticket stuck behind a silently-dead agent waits the full timeout before DLQ-triggered reassignment — on a 1-hour SLA, a single 5-minute stall is already significant, and a second stale agent on retry compounds it (up to `retries × timeout`). Separately, when a stuck ticket is pulled from a dead agent's queue via DLQ and reassigned, that agent's `current_load` in Redis must be decremented as part of the reassignment — otherwise the agent looks permanently at-capacity even after they reconnect, quietly reproducing the same starvation problem later.
- **Fix:** tier the ack-timeout by priority — P1 ≈ 30–60s, P2 ≈ 2min, P3 ≈ 5min (current default) — trading "give the agent time to respond" against "reassign fast when urgent," correctly, per priority instead of one-size-fits-all. And make the DLQ-reassignment handler explicitly decrement the original agent's Redis load counter, not just increment the new agent's.

Detection-window honesty: heartbeat (§9.4) bounds how long a *silently* dead agent looks available (~90–120s), it does not eliminate the window — a crashed process can't send a graceful goodbye. Tightening the heartbeat interval shrinks the window at the cost of more heartbeat traffic; this is a real trade-off, not a free fix.

### 11.2 Key Design Decisions (defensible comparisons)

Three decisions that get probed hardest in review. Each is a deliberate choice against real alternatives, not a default.

#### Decision 1 — Kafka AND RabbitMQ (not one or the other)

The most common challenge: "these overlap, pick one." They don't overlap — they solve opposite problems, and the system genuinely needs both.

| Dimension | Kafka (event backbone) | RabbitMQ (task broker) |
|---|---|---|
| **Core question it answers** | "What happened?" (fact, broadcast) | "What should happen next?" (task, delivered once) |
| **Consumption model** | Many independent consumers each read the same event, at their own offset | One consumer takes the message; once acked, it's gone |
| **Retention** | Retained for days — a *new* consumer can replay history that happened before it existed | Deleted on consume — no replay |
| **Ordering** | Per-partition ordering, keyed (e.g. by `ticket_id`) | Per-queue; priority queues supported |
| **Per-message TTL / expiry** | No native per-message expiry | Native — message expires if unacked in N seconds |
| **Dead-lettering** | Not native | Native DLX/DLQ |
| **In this system** | `tickets`/`conversations`/`agent-availability`/`sla-events` — fan-out to SLA tracker, notifications, analytics, AI enrichment, all reading the same ticket event independently | `agent.{id}.queue` — dispatch one ticket to one agent, with ack-timeout → auto-reassign via DLQ |

**Why not RabbitMQ alone:** it deletes on consume, so having the SLA tracker, notification service, and analytics all react to one "ticket created" event would need three separate queues or a fanout exchange copying the message at publish time — and a consumer added *next month* could never replay history. Kafka gives multi-consumer replay natively.

**Why not Kafka alone:** agent dispatch needs "give this ticket to agent 5, and if they don't ack in 5 minutes, hand it to someone else." That's per-message TTL + dead-lettering — Kafka has no concept of a message expiring on a timer. Building it in Kafka means writing your own polling/timer system, which is exactly the sweep-style polling the whole design avoids.

**One-liner:** Kafka is the newspaper (everyone reads the same copy, archived); RabbitMQ is the to-do list (one person takes each task, it's crossed off when done).

#### Decision 2 — APScheduler + Postgres job store for SLA (not in-memory timers, not Redis TTL)

SLA tracking needs a timer per ticket (warn at 75%, breach at 100%). Three ways to hold those timers:

| Option | Restart behavior | Failure coupling | Observability | Verdict |
|---|---|---|---|---|
| **In-memory `asyncio.sleep`** | All pending timers lost on restart; must replay entire Kafka `tickets` topic (300k+ events at 30-day retention) to rebuild | None extra | None without custom metrics | ❌ Silent SLA loss on every deploy/crash |
| **Redis TTL + keyspace events** | Survives app restart (Redis holds it), but couples SLA to the *same Redis instance* as agent availability | One Redis outage kills availability AND SLA simultaneously | Good (`KEYS sla:*`) | ❌ Two unrelated concerns share one failure domain |
| **APScheduler + Postgres job store** | Jobs persisted in Postgres; on restart APScheduler reconnects, finds pending jobs, resumes exactly — zero Kafka replay | Independent of Redis; shares Postgres (already the durable source of truth) | Queryable (`SELECT * FROM apscheduler_jobs`) | ✅ Chosen |

**Why chosen:** a production CRM can't silently stop monitoring SLAs because a container restarted during a deploy. APScheduler with a persistent job store means the tracker can crash, redeploy, or scale and resume every pending timer exactly, with no Kafka replay and no timer loss. Redis TTL was rejected specifically because it would couple SLA tracking to the same failure domain as agent availability — one Redis outage shouldn't take down two unrelated capabilities.

**Consistency note:** this is the *same* no-polling instinct behind the Redis-TTL agent-availability choice (§9.4) and the lazy `awaiting_confirmation` check (Phase 2A) — but here the durability requirement (must survive restarts) tips it to a persistent scheduler rather than in-memory or TTL.

#### Decision 3 — Redis + Postgres dual-layer for agent availability (graceful degradation)

Covered mechanically in §9.4; the *decision rationale* and rejected alternatives:

| Approach | Redis restart (cold) | Redis sustained outage | Postgres write load | Verdict |
|---|---|---|---|---|
| **Redis only** | Agents vanish until they reconnect; no assignment possible | Total assignment outage | None | ❌ Single point of failure for the whole assignment pipeline |
| **Redis + Postgres synced on every heartbeat** | Survives (Postgres has data) | Survives | ~1.4M writes/day at 500 agents — constant hot-path pressure on the primary DB | ❌ DB write pressure defeats the purpose |
| **Redis primary + Postgres fallback, synced on shift/assign only** | Self-heals within one heartbeat cycle (≤30s) via server-driven heartbeat | Assignment falls back to Postgres read — degrades latency, never fails | ~2000 writes/day — negligible | ✅ Chosen |

**The key insight:** stop treating Redis as a cache *of* Postgres (which forces constant syncing). Treat them as two sources of truth for two different things — Redis owns "who is *currently connected* live," Postgres owns "who is *configured* as an agent and their durable load." They're not synced on every heartbeat; Redis self-heals from live heartbeats on restart, and Postgres is only written on meaningful state changes (shift start/end, assign/resolve). This gives both zero-downtime assignment (Postgres fallback) and low DB write pressure — the two requirements that seemed to conflict.

**Honestly-named residual gap:** during a *sustained* Redis outage the load counters drift (missed incr/decr) and aren't yet reconciled from Postgres on recovery. Postgres stays correct throughout; the planned fix is reconciliation-on-reconnect. Named deliberately — a senior answer includes the gap it hasn't closed yet, not just the parts that work.

#### Decision 4 — Two-hop notification path (Kafka → RabbitMQ → Brevo), not a direct call

When a ticket resolves, the customer needs an email. The naive path is: consume the Kafka `tickets` resolved event, call Brevo directly. The chosen path adds a RabbitMQ hop in between.

| Approach | What happens if Brevo is down | Retry mechanism | Verdict |
|---|---|---|---|
| **Kafka consumer → Brevo directly** | Consumer throws; you either lose the notification or must build custom retry/backoff logic inside the consumer | DIY | ❌ External API failures require custom retry logic |
| **Kafka consumer → RabbitMQ → worker → Brevo** | Consumer publishes to RabbitMQ and moves on; worker retries automatically when Brevo recovers; undeliverable messages go to DLQ | Built-in via RabbitMQ TTL/DLQ | ✅ Chosen |

**Why this matters:** Brevo is an external SaaS — you don't control its uptime. A direct call from a Kafka consumer means a 2-minute Brevo outage either loses notifications silently or requires you to implement exponential backoff, dead-letter handling, and retry queues yourself. RabbitMQ already has all of that built in — it's the same reason agent dispatch uses RabbitMQ rather than direct WebSocket pushes. The two-hop path separates "I know this ticket resolved" (Kafka, durable event log) from "I need to deliver this email" (RabbitMQ, task queue with retry) — each system doing what it was built for.

**Why not Celery** (the original HLD wording): Celery adds a separate process, broker config, and worker management for what is, in Phase 3, a single email call. A plain `aio_pika` consumer running inside the same FastAPI service does the same job with far less infrastructure. Celery becomes justified if notification volume grows to the point of needing distributed workers — not yet.

**Scope boundary for Phase 3:** email only. WhatsApp requires Meta business verification (weeks, not minutes). SMS costs money per message. Email works on Brevo's free tier immediately and proves the full pipeline.

---

## 12. Security & Observability

- **AuthN/Z:** JWT at gateway; per-role RBAC (L1/L2/Supervisor/Admin) enforced in Agent Service.
- **Secrets:** Azure Key Vault (no secrets in code/env files committed).
- **Transport:** TLS everywhere; VPC-internal service-to-service.
- **PII:** encrypt sensitive fields at rest; restrict logs from leaking PII.
- **Tracing:** `request_id`/`ticket_id` as correlation ID across Gateway → Kafka → services → RabbitMQ → DB, enabling single-ticket end-to-end trace.
- **Debug tooling:** Kafka UI (offsets, lag, replay), RabbitMQ Management UI (queue depth, DLQ), structured logs, metrics dashboards.

---

## 13. Observability & NFR Verification

Section 3 states NFR targets (assignment <5s, bot <2s, 99.9% uptime, etc.) as **requirements**. This section defines how each is **measured and proven** — a design without measurement is a set of promises; a dashboard is evidence.

### 13.1 Three distinct observability capabilities (often conflated)

| Capability | Answers | Tool |
|---|---|---|
| **Metrics** | Numbers over time — latency, throughput, error rate | Prometheus + Grafana |
| **Logs** | What happened, per request, in text | Structured logs (JSON), Cloud Logging |
| **Traces** | The path one request took across services | OpenTelemetry → Azure Monitor & Application Insights (§12) |

Tracing is already specified (§12). This section closes the **metrics** gap — the piece that actually produces a provable number against each NFR.

### 13.2 Stack

**Prometheus** (metrics collection/storage) + **Grafana** (dashboards) — both open-source, free, self-hosted as two containers alongside the rest of the stack (local dev via docker-compose; same pattern in Azure). Each Python/Node service exposes a `/metrics` endpoint (`prometheus_client` / `prom-client`); Prometheus scrapes every service every ~15s; Grafana queries Prometheus and renders one panel per NFR.

Two NFRs are **already observable for free**, with no extra instrumentation:
- **Kafka consumer lag** — Kafka UI (already in the stack, §5.7).
- **RabbitMQ queue depth / DLQ size** — RabbitMQ Management UI (already in the stack, §5.8).

Everything else needs an explicit custom metric.

### 13.3 NFR → Metric mapping

| NFR (§3) | Metric | Type | How captured |
|---|---|---|---|
| Bot response <2s | `bot_response_duration_seconds` | Histogram | Wrap the bot call, record duration |
| Assignment <5s | `ticket_assignment_duration_seconds` | Histogram | Timestamp: ticket created → agent WebSocket ack |
| Throughput (10K tickets/min peak) | `tickets_created_total` | Counter | Increment per ticket; `rate()` in Grafana |
| Redis availability lookup <1ms | `redis_lookup_duration_seconds` | Histogram | Wrap the Redis call |
| SLA compliance % | `sla_breached_total` / `tickets_resolved_total` | Counter ratio | Two counters, ratio panel |
| Availability / uptime | `up` (Prometheus built-in) + Azure Container Apps health probes | Gauge | Standard scrape target health |
| Error rate | `http_requests_total{status=~"5.."}` | Counter | Standard middleware, all services |
| Kafka consumer lag | *(no custom metric needed)* | — | Kafka UI |
| RabbitMQ queue depth / DLQ | *(no custom metric needed)* | — | RabbitMQ Management UI |

### 13.4 Proving an NFR (the actual artifact, not just the claim)

1. Run the **k6 load test** (Phase 8) — generate real concurrent-ticket traffic.
2. Watch the **Grafana dashboard live** during the run — p50/p95/p99 per metric above.
3. Capture a **screenshot or short screen-recording** of the dashboard during that run.
4. That artifact — not the HLD text — is what goes in the GitHub README and gets shown in an interview.

> The difference this makes: *"we designed for <5 seconds"* is a claim. *"Here's the p95 assignment latency from my last load test: 3.2 seconds against a 5-second target"* is evidence. This is also the honest answer to *"how do you know your system meets its SLAs?"* — the standard senior-level probe on this topic.

### 13.5 Scope note

This is the same standard stack (or its managed-cloud equivalent, Azure Monitor) used at Browserstack/Postman/Freshworks/Razorpay-scale companies — not an exotic addition. For the portfolio build, Prometheus+Grafana run locally/self-hosted; production would typically swap to Cloud Monitoring/Managed Prometheus, with the same metric definitions.

---

## 14. System Design Key Concepts

How each core concept is addressed. Items marked **[gap-closed]** were not fully specified earlier and are detailed here.

| Concept | How it's handled |
|---------|------------------|
| **Scalability** | Stateless services auto-scale on Azure Container Apps; Kafka scales by partitions+consumers; PostgreSQL via replicas→sharding; Redis cluster; MongoDB native sharding (see §10). |
| **Latency vs Throughput** | Customer-facing paths tuned for **latency** (bot <2s, assignment <5s, Redis lookup <1ms); background paths tuned for **throughput** (Kafka batched millions/sec, notification workers, Azure Synapse Analytics batch). Explicit per-path SLAs. |
| **CAP Theorem** | Chosen **per component**: PostgreSQL = **CP** (correctness for money/state), Redis availability cache = **AP** (stale-for-seconds acceptable), Kafka = CP within a partition, MongoDB = tunable (AP default for conversations). |
| **ACID** | Enforced in PostgreSQL for ticket state + business actions (atomic multi-step, constraints, optimistic/pessimistic locking, WAL durability). Deliberately **not** applied to conversations (MongoDB), analytics (Azure Synapse Analytics), or cache (Redis). |
| **Rate Limiting [gap-closed]** | **Token bucket** at Gateway, keyed per customer/API-key (e.g., 100 req/min) with counters in Redis; **sliding-window** limit on Bot to cap LLM cost/abuse; outbound limits + **circuit breaker** on Business API Layer so we never hammer OMS/Payment. Standard `X-RateLimit-*` response headers. |
| **API Design [gap-closed]** | **Versioned** (`/v1/…`); consistent **error envelope** (`{code, message, request_id}`); **cursor pagination** on history/conversation lists; **idempotency keys** on all mutations (create ticket, refund); OpenAPI/Swagger auto-docs via FastAPI; rate-limit headers. |
| **Strong vs Eventual Consistency** | **Strong** where it must be immediate — ticket status, payment/refund state (PostgreSQL). **Eventual** where tolerable — Redis availability, Azure Synapse Analytics analytics, knowledge-base index, MongoDB replicas. |
| **Distributed Tracing [gap-closed]** | **OpenTelemetry** instrumentation → **Azure Monitor & Application Insights**; a **span per service hop**; `request_id`/`ticket_id` as the correlation ID stitching Gateway→Kafka→services→RabbitMQ→DB; traces correlated with structured logs and metrics for per-hop latency breakdown. |
| **Sync vs Async** | **Sync** where the user waits — customer↔bot, agent↔Business API (refund result), agent-assist RAG. **Async** everywhere else — ticket/conversation events (Kafka), customer notifications (RabbitMQ), supervisor alerts, analytics. |
| **Batch vs Stream** | **Stream** — Kafka consumers (sentiment, intent, summarizer), continuous SLA tracking, live conversation. **Batch** — nightly ticket categorization, SLA-risk model, Azure Synapse Analytics aggregation/reports. |
| **Fault Tolerance [gap-closed]** | Foundation (§11): idempotency keys, Kafka offset-commit-**after**-DB-commit, RabbitMQ DLQ + reassignment, retry-with-backoff, heartbeat recovery. **Added:** **circuit breaker** on external calls, **bulkhead** isolation between services, **graceful degradation** — if Bot/AI down → route straight to human agent; if Slack down → email fallback; **if Redis (availability) down → Assignment Service falls back to a Postgres read of agent status/load, so ticket assignment degrades in latency but never fails** (§9.4, implemented in Phase 3 Task 5), and Redis self-heals from server-driven heartbeats on restart — plus Azure Container Apps **health probes** + auto-restart. |

---

## 15. System Design Building Blocks

| Block | How it's used |
|-------|---------------|
| **Database** | Polyglot: PostgreSQL, MongoDB, Redis, Azure AI Search (vector), Azure Synapse Analytics (see §5.11). |
| **Horizontal vs Vertical Scaling** | **Horizontal** is primary (stateless services, partitions, replicas, shards). Vertical used only for measured bumps to the stateful DB tier. |
| **Caching** | Redis **cache-aside** for hot reads (agent/customer lookups); **write-through** for the active-conversation cache. |
| **Distributed Caching [gap-closed]** | Redis in **cluster mode**; keys TTL'd; **invalidation on write** for cached entities; **stampede prevention** via short lock + TTL jitter on hot keys. |
| **Load Balancing [gap-closed]** | Cloudflare at the edge; **Azure Front Door (L7)**; **Azure Container Apps' built-in load balancing** across instances with **health-probe-based routing**. |
| **SQL vs NoSQL** | Reasoned per access pattern (see §5.11): relational for transactional integrity, document/vector/columnar for the rest. |
| **Database Scaling** | Read **replicas** for read-heavy history/reporting; **sharding** (by customer/region) when write volume demands (see §10). |
| **Data Replication [gap-closed]** | **Master–slave, async** replication; **read/write split** (writes→primary, reads→replicas); consistency-critical reads routed to primary to dodge **replication lag**; automated **failover** (replica promotion). |
| **Data Redundancy [gap-closed]** | **Multi-AZ** deployment; **point-in-time recovery** backups; replication factor for Kafka/DB; explicit **RTO/RPO** targets per data tier (tight for tickets/payments, relaxed for analytics). |
| **Database Sharding** | Hash-based (`key % N`), shard by **resource** not user; the pattern is implemented hands-on in the companion assignment (see §10). |
| **Database Indexes [gap-closed]** | **B-Tree** for id/lookup columns; **composite** for multi-column WHERE (e.g., `user_id, status`); **partial** indexes on high-write tables to keep them small; **BRIN** for append-only time columns (`created_at`); kept minimal on write-heavy tables to protect write throughput. |
| **Proxy Server [gap-closed]** | **Cloudflare** = edge reverse proxy (DNS, WAF, DDoS); **Node.js Gateway** = application reverse proxy / BFF (auth, routing, aggregation). |
| **WebSocket** | Agent dashboard real-time; cross-instance delivery bridged via Redis pub/sub (see §9). |
| **API Gateway** | Node.js BFF — auth, rate limiting, validation, routing (see §5.1). |
| **Message Queues** | **Kafka** = event log/streaming; **RabbitMQ** = task dispatch/notifications — the deliberate split (see §5.7–5.8). |

---

## 16. Environments & Deployment

| Environment | Setup |
|-------------|-------|
| Local dev | docker-compose: all services + Kafka + RabbitMQ + Postgres + Redis |
| Staging | Azure Container Apps, Azure Database for PostgreSQL, Event Hubs (dev tier), CloudAMQP free tier |
| Production | + auto-scaling policies, Cloud Armor (DDoS), Cloud CDN, read replicas |

- **CI/CD:** GitHub Actions (or Azure DevOps Pipelines) → Azure Container Registry → Azure Container Apps deploy.
- **IaC (optional):** Terraform for reproducible infra.

---

## 17. Out of Scope (Phase 1) / Phase 2 Roadmap

**Phase 1 excludes:**
- Payments/settlement logic (only orchestrate via Business API Layer).
- Multi-region active-active (single region first; design leaves room).

**Phase 2 (designed-for, not yet built):**
- **Slack interactive actions** — supervisors approve refund / reassign / join chat directly from the Slack alert via signed button callbacks (see 5.14).
- **Ticket categorization + SLA-risk prediction** — batch LLM categorization and an ML breach-risk model feeding Azure Synapse Analytics (see 5.13).
- **Active conversation Redis cache** — write-through cache for very high concurrency (see 8.7).
- Local dev may stub Slack/email and mock the Business API Layer.

---

## 18. Interview One-Liner

> "Omnichannel bot-first CRM, deployed entirely on Azure — single vendor, sharing infrastructure with the Foundry AI plane, no cross-cloud networking. Node.js gateway at the edge; Python/FastAPI services for bot, assignment, agent, notifications; **Kafka as the event backbone** (Azure Event Hubs, Kafka-protocol-compatible) for audit/SLA/analytics and **RabbitMQ as the task broker** for priority dispatch, ack-timeout reassignment, and notifications; **Redis** (Azure Cache for Redis) for sub-ms agent availability. **Polyglot persistence** — PostgreSQL (Azure Database for PostgreSQL, ACID) for tickets/actions, MongoDB for high-volume conversation transcripts, Azure AI Search (vector) for RAG, Azure Synapse/Fabric for analytics. AI beyond the bot is additive via Kafka fan-out — auto-summarization, RAG agent-assist, intent classification, and sentiment-based escalation — so intelligence is added without destabilizing the transactional path. **React** microfrontend dashboard (Module Federation, same pattern as Myntra's Spectrum). Every NFR is **measured, not just claimed** — Prometheus/Grafana dashboards proved against real k6 load tests. Right tool per concern, stateless services scaling 20→1000+ on Azure Container Apps without re-architecture."