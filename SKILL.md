---
name: crm-platform
description: >
  Reference for Khushbu's CRM platform build — an omnichannel (voice + chat)
  bot-first customer support system with event-driven microservices, AI
  enrichment, real-time agent assignment, and a microfrontend dashboard.
  Use this skill whenever the user asks to build, explain, review, extend,
  or debug ANY part of the CRM platform: the gateway, bot-service,
  assignment-service, agent-service, notification-service, business API
  layer, Kafka/RabbitMQ topology, PostgreSQL/MongoDB/Redis data model,
  the voice channel (LiveKit/Agent-S/SOP ingestion), the microfrontend
  dashboard, GCP/Azure deployment, or the 9-phase execution plan. Also use
  when the user references "the CRM project", "the assignment service",
  "the HLD", "the execution plan", "Phase 0/1/N", or docker-compose issues
  for this project.
---

# CRM Platform — Project Reference

## What This Is

An omnichannel customer-support CRM. Customer reaches out via **phone or
chat** → a **bot tries first** (RAG over an SOP knowledge base) → unresolved
tickets are **auto-assigned** to an agent by skill + availability + priority
→ agent resolves via **one-click business actions** (refund/cancel/exchange)
→ customer is notified (Email/WhatsApp/SMS). Supervisors get AI-enriched
Slack alerts on escalation/SLA-breach/angry-sentiment. Built as a portfolio
project **and** informed by real Myntra CRM/MS-Voice-Bot work — both
purposes matter, be honest about which parts are "designed" vs "built."

## Governing Instruction — How to Work With the User on This Project

**Give specs, checklists, and reviews. Do not write the code/config files
yourself unless explicitly asked to.** The user learns by building and
wants review afterward, not code handed to them. This was corrected
multiple times early in the build — treat it as a hard rule, not a
preference. Documents (HLD, execution plan, this skill file) are different
— those are fine to write/edit directly since they were explicitly
requested as documents, not as the hands-on build itself.

**Teaching style that actually lands with this user:** abstract, fragmented
explanations ("Part 1... Part 2... Part 3...") do NOT work — this was
explicitly called out as confusing more than once. What works: (1) one
unified narrative, not fragments, (2) an explicit table mapping
step → trigger → which store → exact operation (INSERT/UPDATE/DECR/etc.)
→ why, (3) a state diagram for any entity with a lifecycle, (4) a sequence
diagram showing ALL involved systems together in one picture, not one
system in isolation. When something still isn't landing, don't just
re-explain more abstractly — get MORE concrete (real table names, real
operations, a diagram) and always give the full connected picture, not a
partial slice.

When asked for "Phase N", default to: a structured spec/checklist (files
needed, config keys, table of what goes where, done-when criteria) — not
generated files — unless the user says "write it" / "generate it" / "give
me the code."

## Reference Files — Read Before Deep Work

- `docs/CRM_Platform_HLD.md` — 18 sections: requirements, every component +
  tech justification, frontend microfrontend architecture (§6), data flows
  (§8), session/transcript management (§9), scaling (§10), reliability +
  three named open gaps (§11/§11.1), security (§12), observability/NFR
  proof via Prometheus+Grafana (§13), system design concepts (§14) and
  building blocks (§15), deployment (§16), roadmap (§17).
- `docs/CRM_Execution_Plan.md` — 9-phase build sequence (0 through 9),
  critical path, minimum-viable-demo subset, de-risking notes, per-phase
  interview talking points.

Both docs have been corrected multiple rounds (renumbering, KRaft fix,
repo-strategy changes, gap additions) — treat them as current source of
truth over anything summarized here if they ever conflict.

## The 30-Second Mental Model

```
Customer (phone/chat) → Gateway (Node.js BFF) → Bot Service (Azure OpenAI
+ RAG) → resolved, or escalate → Assignment Service (skill+availability
+priority, Redis lookup) → RabbitMQ (agent's priority queue) → Agent
Service (WebSocket, <5s) → agent resolves via Business API Layer → Kafka
event → Notification Service (Brevo: email/WhatsApp/SMS) + Supervisor
Notification Service (Slack, on escalation/SLA/sentiment)
```

**Kafka = event backbone** ("what happened" — audit, SLA, analytics,
replay). **RabbitMQ = task broker** ("what to do next" — priority
dispatch, ack-timeout reassignment, notifications, DLQ). This split is
deliberate and is the single most-probed design decision.

**Important scope nuance: Kafka is NOT wired up until Phase 3.** Phase 1's
ticket-creation → assignment flow is direct/synchronous (Gateway →
Assignment Service, no event bus yet). Don't retrofit Kafka into Phase 1
explanations — the "full loop" mental model above is the Phase 3+ end
state, not what Phase 1 builds.

## Architecture At a Glance

| Layer | Tech | Role |
|---|---|---|
| Gateway | Node.js | BFF — auth, rate limit, routing only, no business logic, no DB access |
| Bot Service | Python/FastAPI + Azure OpenAI/Speech/AI Search | Voice+chat first responder, RAG (Phase 2+) |
| Assignment Service | Python/FastAPI | Skill+availability+priority routing |
| Agent Service | Python/FastAPI | WebSocket dashboard backend + business actions |
| Notification Service | Python/Celery | Customer Email/WhatsApp/SMS via Brevo (Phase 3+) |
| Supervisor Notification Service | Python/FastAPI | Internal Slack+email alerts, AI-enriched (Phase 4+) |
| Business API Layer | Python/FastAPI (mocked) | Order/Payment/Return/Exchange — circuit-breakered (Phase 6+) |
| Event backbone | Kafka (KRaft mode, no ZooKeeper) | tickets, conversations, agent-availability, sla-events, supervisor-alerts — **Phase 3+** |
| Task broker | RabbitMQ | agent.{id}.queue, unassigned.queue, notification queues, DLQs — **Phase 1+** (simple queue; TTL/DLQ/priority added Phase 3) |
| Hot state | Redis | agent availability (<1ms), sessions, WS↔instance mapping — **Phase 1+** |
| Source of truth | PostgreSQL (Cloud SQL) | tickets, agents, agent_actions — ACID — **Phase 1+** (agent_actions used from Phase 6) |
| Conversation transcripts | MongoDB | document-shaped, high-volume; audit team is MongoDB-skilled, not event-driven — this is the deciding reason — **Phase 2+** |
| Knowledge base | Azure AI Search (vector) | RAG for bot + agent-assist, shared across voice+chat |
| Analytics | BigQuery | fed from Kafka, off operational DBs |
| Frontend | React, Module Federation | shell + 3 remotes: agent-dashboard, customer-chat (dual-mode/embeddable), sop-onboarding — **Phase 9 ONLY, zero frontend before that** |

## The 9 Phases — Current Status

| # | Phase | Status |
|---|---|---|
| 0 | Foundation & infra | Done — 11 containers healthy, schema applied, 4 agents seeded |
| 1 | Core ticket spine | Fully designed, not yet built. See "Phase 1 Design" below. |
| 2 | Bot + escalation | Not started |
| 3 | Events + notifications (Kafka goes live here) | Not started |
| 4 | AI enrichment + supervisor alerts | Not started |
| 5A–5D | Voice channel (bridge → ingestion/SOP-UI → Agent-S → MCP tools) | Not started; underlying LiveKit+STT+TTS loop already works (pre-existing real Myntra work) |
| 6 | Business actions (mocked) | Not started |
| 7 | Cloud deployment (GCP+Azure) | Not started |
| 8 | Hardening + NFR proof (Prometheus/Grafana/k6) | Not started |
| 9 | Microfrontend conversion (hands-on, 4 separate repos) | Not started; deliberately last |

**Zero frontend until Phase 9** — every phase before it is verified via
`curl` / WebSocket CLI clients (`wscat`) / Kafka UI / RabbitMQ Management
UI / Grafana.

## Phase 0 — What Was Actually Built (Reference)

```
11 containers: postgres, redis, mongodb, kafka (KRaft), kafka-ui, rabbitmq,
gateway, bot-service, assignment-service, agent-service, notification-service
Each Python service: Dockerfile (python:3.12-slim) + requirements.txt
  (fastapi, uvicorn) + main.py exposing GET /health only
Gateway: Dockerfile (node:18-alpine) + package.json + src/app.js, /health only
schema.sql: agents, tickets, agent_actions tables + 4 seeded agents
docker-compose used `${VAR:-default}` pattern throughout for overridable
  credentials, and `depends_on: condition: service_healthy` everywhere
```

## Phase 1 Design — Core Ticket Spine (Speced, Ready to Build)

**Scope boundary — critical:** Phase 1 has NO Kafka, NO DLQ-based
reassignment, NO priority queues, NO notifications. Those are all Phase 3+.
Phase 1 is the linear happy path only.

**Ticket state subset for Phase 1:** `Open → Assigned → InProgress → Resolved`
(no reassignment loop yet — that's Phase 3's ack-timeout+DLQ mechanism).

**Flow:** `curl POST /tickets` with body `{customer_id, channel, type, priority}`
(Gateway, auth-stub, pure proxy, no DB) → Assignment Service does, in one
synchronous call: INSERT ticket (Postgres) → read Redis availability → UPDATE
ticket assigned (Postgres) → atomic `INCR` agent load (Redis) → publish to
`agent.{id}.queue` (RabbitMQ, plain queue, no TTL/priority yet) → Agent
Service consumer (single instance, holds WebSocket in memory, no
cross-instance Redis pub/sub bridge needed yet — that's only needed once Agent
Service scales horizontally) → pushes `{event: "ticket_assigned", ticket:
{...}}` over WebSocket → test client (`wscat`) sees it within 5s → `curl POST
/tickets/{id}/resolve` → UPDATE resolved (Postgres) + atomic `DECR` agent load
(Redis).

**Phase 1 ticket payload contract:** the create endpoint accepts
`customer_id`, `channel`, `type`, and `priority`. The ticket row should store
these fields plus the lifecycle fields needed for Phase 1 (`status`,
`assigned_to`, `created_at`, `updated_at`). In Phase 1, `assigned_to` is the
agent assignment reference for the ticket, not a generic user foreign key.

**Heartbeat (Phase 1 scope):** WS connect → Redis `status=available`,
`last_seen=now`. Client sends `{event:"heartbeat"}` every ~30s → updates
`last_seen`. Background sweep every 30-60s: `last_seen > 90s` →
`status=offline`. Clean disconnect → immediate offline.

**No new tables needed** — reuses `agents`/`tickets` from Phase 0's
`schema.sql`, but the `tickets` table must be shaped to support the Phase 1
create contract: `customer_id`, `channel`, `type`, `priority`, plus the
lifecycle columns for `status`, `assigned_to`, and timestamps.

## Key Decisions and Why (compressed — HLD has full reasoning)

- **Kafka vs RabbitMQ**: audit/replay/fan-out vs priority-dispatch/DLQ/retry. Never conflate them. Kafka doesn't arrive until Phase 3.
- **Redis over Postgres for availability**: Assignment Service hits this on every ticket; needs <1ms.
- **MongoDB for conversations, not Postgres+JSONB**: the deciding factor was organizational — the audit team's skillset is MongoDB queries, not Kafka consumers.
- **PostgreSQL is not owned by any cloud vendor** — community-governed (PGDG), permissive license. Cloud SQL / Azure Database for PostgreSQL both run vanilla Postgres — low lock-in. Don't confuse with Amazon Aurora (proprietary storage engine, real lock-in — not used here).
- **Kafka runs in KRaft mode, no ZooKeeper** — removed as of Confluent Platform 8.0.
- **Repo strategy**: backend is one monorepo. Frontend is the opposite — **4 separate repos**, deliberately, for genuine hands-on Module Federation practice, done entirely in Phase 9. **No frontend folder exists before Phase 9** — this was corrected mid-planning (originally a "simple React app" was planned for early phases; scrapped in favor of curl-only verification throughout).
- **Business API Layer is mocked** — real Myntra order/payment/OMS APIs aren't available outside work.
- **Observability is a proof mechanism** — HLD §13 maps every NFR to a Prometheus metric so claims become evidence (a Grafana screenshot from a k6 run), not assertions.

## Reliability Gaps — Now Documented in HLD §11.1 (Not Just Flagged)

These were surfaced by walking through the ticket-assignment flow end-to-end
and are now written into the HLD with fixes, not just noted as open:

1. **Dual-write problem** — ticket INSERT + Kafka publish (or assignment
   UPDATE + RabbitMQ publish) are two separate writes; if one fails after
   the other succeeds, the ticket goes silently missing downstream. **Fix:
   Transactional Outbox pattern** — write the event into an outbox table
   in the SAME Postgres transaction as the primary write; a poller/CDC
   publishes from there.
2. **Redis check-then-act race** — two Assignment Service instances can
   both read `agent:5:load=3`, both see room, both increment — real load
   should be 5, Redis shows 4. **Fix: atomic `INCR`/`DECR`**, or a Lua
   script for compound check+increment. Same shape as the SQL race
   conditions from the companion sharding/locking assignment — apply that
   same optimistic-locking instinct here.
3. **Ack-timeout not priority-aware + missing load decrement on
   reassignment** — RabbitMQ's per-agent-queue TTL is currently one fixed
   value for every priority; a P1 stuck behind a dead agent waits the full
   timeout. Also, when DLQ-reassignment pulls a ticket from a dead agent,
   that agent's Redis load must be decremented or they look permanently
   full. **Fix: tier the TTL by priority** (P1≈30-60s, P2≈2min, P3≈5min)
   and make reassignment explicitly decrement the original agent's load.

## Docker/Infra Gotchas Actually Hit During Phase 0

- **`bitnami/kafka` versioned tags (e.g. `3.7`) will fail to pull** —
  Broadcom removed free access to versioned Bitnami images (Aug/Sept
  2025). Use `confluentinc/cp-kafka:7.6.x` in KRaft mode instead (needs an
  explicit `CLUSTER_ID` env var, unlike Bitnami which auto-generates one).
- **RabbitMQ's `guest` user is loopback-only since v3.3** — connecting
  from another container (not literally localhost) gets
  `ACCESS_REFUSED`. Must set `RABBITMQ_DEFAULT_USER`/`RABBITMQ_DEFAULT_PASS`
  to a real non-guest user and update every service's `RABBITMQ_URL`
  accordingly.
- Both of these were caught during Phase 0 review before they became
  Phase 1 blockers — check any new compose/infra config against these two
  specifically.

## Mermaid Sequence Diagram Gotcha (Visualizer Tool, This Project)

When rendering `sequenceDiagram` (not `erDiagram`) via the Visualizer,
setting only generic `textColor`/`lineColor` in `themeVariables` leaves
actor-box labels invisible — sequence diagrams need their own explicit
set: `actorTextColor`, `actorBkg`, `actorBorder`, `actorLineColor`,
`signalColor`, `signalTextColor`, `labelBoxBkgColor`, `labelBoxBorderColor`,
`labelTextColor`, `noteBkgColor`, `noteBorderColor`, `noteTextColor`,
`activationBorderColor`, `activationBkgColor`, `sequenceNumberColor`. Hit
and fixed once already in this project — set all of these explicitly every
time, don't rely on inheritance from the generic ERD-style init block.

## Production Lessons Carried Over from the Real MS Voice Bot Build

- Provision **all** Azure AI resources in the **same subscription** as the
  runtime — cross-subscription network access is blocked and looks like
  an auth error.
- LiveKit + WebSocket is new infra (ingress, LB, network policy) — don't
  underestimate setup time even in a portfolio build.
- Langfuse was rejected for the real build (no India data region) → New
  Relic AIM. Cekura over Hamming for voice regression (call simulation,
  not just prompt regression).

## Repo Layout (backend monorepo; frontend repos separate, Phase 9 only)

```
crm-platform/                    (backend monorepo — currently exists)
├── docker-compose.yml           (Phase 0 — built, all containers healthy)
├── schema.sql                   (Phase 0 — built: agents, tickets, agent_actions)
├── gateway/                     Node.js — /health only so far
├── services/
│   ├── bot-service/             Python/FastAPI — /health only so far
│   ├── assignment-service/      Python/FastAPI — /health only so far
│   ├── agent-service/           Python/FastAPI — /health only so far
│   └── notification-service/    Python/FastAPI + Celery — /health only so far
└── docs/
    ├── CRM_Platform_HLD.md
    └── CRM_Execution_Plan.md

crm-shell/                       (Phase 9 — separate repo, not yet created)
crm-agent-dashboard-mfe/         (Phase 9 — separate repo, not yet created)
crm-customer-chat-mfe/           (Phase 9 — separate repo, dual-mode/embeddable, not yet created)
crm-sop-onboarding-mfe/          (Phase 9 — separate repo, not yet created)
```