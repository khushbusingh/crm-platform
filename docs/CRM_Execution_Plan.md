# CRM Platform — Phased Execution Plan

**Companion to:** `CRM_Platform_HLD.md`
**Context:** Solo, part-time build. Goal is a **demoable, interview-ready** system — not 100% of the HLD shipped. Reuse the existing MS Voice Bot where possible; mock what can't be built solo (real Myntra business APIs).

---

## Guiding Principles

1. **Build the spine first, vertically.** A thin end-to-end slice (ticket → assign → agent → resolve) before adding any breadth. Never build "all of one layer" before the next.
2. **Every phase ends demoable.** Each phase produces something you can show on screen and talk about.
3. **Defer the hard/optional.** Voice, real business APIs, Slack interactive buttons, multi-region — later or mocked.
4. **Learn Python/FastAPI on the spine.** Phase 0–1 doubles as your FastAPI ramp; the spine is small enough to learn on.
5. **Design says more than you build.** The HLD specifies replication, tracing, circuit breakers, etc. Implement the impressive core; *design-and-stub* the rest and be honest about which is which.
6. **Repo strategy: backend monorepo, zero frontend until Phase 9.** Backend (gateway, services) lives in one repo — solo builder, no ownership boundaries to enforce, one `docker-compose up` for everything. **No React app at all through Phases 0–8** — every backend capability is verified with `curl`/HTTP client and the Kafka UI / RabbitMQ Management UI / Grafana already in the stack. The **microfrontend conversion (shell + 3 remotes, Module Federation, 4 separate repos)** is the **entire frontend build, done once, in Phase 9** — a focused, dedicated hands-on exercise, not something learned mid-flight while also learning FastAPI/Kafka/AI.
7. **Never reference or reuse real Myntra infrastructure, resource names, or credentials — anywhere.** All Azure resources (OpenAI, Speech, AI Search, Foundry) used in this build are personally-owned and standalone. Production names like `ms-voice-bot-llm-prod` may appear when *discussing* the real MS Voice Bot architecture for context/comparison, but must never be instructed as something to actually connect to or reuse in this codebase. Caught and fixed once already (Phase 5B's Foundry reference) — treat as a standing rule, not a one-time fix, especially from Phase 2B onward where this codebase first touches any real Azure service.

---

## Phase Overview

| Phase | Focus | Outcome (demoable) | Key tech introduced |
|-------|-------|--------------------|---------------------|
| 0 | Foundation & infra | All containers up, health checks green | docker-compose, Postgres, Kafka, RabbitMQ, Redis, Mongo |
| 1 | Core ticket spine | Ticket → assigned → agent sees <5s → resolves | FastAPI, WebSocket, Redis availability (verified via curl + WebSocket test client) |
| 2 | Bot + escalation | Bot resolves simple, escalates hard, full context on open | OpenAI/Azure OpenAI, RAG, MongoDB transcripts |
| 3 | Events + notifications | Full event-driven flow; customer gets email/WhatsApp/SMS; SLA tracked | Kafka topics, RabbitMQ DLQ, Brevo |
| 4 | AI enrichment + supervisor | AI summary/sentiment/intent; supervisor Slack alerts | AI consumers, Supervisor Notification Service, Slack webhook |
| 5A | Voice pipeline (partly done) | Phone call → STT → (bot) → TTS; searchable transcript | LiveKit + Azure Speech (STT/TTS **done**), telephony bridge, VAD |
| 5B | Voice knowledge + orchestration | SOP upload → ingested → bot answers from SOPs, escalates | Azure AI Foundry, AI Search, Blob, ingestion pipeline, Agent-S, SOP-upload UI |
| 6 | Business actions | Agent one-click refund/cancel/exchange | Business API Layer (mocked), circuit breaker, MCP-style tool server |
| 7 | Cloud deployment | Live URL on Azure | Azure Container Apps, Azure Database for PostgreSQL, Azure Cache for Redis, Azure Event Hubs, CloudAMQP |
| 8 | Hardening + polish + NFR proof | Tracing, metrics dashboards, load test, docs, demo script | OpenTelemetry, Prometheus, Grafana, k6, README, diagram |
| 9 | **Microfrontend conversion (hands-on, last)** | Same 3 UIs, now a shell + 3 independently-deployed remotes | Module Federation, 4 separate repos |

---

## Detailed Phases

### Phase 0 — Foundation & Infra
- **Repo strategy:** one **monorepo** for everything (`gateway/`, `services/`, `infrastructure/`, `docs/`) — see Guiding Principle 6. **No `dashboard/` folder yet** — zero frontend until Phase 9.
- **Build:** repo structure (gateway / services / dashboard / infra), `docker-compose` with Postgres, Kafka (KRaft mode — no ZooKeeper; removed as of Confluent Platform 8.0), Kafka-UI, RabbitMQ (mgmt), Redis, MongoDB. Schema + seed. Basic health-check endpoint per service.
- **Reuse/Stub:** none.
- **Milestone:** `docker-compose up` brings everything green; each service returns `/health`.
- **Interview value:** shows you can stand up a polyglot, multi-broker environment locally.

### Phase 1 — Core Ticket Spine (the critical path)
- **Build:** Gateway (auth stub + routing); **ticket creation** (chat text, no bot yet); **Assignment Service** (skill + availability + priority) writing to RabbitMQ; **Agent Service** (WebSocket + REST); PostgreSQL tickets/agents; Redis availability + heartbeat.
- **Reuse/Stub:** auth is a simple JWT stub; no bot yet (customer message → ticket directly). **No UI at all** — verify entirely with `curl`/HTTP client for REST, and a CLI WebSocket client (`wscat`, or a 10-line Python `websockets` script) to watch the agent's socket receive the ticket push.
- **Milestone:** `curl -X POST /api/ticket` creates a ticket → `wscat` connected as "agent 1" prints the assignment push in <5s → `curl -X POST /api/agent/resolve` flips status → `curl -X GET /api/ticket/{id}` confirms `resolved`. **This is your first real demo — a terminal, not a browser, and that's fine.**
- **Interview value:** microservices + real-time assignment + WebSocket + Redis availability — the heart of the system. Proven via API contract, not UI polish — arguably a *stronger* backend signal, since nothing is hidden behind a frontend.

### Phase 2 — Bot Layer + Escalation
- **Build:** chat bot (Azure OpenAI) with **RAG** over Azure AI Search; escalation after N attempts / on opt-out; **conversation storage in MongoDB**; full-context load on agent ticket open (summary + history + past convos). Verified via `curl`/HTTP client sending a sequence of chat messages — no UI (see Guiding Principle 6).
- **Reuse/Stub:** knowledge base can start with a small FAQ set.
- **Milestone:** bot answers a simple query and resolves; a hard query escalates and the agent opens with full context.
- **Interview value:** bot-first, RAG, document-store transcripts, context assembly.

### Phase 3 — Event Backbone + Notifications
- **Build:** Kafka topics (`tickets`, `conversations`, `agent-availability`, `sla-events`); **SLA tracker** consumer (warn at 75%, breach at 100%); customer **Notification Service** (RabbitMQ → Brevo email/WhatsApp/SMS); **reassignment via DLQ**; **no-agent queue** with customer ETA.
- **Reuse/Stub:** Brevo free tier; WhatsApp may be sandbox.
- **Milestone:** resolve a ticket → customer receives email + SMS; let an assignment time out → it reassigns; breach an SLA → event fires.
- **Interview value:** event-driven architecture, Kafka vs RabbitMQ split, DLQ resilience, SLA tracking.

### Phase 4 — AI Enrichment + Supervisor Alerts
- **Build:** Kafka consumers — **Auto-Summarizer**, **Sentiment Analyzer** (angry → raise priority), **Intent Classifier** (routing hint); **Agent Assist** (RAG suggestions in dashboard); **Supervisor Notification Service** consuming `supervisor-alerts` → **Slack webhook** + email, with AI summary + suggested action.
- **Reuse/Stub:** reuse Azure OpenAI from Phase 2.
- **Milestone:** angry message auto-escalates and pings a Slack channel with an AI summary; agent sees suggested replies.
- **Interview value:** AI as an *additive* layer via Kafka fan-out; internal-vs-customer notification separation.

### Phase 5 — Voice Channel (into the CRM)

The voice bot is the CRM's **voice channel**, not a separate product. It shares the same tickets, conversations store, assignment, and agent handoff as chat. Sequenced into sub-phases because only the media loop is done today.

**Current state (done):** LiveKit real-time loop working — you speak → **STT** → **LLM** → **TTS** → you hear a reply. Turn-taking works. The LLM answers **free-form** (its own knowledge), with **no grounding, no SOP execution, no backend calls** yet.

#### Phase 5A — Bridge into the CRM (make the loop a real channel)
- **Build:** persist every turn (STT + TTS text) into the shared **MongoDB `conversations`** store with `channel='phone'`; create/attach a **ticket** on call start; add **VAD** tuning (barge-in, silence threshold) if not already; on "talk to agent" or low confidence, **escalate** — transfer to the same Assignment Service used by chat.
- **Reuse:** your existing LiveKit + STT/TTS loop.
- **Milestone:** a phone/voice session shows up as a ticket, its transcript is searchable exactly like chat, and it can escalate to an agent.
- **Interview value:** omnichannel unification — voice and chat converge on one ticket/conversation model.

#### Phase 5B — Knowledge Ingestion + SOP Upload UI (grounding)
- **Build:** the ingestion pipeline so the bot answers from **your SOPs**, not free-form: an **SOP-upload endpoint** (`curl -F "file=@sop.pdf" /api/sop/upload` — no UI yet, see Guiding Principle 6) → file lands in **Azure Blob** → **Azure AI Search** pipeline (**Skillset → Index → Indexer**) with **hybrid retrieval (BM25 + vector)** using `text-embedding-3-large`; bot does **RAG** over this at answer time. This is the same knowledge base the chat bot (Phase 2) uses — build once, both channels share it.
- **Reuse:** your own, personally-owned Azure AI Foundry deployment — mirrors the production pattern structurally (same role: hosts the chat model + embedding model deployments) but shares no actual infrastructure, subscription, or credentials with real Myntra systems. **Never reference or reuse actual production resource names/credentials in this portfolio build.**
- **Milestone:** `curl` an SOP file to the upload endpoint → within the indexer cycle a `curl` question to the bot returns an answer grounded in that SOP, with the source retrievable.
- **Interview value:** RAG ingestion pipeline end-to-end, self-serve knowledge onboarding, one KB across voice+chat.
- **Production lesson to bank (from the real build):** provision **all** Azure AI resources in the **same subscription** as the runtime — cross-subscription network access is blocked and masquerades as auth errors.

#### Phase 5C — Agent-S Orchestration (deterministic SOP execution)
- **Build:** replace free-form replies with the **Agent-S** state machine so the bot *navigates* an SOP rather than chatting. Three LLM roles: **StateLLM** (GPT-4.1 Mini, temp 0 — decide next action every turn), **ActionLLM** (GPT-4.1 — generate data to execute an action), **UserInteractionLLM** (GPT-4.1 — interpret/validate user input). A **TriageAgent** (GPT-4.1 Mini) classifies intent up front; a **complexity classifier** routes simple queries to a single LLM call and complex ones (returns/exchange) into the multi-step state machine.
- **Reuse:** prompts modeled on `state_llm_prompt` / `action_llm_prompt` / `user_interaction_llm_prompt`.
- **Milestone:** a returns/exchange query runs as a guided multi-step flow (deterministic, no hallucinated steps); a greeting resolves in one call.
- **Interview value:** the differentiator — LLM as **controlled SOP navigator**, not free-form chat; cost/latency control via model tiering + complexity routing.

#### Phase 5D — MCP Tool Server (backend actions from voice)
- **Build:** an **MCP-style tool-execution server** (fastmcp + Streamable HTTP / JSON-RPC) that the ActionLLM calls to hit backend systems (order/return/exchange/pickup). For the portfolio build these are the **same mocked Business API Layer** from Phase 6; in the real Myntra build they are Thanos/RMS/OMS via OAuth2 client-credentials, with a **cache-aware layer** (aiocache, ~5-min TTL).
- **Reuse:** shares the Business API Layer / mocks with the agent-facing actions (Phase 6) — one integration surface for both voice-bot and human agents.
- **Milestone:** voice bot completes an order-status or return end-to-end by calling a tool, then confirms verbally.
- **Interview value:** tool-calling agent, shared integration layer across bot and human, caching for backend protection.

> **Sequencing within Phase 5:** 5A first (make the working loop a real CRM channel), then 5B (grounding — also unblocks/aligns with the chat bot's KB), then 5C (orchestration), then 5D (actions). 5B's knowledge base is shared with Phase 2's chat bot, so if Phase 2 is done, 5B partly overlaps — build the KB once.

### Phase 6 — Business Actions
- **Build:** **Business API Layer** (mock Order/Payment/Return/Exchange/Customer with realistic responses); agent **one-click actions** (refund/cancel/exchange); `agent_actions` audit + Kafka `agent-actions`; **circuit breaker** + timeouts on external calls.
- **Reuse/Stub:** all external systems mocked (real Myntra APIs unavailable outside work — state this clearly).
- **Milestone:** agent clicks "Initiate Refund" → mock service responds → audit logged → customer notified.
- **Interview value:** integration layer, saga/compensation thinking, circuit breaker, "what agents actually do."

### Phase 7 — Cloud Deployment (Azure, Single Cloud)
- **Build:** containers → Azure Container Registry → **Azure Container Apps**; **Azure Database for PostgreSQL**, **Azure Cache for Redis**, **Azure Event Hubs** (Kafka-compatible), **CloudAMQP** (RabbitMQ), MongoDB Atlas; Azure Key Vault; Azure Front Door + Application Gateway; health probes. Same Azure account already hosts the Foundry AI plane from Phase 2B — no cross-cloud networking, single vendor.
- **Reuse/Stub:** managed free tiers where available.
- **Milestone:** a **live URL** you can open in the interview.
- **Interview value:** real Azure deployment, managed services end-to-end (compute, DB, cache, Kafka-compatible streaming, secrets, LB), single-vendor simplicity vs the earlier two-cloud draft.

### Phase 8 — Hardening + Polish + Docs (NFR Proof)
- **Build:**
  - **Metrics stack:** Prometheus + Grafana containers; `/metrics` endpoint on each service (Bot, Assignment, Agent, Notification); custom metrics per NFR — `bot_response_duration_seconds`, `ticket_assignment_duration_seconds`, `tickets_created_total`, `redis_lookup_duration_seconds`, `sla_breached_total`/`tickets_resolved_total`, standard HTTP error-rate metric. (Kafka lag and RabbitMQ queue depth are already free via their existing UIs — no extra work.)
  - **Grafana dashboard:** one panel per NFR from HLD §3, mapped 1:1 (see HLD §13).
  - **OpenTelemetry → Cloud Trace** (span per hop, `request_id` correlation).
  - **Rate limiting** (token bucket) + API versioning/pagination/error envelope.
  - **Load test with k6**, run **while the Grafana dashboard is live** — this is the actual NFR-proof step, not a separate afterthought.
  - Alert **deduplication** in Supervisor service.
  - README + architecture diagram (draw.io) + **3-minute demo script**.
- **Milestone:** trace one ticket end-to-end; **run k6, capture a screenshot/recording of the live dashboard showing real p95 numbers against each NFR target** (e.g., "assignment p95 = 3.2s vs 5s target"); repo reads clean.
- **Interview value:** the difference between *"we designed for <5 seconds"* and *"here's the dashboard from my load test proving it"* — this is the artifact that turns claimed NFRs into evidence. See HLD §13 for the full NFR→metric mapping.

### Phase 9 — Microfrontend Conversion (hands-on Module Federation, deliberately last)

Everything through Phase 8 was built and verified with `curl`, WebSocket CLI clients, and the Kafka/RabbitMQ/Grafana UIs — **zero frontend so far** (Guiding Principle 6). This phase is where the frontend gets built, **for the first time, directly as microfrontends** — there's no "monolith to convert," just three known REST/WebSocket API contracts (agent queue/actions, customer chat, SOP upload) to build UIs against, each as its own remote from day one, plus a shell that composes them.

- **Build:**
  - `crm-shell` (repo 1) — host app: routing, auth/RBAC guards, shared design-system/auth-context as Module Federation singletons, page switcher.
  - `crm-agent-dashboard-mfe` (repo 2) — build the agent queue/live-chat/actions UI fresh against the existing Agent Service REST + WebSocket API; expose it via `ModuleFederationPlugin`; own dev server/port.
  - `crm-customer-chat-mfe` (repo 3) — build the chat widget fresh against the existing Bot Service/Gateway API; **build the dual-mode** explicitly (renders inside the shell **and** is standalone-embeddable via script tag, per HLD §6.3) — this is the one piece of hands-on value that doesn't exist yet even conceptually.
  - `crm-sop-onboarding-mfe` (repo 4) — build the upload/status UI fresh against the existing SOP-upload endpoint (Phase 5B).
  - Wire the shell to fetch each `remoteEntry.js` from its own dev server URL at runtime (not a build-time import) — this is the actual thing to feel, not just read about.
- **Reuse:** all business logic, REST endpoints, and WebSocket contracts already exist and are already verified (via `curl`/CLI clients) from Phases 1/2/5B — this phase is pure frontend build against known, working contracts, not new backend functionality.
- **Milestone:** stop the agent-dashboard-mfe dev server → shell shows a graceful fallback, other two remotes keep working (proves independence). Change something in `agent-dashboard-mfe` and redeploy it alone → shell picks it up without a shell rebuild.
- **Interview value:** this is your strongest, most direct resume continuity — the same Module Federation pattern as Myntra's Spectrum platform, hands-on, with the added nuance of a dual-mode embeddable widget.
- **Why last, deliberately:** doing this first would mean learning FastAPI/Kafka/AI *and* a new frontend pattern simultaneously — needless stacked risk (see De-risking Notes below). Sequencing it last means the backend is stable and this becomes a focused, low-risk, high-signal exercise on ground you already mostly own.

---

## Minimum Viable Interview Demo (if time-constrained)

If you can't finish everything, this subset still tells a complete, impressive story:

**Phases 0 → 1 → 2 → 3 → 4 → 7** (deploy) **+ partial 8** (README + demo script).

- **Voice (Phase 5):** the LiveKit STT↔LLM↔TTS loop already works — show it. **5A** (make it a real CRM channel) is a cheap, high-impact add even in the minimum path. **5B–5D** (ingestion/Agent-S/MCP) can be described-and-deferred if time-constrained. Note 5B's knowledge base is shared with the Phase 2 chat bot — build the KB once, both channels use it.
- Skip/mock: **Phase 6** — keep business actions mocked and minimal (the voice MCP tool server in 5D shares this same layer).
- **Real consequence of "zero frontend until Phase 9":** if Phase 9 is skipped, the entire demo (Phases 0–8) is `curl`/WebSocket-CLI + Kafka UI + RabbitMQ UI + Grafana — no visual product screen at all. That's a legitimate, credible **backend/systems interview story** ("here's the API contract, here's the dashboard proving the SLA"), but it is a different story from "here's a live product." Be deliberate about which story fits the room: for a backend/platform-leaning interview, curl+dashboards is fine and arguably sharper; for a full-stack story, Phase 9 is what makes it demoable as a product, not just a system.
- If Phase 9 is skipped, this path still demonstrates: microservices, event-driven, Kafka+RabbitMQ, WebSocket real-time, RAG bot, AI enrichment, Slack alerts, a working voice loop, live on Azure, and NFRs proven on a dashboard — a strong senior backend + AI story, just not a visual product demo.

---

## Deferred (Phase-2 Roadmap — designed, not built)

- **Slack interactive actions** (approve/reassign from Slack) — callback signing + authorization.
- **Ticket categorization + SLA-risk ML** — batch → Azure Synapse Analytics.
- **Active-conversation Redis cache** — write-through for very high concurrency.
- **DB replication / multi-AZ / sharding** — Azure Database for PostgreSQL's flexible server gives baseline (zone-redundant HA, read replicas); full custom setup is design-and-stub.
- **Distributed tracing depth, chaos/failover drills** — beyond a portfolio build.

State these as *"designed for, deliberately deferred"* — it shows judgment, not gaps.

---

## Critical Path & Sequencing

```
Phase 0 (infra)
   ↓
Phase 1 (spine)  ←── everything depends on this
   ↓
Phase 2 (bot + KB) ──► Phase 3 (events+notif) ──► Phase 4 (AI+supervisor)
   │  \                        ↓
   │   \__ shares KB __        Phase 5 (voice channel)
   │                  \          5A bridge → 5B ingest+SOP UI
   ↓                   \──────►  → 5C Agent-S → 5D MCP tools
Phase 6 (business actions) ◄── shares Business API Layer with 5D
   ↓
Phase 7 (deploy)  ←── after core is stable
   ↓
Phase 8 (harden + docs)
   ↓
Phase 9 (MFE conversion — hands-on, last, entirely optional for demo)
```

- **Phase 1 is the bottleneck** — nothing works until the spine works. Invest here.
- **Voice is sub-sequenced (5A→5B→5C→5D).** 5A (bridge the working loop into the CRM) is cheap and high-value. 5B's knowledge base is **shared with the Phase 2 chat bot** — build it once. 5D's tool server **shares the Business API Layer with Phase 6** — build that integration surface once, use it for both bot and human agents.
- **Phases 5 and 6 overlap on shared components** — sequence so shared pieces (KB, Business API Layer) are built once, not twice.
- **Deploy (7) only after the core is stable** — don't debug locally *and* in the cloud at once.
- **Phase 9 is deliberately isolated at the end** — it depends only on the REST/WebSocket API contracts from Phases 1, 2, and 5B already existing and being stable (verified via curl throughout), and nothing else in the plan depends on it. It's a standalone, low-risk exercise, not a dependency for anything.

---

## De-risking Notes

- **Learn on the spine, not the fancy parts.** If FastAPI/Kafka is new, Phase 1–3 is where you build fluency; don't start with voice or AI.
- **One new technology per phase where possible** — avoid stacking unknowns.
- **Keep a running demo.** After each phase, record a 60-sec screen capture — it becomes your interview material and proves progress.
- **Be explicit about built vs designed.** In interviews: *"the HLD specifies X; the running build implements Y and stubs Z."* Never overclaim.
- **Mock boundaries cleanly.** The Business API Layer and (optionally) Slack/email should be swappable mocks so local dev never blocks on external systems.

---

## Per-Phase Interview Talking Points

| Phase | The line you can say |
|-------|----------------------|
| 1 | "Microservices with real-time assignment — Redis for sub-ms availability, WebSocket to the agent in under 5 seconds." |
| 2 | "Bot-first with RAG over a vector store; conversations in MongoDB because they're document-shaped and append-heavy." |
| 3 | "Event-driven on Kafka for audit/SLA, RabbitMQ for task dispatch and notifications with DLQ-based reassignment." |
| 4 | "AI is additive via Kafka fan-out — summarization, sentiment, intent — without touching the transactional path; supervisors get enriched Slack alerts." |
| 5A | "Omnichannel — the LiveKit voice loop bridges into the same ticket + conversation model as chat, so voice calls are searchable and escalate identically." |
| 5B | "Self-serve SOP upload feeds an Azure AI Search ingestion pipeline — Blob → skillset → index → indexer — with hybrid BM25+vector retrieval; one knowledge base grounds both voice and chat." |
| 5C | "Agent-S runs the LLM as a deterministic SOP navigator, not free-form chat — three roles (state/action/interaction) with a triage + complexity classifier tiering models for cost and latency." |
| 5D | "An MCP-style tool server lets the bot execute backend actions (order/return/exchange), sharing the same integration layer human agents use, with a cache-aware layer protecting backends." |
| 6 | "Agents resolve with one-click business actions behind a circuit-breakered integration layer; every action is audited — the same layer the voice bot calls via MCP." |
| 7 | "Deployed entirely on Azure — Container Apps, managed Postgres/Redis/Event Hubs/RabbitMQ, sharing the same account as the Foundry AI plane. Single vendor, no cross-cloud networking." |
| 8 | "Every NFR is measured, not claimed — Prometheus/Grafana dashboards, load-tested with k6, plus end-to-end tracing with OpenTelemetry and token-bucket rate limiting." |
| 9 | "The same three UIs, converted into a Module Federation shell with three independently deployable remotes — the same pattern as Myntra's Spectrum platform — including a customer-chat widget that's dual-mode: embedded here, and standalone-embeddable via script tag elsewhere." |
