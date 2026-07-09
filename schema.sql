-- ═════════════════════════════════════════════════════════
-- CRM Platform — Phase 0/1 schema
-- Source of truth for transactional data (HLD §5.11)
-- ═════════════════════════════════════════════════════════

CREATE TABLE agents (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  role VARCHAR(50) NOT NULL,           -- l1, l2, supervisor, admin
  skills TEXT[] NOT NULL DEFAULT '{}', -- e.g. {billing, technical, refund}
  status VARCHAR(50) NOT NULL DEFAULT 'offline', -- available, busy, away, offline
  max_load INT NOT NULL DEFAULT 5,
  current_load INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE tickets (
  id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL,
  channel VARCHAR(50) NOT NULL,        -- chat, phone
  type VARCHAR(50) NOT NULL,           -- billing, technical, refund, delivery
  priority VARCHAR(10) NOT NULL DEFAULT 'P3', -- P1, P2, P3
  status VARCHAR(50) NOT NULL DEFAULT 'open', -- open, assigned, resolved, closed
  assigned_agent_id INT REFERENCES agents(id),
  bot_handled BOOLEAN NOT NULL DEFAULT false,
  sla_deadline TIMESTAMP,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  resolved_at TIMESTAMP
);

CREATE TABLE agent_actions (
  id SERIAL PRIMARY KEY,
  ticket_id INT REFERENCES tickets(id),
  agent_id INT REFERENCES agents(id),
  action VARCHAR(100) NOT NULL,        -- initiate_refund, cancel_order, etc.
  request_payload JSONB,
  response_payload JSONB,
  status VARCHAR(50),                  -- success, failed
  created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes (see HLD §14 — B-Tree for lookups, kept minimal on write-heavy tables)
CREATE INDEX idx_tickets_agent_id ON tickets(assigned_agent_id);
CREATE INDEX idx_tickets_status ON tickets(status);
CREATE INDEX idx_agent_actions_ticket_id ON agent_actions(ticket_id);

-- Seed: a handful of agents to assign against in Phase 1
INSERT INTO agents (name, email, role, skills, status, max_load) VALUES
  ('Priya Sharma',  'priya@crm.local',  'l2', '{billing,refund}',        'available', 5),
  ('Amit Kumar',    'amit@crm.local',   'l1', '{technical}',             'available', 5),
  ('Neha Verma',    'neha@crm.local',   'l1', '{delivery,technical}',    'available', 5),
  ('Rahul Singh',   'rahul@crm.local',  'supervisor', '{billing,refund,technical,delivery}', 'available', 10);