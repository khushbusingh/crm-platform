# CRM Platform HLD

## Overview
This repository contains a starter microservice scaffold for a CRM platform with a gateway, a PostgreSQL database, and domain services for bot, assignment, agent, and notification workflows.

## Architecture
- Gateway: Node.js/Express entrypoint
- Services: Python/FastAPI microservices
- Database: PostgreSQL with a basic schema
- Orchestration: Docker Compose for local development

## Execution Plan
1. Start the stack with Docker Compose.
2. Apply the initial schema to PostgreSQL.
3. Validate health endpoints for the gateway and services.
4. Extend the services with domain-specific APIs and integrations.
