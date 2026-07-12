#!/bin/bash
# Auto-run once, on first container start, against a fresh volume —
# same mechanism as schema.sql for Postgres, via
# /docker-entrypoint-initdb.d/. Mongo's official image sources any
# .sh file found there the very first time it initializes an empty
# data directory (alongside .js files, which run directly through
# mongosh instead of being wrapped in a shell script).
set -e

mongosh --eval '
db = db.getSiblingDB("crmplatform");
db.conversations.createIndex({ customer_id: 1, status: 1, created_at: -1 });
print("mongo-init.sh: conversations index created");
'