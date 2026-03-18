# ============================================================
# Stage 1: Install all deps, build, then prune devDeps
# (single npm install = faster than running two)
# ============================================================
FROM node:20-alpine AS builder

WORKDIR /app

# dumb-init for proper PID 1 / signal handling in containers
RUN apk add --no-cache dumb-init

COPY package.json ./

# Install ALL deps (devDeps needed for tsc / @nestjs/cli)
# --ignore-scripts skips the "prepare" husky hook
RUN npm install --ignore-scripts

# Copy source files needed for compilation
COPY tsconfig.json tsconfig.build.json nest-cli.json ./
COPY src/ ./src/
COPY config/ ./config/

# Compile TypeScript → dist/
RUN npm run build

# Remove devDependencies in-place so we can copy a clean node_modules
RUN npm prune --omit=dev --ignore-scripts


# ============================================================
# Stage 2: Lean production image
# ============================================================
FROM node:20-alpine AS production

WORKDIR /app

# Binary is already built into Alpine's node image; pull dumb-init from builder
COPY --from=builder /usr/bin/dumb-init /usr/bin/dumb-init

# Production-only node_modules (pruned in Stage 1)
COPY --from=builder /app/node_modules ./node_modules

# Compiled output  (dist/src/ + dist/config/)
COPY --from=builder /app/dist ./dist

# Minimal package metadata (needed by some NestJS internals)
COPY package.json ./

# Run as the built-in non-root 'node' user (uid 1000)
USER node

# Build-time arguments — passed from docker-compose build.args (sourced from .env)
ARG PORT=3004
ARG NODE_ENV=production
ARG ENVIRONMENT=production
ARG DEV_LOGS=false
ARG DATABASE_URL
ARG NATS_HOST
ARG NATS_PORT=4222
ARG NATS_USERNAME
ARG NATS_PASSWORD
ARG CLICKHOUSE_HOST
ARG CLICKHOUSE_PORT=8123
ARG CLICKHOUSE_USERNAME
ARG CLICKHOUSE_PASSWORD
ARG CLICKHOUSE_DATABASE=andes_db
ARG REDIS_HOST
ARG REDIS_PORT=9002
ARG REDIS_PASSWORD
ARG REDIS_DB=0
ARG REDIS_TTL=600
ARG REDIS_MAX_RETRIES=3
ARG REDIS_RETRY_DELAY=1000
ARG REDIS_QUEUE_DB=1
ARG USE_EVENT_QUEUE=true
ARG USE_SESSION_QUEUE=true
ARG USE_CONTRACTOR_QUEUE=true
ARG USE_ETL_QUEUE=true
ARG QUEUE_CONCURRENCY_EVENTS=5
ARG QUEUE_CONCURRENCY_SESSIONS=3
ARG QUEUE_CONCURRENCY_CONTRACTORS=2
ARG QUEUE_CONCURRENCY_ETL=1
ARG QUEUE_RETRY_ATTEMPTS=3
ARG QUEUE_RETRY_BACKOFF_TYPE=exponential
ARG QUEUE_RETRY_BACKOFF_DELAY=5000
ARG USE_INACTIVITY_ALERTS=true
ARG INACTIVITY_THRESHOLD_MINUTES=60
ARG INACTIVITY_SCAN_INTERVAL_MINUTES=10

# Promote ARGs to runtime ENV variables
ENV PORT=${PORT} \
    NODE_ENV=${NODE_ENV} \
    ENVIRONMENT=${ENVIRONMENT} \
    DEV_LOGS=${DEV_LOGS} \
    DATABASE_URL=${DATABASE_URL} \
    NATS_HOST=${NATS_HOST} \
    NATS_PORT=${NATS_PORT} \
    NATS_USERNAME=${NATS_USERNAME} \
    NATS_PASSWORD=${NATS_PASSWORD} \
    CLICKHOUSE_HOST=${CLICKHOUSE_HOST} \
    CLICKHOUSE_PORT=${CLICKHOUSE_PORT} \
    CLICKHOUSE_USERNAME=${CLICKHOUSE_USERNAME} \
    CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD} \
    CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE} \
    REDIS_HOST=${REDIS_HOST} \
    REDIS_PORT=${REDIS_PORT} \
    REDIS_PASSWORD=${REDIS_PASSWORD} \
    REDIS_DB=${REDIS_DB} \
    REDIS_TTL=${REDIS_TTL} \
    REDIS_MAX_RETRIES=${REDIS_MAX_RETRIES} \
    REDIS_RETRY_DELAY=${REDIS_RETRY_DELAY} \
    REDIS_QUEUE_DB=${REDIS_QUEUE_DB} \
    USE_EVENT_QUEUE=${USE_EVENT_QUEUE} \
    USE_SESSION_QUEUE=${USE_SESSION_QUEUE} \
    USE_CONTRACTOR_QUEUE=${USE_CONTRACTOR_QUEUE} \
    USE_ETL_QUEUE=${USE_ETL_QUEUE} \
    QUEUE_CONCURRENCY_EVENTS=${QUEUE_CONCURRENCY_EVENTS} \
    QUEUE_CONCURRENCY_SESSIONS=${QUEUE_CONCURRENCY_SESSIONS} \
    QUEUE_CONCURRENCY_CONTRACTORS=${QUEUE_CONCURRENCY_CONTRACTORS} \
    QUEUE_CONCURRENCY_ETL=${QUEUE_CONCURRENCY_ETL} \
    QUEUE_RETRY_ATTEMPTS=${QUEUE_RETRY_ATTEMPTS} \
    QUEUE_RETRY_BACKOFF_TYPE=${QUEUE_RETRY_BACKOFF_TYPE} \
    QUEUE_RETRY_BACKOFF_DELAY=${QUEUE_RETRY_BACKOFF_DELAY} \
    USE_INACTIVITY_ALERTS=${USE_INACTIVITY_ALERTS} \
    INACTIVITY_THRESHOLD_MINUTES=${INACTIVITY_THRESHOLD_MINUTES} \
    INACTIVITY_SCAN_INTERVAL_MINUTES=${INACTIVITY_SCAN_INTERVAL_MINUTES}

EXPOSE ${PORT}

# dumb-init ensures proper PID 1 handling and signal forwarding
# Entry point is dist/src/main (NestJS tsc output: src/ → dist/src/)
ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "dist/src/main"]
