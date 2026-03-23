# File Processing System

A microservices-based file upload, processing, and download system with real-time progress notifications.

---

## Architecture

```
Browser (4004)
    │
    ▼
API Gateway (4000)          ← single entry point, JWT auth, reverse proxy
    │
    ├──▶ Upload Service (4001)   ← chunk upload, metadata, file assembly trigger
    │         │
    │         ├── Redis (6379)       ← metadata cache, chunk tracking, TTL
    │         └── RabbitMQ (5672)    ← publishes file_jobs queue
    │
    ├──▶ Notification Service (4003) ← Socket.IO, broadcasts events to browser
    │
    └──▶ Worker Service (background)
              │
              ├── RabbitMQ           ← consumes file_jobs queue
              ├── Redis              ← reads metadata
              └── Notification Service ← pushes processing_status events
```

### Services

| Service | Port | Description |
|---|---|---|
| api-gateway | 4000 | Reverse proxy, optional JWT auth, CORS |
| upload-service | 4001 (internal) | Chunked upload, metadata, download endpoints |
| worker-service | — | Background consumer, merges chunks |
| notification-service | 4003 | Socket.IO server, event broadcaster |
| frontend | 4004 | Vite + React UI |
| Redis | 6379 | Metadata cache, chunk set tracking |
| RabbitMQ | 5672 / 15672 | Job queue (management UI on 15672) |

### Upload Flow

```
1. POST /upload/init        → returns fileId
2. POST /upload/chunk       → send each chunk (multipart), repeat for all chunks
3. POST /upload/complete    → enqueues file_jobs message
4. Worker picks up job      → merges chunks → notifies via Socket.IO
```

### Storage Layout

```
data/
├── uploads/
│   └── <fileId>/
│       ├── meta.json
│       └── chunks/
│           ├── 0
│           ├── 1
│           └── ...
└── merged/
    └── <fileId>_<filename>
```

---

## Setup

### Prerequisites

- Docker Desktop
- Docker Compose v2

### Production

```bash
docker compose up --build
```

Open `http://localhost:4004`

### Development (hot reload)

```bash
docker compose -f docker-compose.dev.yml up
```

All services mount source files as volumes and run via nodemon. Frontend runs Vite dev server.

### Useful commands

```bash
# View logs for a specific service
docker compose logs -f worker-service

# Flush Redis (clears rate limit keys, metadata cache)
docker compose exec redis redis-cli FLUSHDB

# RabbitMQ management UI
open http://localhost:15672   # guest / guest
```

---

## API

All requests go through the gateway at `http://localhost:4000`.

### Health

```
GET /health
→ 200 { ok: true }
```

---

### Upload

#### Initialize upload

```
POST /upload/init
Content-Type: application/json

{
  "filename": "video.mp4",
  "totalChunks": 10,
  "mime": "video/mp4"
}

→ 200 { "fileId": "<uuid>" }
→ 400 { "error": "filename and valid totalChunks required" }
```

#### Upload chunk

```
POST /upload/chunk
Content-Type: multipart/form-data

Fields:
  fileId       string   — from init
  chunkIndex   number   — 0-based index
  totalChunks  number   — total number of chunks
  chunk        file     — binary chunk data (max 50 MB per chunk)

→ 200 { "ok": true, "received": 3, "total": 10 }
→ 400 { "error": "..." }
```

#### Complete upload

```
POST /upload/complete
Content-Type: application/json

{ "fileId": "<uuid>" }

→ 200 { "ok": true, "status": "queued" }
→ 400 { "error": "missing chunks", "received": 8, "total": 10 }
→ 503 { "error": "queue not ready" }
```

#### Upload status

```
GET /upload/status/:fileId

→ 200 {
    "fileId": "<uuid>",
    "totalChunks": 10,
    "received": [0, 1, 2, 3],
    "count": 4
  }
→ 404 { "error": "not found" }
```

---

### Files (Download)

#### File metadata

```
GET /files/:fileId/meta

→ 200 {
    "fileId": "<uuid>",
    "filename": "video.mp4",
    "mime": "video/mp4",
    "totalChunks": "10"
  }
→ 404 { "error": "not found" }
```

#### Download chunk

```
GET /files/:fileId/chunk/:index

→ 200  application/octet-stream  (binary chunk data)
→ 404 "File not found"
```

Fetch all chunks in order (0 to totalChunks-1) and concatenate to reconstruct the file.

---

### Real-time Events (Socket.IO)

Connect to `http://localhost:4003` or via gateway at `http://localhost:4000` (socket.io path proxied).

| Event | Payload | Description |
|---|---|---|
| `connected` | `{ ok: true }` | Fired on socket connection |
| `upload_progress` | `{ fileId, received, total }` | Fired after each chunk is saved |
| `processing_status` | `{ fileId, status, outputPath? }` | Fired by worker on status change |

`processing_status` values: `queued` → `processing` → `completed` / `failed`

---

## Environment Variables

| Variable | Service | Default | Description |
|---|---|---|---|
| PORT | all | varies | Listening port |
| RABBITMQ_URL | upload, worker | `amqp://rabbitmq:5672` | RabbitMQ connection |
| REDIS_URL | upload, worker | `redis://redis:6379` | Redis connection |
| NOTIFY_URL | upload, worker | `http://notification-service:4003` | Notification service URL |
| DATA_DIR | upload, worker | `/data` | Root path for file storage |
| AUTH_ENABLED | api-gateway | `false` | Enable JWT auth on proxied routes |
| JWT_SECRET | api-gateway | — | Secret for JWT verification |
| VITE_API_BASE | frontend | `http://127.0.0.1:4000` | Gateway URL used by browser |
