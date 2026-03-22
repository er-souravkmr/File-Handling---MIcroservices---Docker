# File Processing System

## Services
- API Gateway: `http://localhost:4000`
- Upload Service: internal on `4001`
- Worker Service: background consumer
- Notification Service (Socket.IO): `http://localhost:4003`

## Run
```
docker compose up --build
```

## Frontend
Open `http://localhost:4004`

## Upload Flow
1. Init
```
POST /upload/init
{
  "filename": "big.mp4",
  "totalChunks": 10,
  "mime": "video/mp4"
}
```

2. Send chunks
```
POST /upload/chunk
form-data:
  fileId
  chunkIndex
  totalChunks
  chunk: <binary>
```

3. Complete
```
POST /upload/complete
{
  "fileId": "..."
}
```

## Download Flow (Chunk-Based)
- Metadata
```
GET /files/:fileId/meta
```
- Chunk
```
GET /files/:fileId/chunk/:index
```
Frontend can fetch chunks in order and reconstruct the file.

## Events (Socket.IO)
Connect to `http://localhost:4003` (or `http://localhost:4000/socket.io` via gateway)
- `upload_progress` { fileId, received, total }
- `processing_status` { fileId, status, outputPath? }

## Storage
- Chunks: `./data/uploads/<fileId>/chunks/<index>`
- Merged: `./data/merged/<fileId>_<filename>`

## Redis Usage
- File metadata cache
- Rate limiting
