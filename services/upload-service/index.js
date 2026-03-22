const fs = require("fs");
const path = require("path");
const express = require("express");
const multer = require("multer");
const amqp = require("amqplib");
const Redis = require("ioredis");
const axios = require("axios");
const { v4: uuidv4 } = require("uuid");
const cors = require("cors");
const morgan = require("morgan");

const PORT = process.env.PORT || 4001;
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";
const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
const NOTIFY_URL = process.env.NOTIFY_URL || "http://notification-service:4003";
const DATA_DIR = process.env.DATA_DIR || "/data";

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

const redis = new Redis(REDIS_URL);

app.use(express.json({ limit: "5mb" }));
app.use(cors({ origin: true, credentials: true }));
app.use(morgan("tiny"));

let channel;

async function connectRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertQueue("file_jobs", { durable: true });
}

function fileDir(fileId) {
  return path.join(DATA_DIR, "uploads", fileId);
}

function chunksDir(fileId) {
  return path.join(fileDir(fileId), "chunks");
}

async function writeMeta(fileId, meta) {
  const dir = fileDir(fileId);
  await fs.promises.mkdir(dir, { recursive: true });
  await fs.promises.writeFile(path.join(dir, "meta.json"), JSON.stringify(meta));
}

async function readMeta(fileId) {
  const metaKey = `file:${fileId}`;
  const data = await redis.hgetall(metaKey);
  if (data && data.fileId) {
    console.log("Redis has Meta : ", { fileId, data });
    return data;
  }
  const metaPath = path.join(fileDir(fileId), "meta.json");
  const raw = await fs.promises.readFile(metaPath, "utf8");
  return JSON.parse(raw);
}

async function notify(event, data) {
  try {
    await axios.post(`${NOTIFY_URL}/notify`, { event, data });
  } catch (err) {
  }
}

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

app.post("/upload/init", async (req, res) => {
  const { filename, totalChunks, mime } = req.body || {};
  if (!filename || !totalChunks) {
    return res.status(400).json({ error: "filename and totalChunks required" });
  }
  console.log("upload/init", { filename, totalChunks, mime });
  const fileId = uuidv4();
  const meta = {
    fileId,
    filename,
    mime: mime || "application/octet-stream",
    totalChunks: String(totalChunks)
  };
  await redis.hset(`file:${fileId}`, meta);
  await writeMeta(fileId, meta);
  res.json({ fileId });
});

app.post("/upload/chunk", upload.single("chunk"), async (req, res) => {
  const { fileId, chunkIndex, totalChunks } = req.body || {};
  if (!fileId || chunkIndex === undefined || !req.file) {
    return res.status(400).json({ error: "fileId, chunkIndex and chunk file required" });
  }
  const dir = chunksDir(fileId);
  await fs.promises.mkdir(dir, { recursive: true });
  const chunkPath = path.join(dir, String(chunkIndex));
  if (!fs.existsSync(chunkPath)) {
    await fs.promises.writeFile(chunkPath, req.file.buffer);
  }

  const metaKey = `file:${fileId}`;
  if (totalChunks) {
    await redis.hset(metaKey, { totalChunks: String(totalChunks) });
  }
  await redis.sadd(`file:${fileId}:chunks`, String(chunkIndex));
  const received = await redis.scard(`file:${fileId}:chunks`);
  const meta = await redis.hgetall(metaKey);
  const total = Number(meta.totalChunks || totalChunks || 0);

  await notify("upload_progress", { fileId, received, total });

  res.json({ ok: true, received, total });
});

app.get("/upload/status/:fileId", async (req, res) => {
  const { fileId } = req.params;
  try {
    const meta = await readMeta(fileId);
    const receivedList = await redis.smembers(`file:${fileId}:chunks`);
    const received = receivedList.map((v) => Number(v)).sort((a, b) => a - b);
    res.json({
      fileId,
      totalChunks: Number(meta.totalChunks || 0),
      received,
      count: received.length
    });
  } catch (err) {
    res.status(404).json({ error: "not found" });
  }
});

app.post("/upload/complete", async (req, res) => {
  try {
    const { fileId } = req.body || {};
    if (!fileId) {
      return res.status(400).json({ error: "fileId required" });
    }
    if (!channel) {
      return res.status(503).json({ error: "queue not ready" });
    }
    const meta = await readMeta(fileId);
    const total = Number(meta.totalChunks || 0);
    const received = await redis.scard(`file:${fileId}:chunks`);
    if (received !== total) {
      return res.status(400).json({ error: "missing chunks", received, total });
    }

    const job = { fileId };
    channel.sendToQueue("file_jobs", Buffer.from(JSON.stringify(job)), { persistent: true });
    await notify("processing_status", { fileId, status: "queued" });
    res.json({ ok: true, status: "queued" });
  } catch (err) {
    res.status(500).json({ error: "complete failed" });
  }
});

app.get("/files/:fileId/meta", async (req, res) => {
  const { fileId } = req.params;
  try {
    const meta = await readMeta(fileId);
    res.json(meta);
  } catch (err) {
    res.status(404).json({ error: "not found" });
  }
});

app.get("/files/:fileId/chunk/:index", async (req, res) => {
  const { fileId, index } = req.params;
  const chunkPath = path.join(chunksDir(fileId), String(index));
  if (!fs.existsSync(chunkPath)) {
    return res.status(404).json({ error: "chunk not found" });
  }
  res.setHeader("Content-Type", "application/octet-stream");
  fs.createReadStream(chunkPath).pipe(res);
});

connectRabbit()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`upload-service listening on ${PORT}`);
    });
  })
  .catch((err) => {
    console.error("Failed to start upload-service", err);
    process.exit(1);
  });
