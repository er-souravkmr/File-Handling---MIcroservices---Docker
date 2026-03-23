const fs = require("fs");
const path = require("path");
const os = require("os");
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
const FILE_JOBS_QUEUE = "file_jobs";
const FILE_JOBS_QUEUE_ARGS = {
  "x-message-ttl": 3600000,
  "x-max-length": 1000,
  "x-overflow": "reject-publish",
};

const app = express();
const upload = multer({
  storage: multer.diskStorage({ //Used DiskStorage to handle large files without buffering in memory
    destination: (req, file, cb) => cb(null, os.tmpdir()),
    filename: (req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`),
  }),
  limits: { fileSize: 50 * 1024 * 1024 }, 
});
const UPLOAD_TTL = 24 * 60 * 60;

const redis = new Redis(REDIS_URL);

app.use(express.json({ limit: "5mb" }));
app.use(cors({ origin: true, credentials: true }));
app.use(morgan("tiny"));

let channel;

async function connectRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertQueue(FILE_JOBS_QUEUE, {
    durable: true,
    arguments: FILE_JOBS_QUEUE_ARGS,
  });
  conn.on("error", (err) => console.error("AMQP connection error:", err));
  conn.on("close", () => console.error("AMQP connection closed"));
  channel.on("error", (err) => console.error("AMQP channel error:", err));
  channel.on("close", () => console.error("AMQP channel closed"));
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
  await fs.promises.writeFile(
    path.join(dir, "meta.json"),
    JSON.stringify(meta),
  );
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
  } catch (err) {}
}

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

app.post("/upload/init", async (req, res) => {
  const { filename, totalChunks, mime } = req.body || {};

  const total = parseInt(totalChunks, 10);
  if (!filename || isNaN(total) || total <= 0)
    return res.status(400).json({ error: "filename and valid totalChunks required" });

  const fileId  = uuidv4();
  const metaKey = `file:${fileId}`;
  const meta    = {
    fileId,
    filename,
    mime:        mime || "application/octet-stream",
    totalChunks: String(total),
  };

  // persist to redis and disk in parallel
  await Promise.all([
    redis.hset(metaKey, meta).then(() => redis.expire(metaKey, UPLOAD_TTL)),
    writeMeta(fileId, meta),
  ]);

  res.json({ fileId });
});

app.post("/upload/chunk", upload.single("chunk"), async (req, res) => {
  const { fileId, chunkIndex, totalChunks } = req.body || {};

  const idx   = parseInt(chunkIndex, 10);
  const total = parseInt(totalChunks, 10);

  if (!fileId || isNaN(idx) || idx < 0 || !req.file)
    return res.status(400).json({ error: "fileId, valid chunkIndex, and chunk file required" });

  if (!isNaN(total) && (total <= 0 || idx >= total))
    return res.status(400).json({ error: "Invalid totalChunks or chunkIndex out of range" });

  const dir       = chunksDir(fileId);
  const chunkPath = path.join(dir, String(idx));

  await fs.promises.mkdir(dir, { recursive: true });

  const exists = await fs.promises.access(chunkPath).then(() => true).catch(() => false);
  if (!exists) {
    await fs.promises.rename(req.file.path, chunkPath).catch(() =>
      fs.promises.copyFile(req.file.path, chunkPath)
        .then(() => fs.promises.unlink(req.file.path))
    );
  } else {
    await fs.promises.unlink(req.file.path).catch(() => {}); 
  }

  const metaKey  = `file:${fileId}`;
  const chunkKey = `file:${fileId}:chunks`;


  const pipeline = redis.pipeline();
  if (!isNaN(total)) pipeline.hset(metaKey, { totalChunks: String(total) });
  pipeline.expire(metaKey, UPLOAD_TTL);
  pipeline.sadd(chunkKey, String(idx));
  pipeline.expire(chunkKey, UPLOAD_TTL);
  pipeline.scard(chunkKey);
  const results = await pipeline.exec();

  const received     = results.at(-1)[1];         
  const meta         = await redis.hgetall(metaKey);
  const resolvedTotal = Number(meta?.totalChunks || total || 0);

  await notify("upload_progress", { fileId, received, total: resolvedTotal });
  res.json({ ok: true, received, total: resolvedTotal });
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
      count: received.length,
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
    channel.sendToQueue(FILE_JOBS_QUEUE, Buffer.from(JSON.stringify(job)), {
      persistent: true,
    });
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

  res.setHeader("Content-Type", "application/octet-stream");
  const stream = fs.createReadStream(chunkPath);

  stream.on("error", (err) => {
    res.status(404).send("File not found");
  });

  stream.pipe(res);
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
