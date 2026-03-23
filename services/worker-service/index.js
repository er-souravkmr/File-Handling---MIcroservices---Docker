const fs = require("fs");
const path = require("path");
const amqp = require("amqplib");
const axios = require("axios");
const Redis = require("ioredis");

const RABBITMQ_URL = "amqp://rabbitmq:5672";
const REDIS_URL = "redis://redis:6379";
const NOTIFY_URL = "http://notification-service:4003";
const DATA_DIR = "/data";

const redis = new Redis(REDIS_URL);

function fileDir(fileId) {
  return path.join(DATA_DIR, "uploads", fileId);
}

function chunksDir(fileId) {
  return path.join(fileDir(fileId), "chunks");
}

function mergedDir() {
  return path.join(DATA_DIR, "merged");
}

async function notify(event, data) {
  try {
    await axios.post(`${NOTIFY_URL}/notify`, { event, data });
  } catch (err) {
    console.error("Failed to notify", err);
  }
}

async function readMeta(fileId) {
  const meta = await redis.hgetall(`file:${fileId}`);
  if (meta && meta.fileId) {
    return meta;
  }
  const metaPath = path.join(fileDir(fileId), "meta.json");
  const raw = await fs.promises.readFile(metaPath, "utf8");
  return JSON.parse(raw);
}

async function mergeChunks(fileId, totalChunks, filename) {
  const outDir = mergedDir();
  await fs.promises.mkdir(outDir, { recursive: true });
  const outPath = path.join(outDir, `${fileId}_${filename}`);
  const writeStream = fs.createWriteStream(outPath);

  try {
    for (let i = 0; i < totalChunks; i += 1) {
      const chunkPath = path.join(chunksDir(fileId), String(i));
      await new Promise((resolve, reject) => {
        const readStream = fs.createReadStream(chunkPath);
        readStream.pipe(writeStream, { end: false });
        readStream.once("end", resolve);
        readStream.once("error", reject);
        writeStream.once("error", reject);  
      });
    }

    await new Promise((resolve, reject) => {
      writeStream.end();
      writeStream.once("finish", resolve);
      writeStream.once("error", reject);
    });

  } catch (err) {
    writeStream.destroy();                          
    await fs.promises.unlink(outPath).catch(() => {}); 
    throw err;                                    
  }

  return outPath;
}

async function start(retries = 10, retryDelay = 3000) {
  let conn;

  for (let i = 0; i < retries; i++) {
    try {
      conn = await amqp.connect(RABBITMQ_URL, {
        heartbeat: 60,
        reconnect: true,
      });
      break;
    } catch (err) {
      console.log(`RabbitMQ not ready, retry ${i + 1}/${retries}...`);
      if (i < retries - 1) await new Promise(res => setTimeout(res, retryDelay));
    }
  }

  if (!conn) throw new Error("Could not connect to RabbitMQ after max retries");

  const channel = await conn.createChannel();

  await channel.assertQueue("file_jobs", {
    durable: true,
    arguments: {
      "x-message-ttl": 3600000,      
      "x-max-length": 1000,          
      "x-overflow": "reject-publish",  
    },
  });

  channel.prefetch(1); 


  conn.on("error", (err) => console.error("AMQP connection error:", err));
  conn.on("close", () => { console.error("AMQP connection closed"); process.exit(1); });

  console.log("worker-service ready, consuming file_jobs........");

  channel.consume("file_jobs", async (msg) => {
    if (!msg) return;

    let fileId;
    try {
      ({ fileId } = JSON.parse(msg.content.toString()));
    } catch {
      channel.nack(msg, false, false);
      return;
    }

    try {
      await notify("processing_status", { fileId, status: "processing" });

      const meta = await readMeta(fileId);
      const total = Number(meta.totalChunks) || 0;
      const filename = meta.filename || "file";

      if (total === 0) throw new Error(`No chunks found for fileId: ${fileId}`);

      const outputPath = await mergeChunks(fileId, total, filename);

      await notify("processing_status", { fileId, status: "completed", outputPath });
      channel.ack(msg);
    } catch (err) {
      console.error(`Job failed [fileId=${fileId}]:`, err.message);

      const retryCount = (msg.properties.headers?.["x-retry-count"] ?? 0);

      if (retryCount < 3) {
        channel.nack(msg, false, false);
        await channel.sendToQueue("file_jobs", msg.content, {
          persistent: true,
          headers: { "x-retry-count": retryCount + 1 },
        });
      } else {
        await notify("processing_status", { fileId, status: "failed" });
        channel.nack(msg, false, false);
      }
    }
  }, { noAck: false });
}



start().catch((err) => {
  console.error("worker-service failed", err);
  process.exit(1);
});
