const http = require("http");
const express = require("express");
const { Server } = require("socket.io");
const cors = require("cors");

const PORT = process.env.PORT || 4003;

const app = express();
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: "1mb" }));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

io.on("connection", (socket) => {
  socket.emit("connected", { ok: true });
});

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

//For Now BroadcaSting for simplicity----
app.post("/notify", (req, res) => {
  const { event, data } = req.body || {};
  if (!event) {
    return res.status(400).json({ error: "event required" });
  }
  io.emit(event, data || {});
  res.json({ ok: true });
});

server.listen(PORT, () => {
  console.log(`notification-service listening on ${PORT}`);
});
