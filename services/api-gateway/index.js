const express = require("express");
const { createProxyMiddleware } = require("http-proxy-middleware");
const jwt = require("jsonwebtoken");
const cors = require("cors");

const PORT =  4000;
const UPLOAD_SERVICE =  "http://upload-service:4001";
const NOTIFY_SERVICE =  "http://notification-service:4003";
const AUTH_ENABLED = false;
const JWT_SECRET = "sourav-kumar";

const app = express();
app.use(cors({ origin: true, credentials: true }));

function auth(req, res, next) {
  if (!AUTH_ENABLED) return next();
  const header = req.headers.authorization || "";
  const token = header.startsWith("Bearer ") ? header.slice(7) : null;
  if (!token) return res.status(401).json({ error: "missing token" });
  try {
    jwt.verify(token, JWT_SECRET);
    return next();
  } catch (err) {
    return res.status(401).json({ error: "invalid token" });
  }
}

app.get("/health", (req, res) => res.json({ ok: true }));

app.use(
  "/upload",
  auth,
  createProxyMiddleware({
    target: UPLOAD_SERVICE,
    changeOrigin: true,
    proxyTimeout: 120000,
    timeout: 120000,
    onError(err, req, res) {
      if (res.headersSent) return;
      res.status(502).json({ error: "upstream error", code: err.code || "proxy_error" });
    }
  })
);
app.use("/files", auth, createProxyMiddleware({ target: UPLOAD_SERVICE, changeOrigin: true }));
app.use("/notify", createProxyMiddleware({ target: NOTIFY_SERVICE, changeOrigin: true }));
app.use("/socket.io", createProxyMiddleware({ target: NOTIFY_SERVICE, changeOrigin: true, ws: true }));

app.listen(PORT, () => {
  console.log(`api-gateway listening on ${PORT}`);
});
