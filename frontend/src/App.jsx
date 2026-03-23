import React, { useEffect, useMemo, useState } from "react";
import { io } from "socket.io-client";

const API = import.meta.env.VITE_API_BASE || "http://127.0.0.1:4000";

export default function App() {
  const [file, setFile] = useState(null);
  const [chunkSizeMb, setChunkSizeMb] = useState(5);
  const [uploadStatus, setUploadStatus] = useState("Idle");
  const [uploadProgress, setUploadProgress] = useState(0);
  const [events, setEvents] = useState([]);
  const [downloadId, setDownloadId] = useState("");
  const [downloadStatus, setDownloadStatus] = useState("Idle");
  const [downloadProgress, setDownloadProgress] = useState(0);

  const socket = useMemo(() => io(API), []);

  useEffect(() => {
    socket.on("connected", () => pushEvent("socket connected"));
    socket.on("upload_progress", (d) =>
      pushEvent(`upload_progress ${d.fileId} ${d.received}/${d.total}`),
    );
    socket.on("processing_status", (d) => {
      pushEvent(`processing_status ${d.fileId} ${d.status}`);
      if (d.status === "completed") setUploadStatus(`Done ✓ ${d.fileId}`);
      if (d.status === "failed") setUploadStatus(`Processing failed`);
    });

    return () => {
      socket.disconnect();
    };
  }, [socket]);

  function pushEvent(text) {
    setEvents((prev) => [text, ...prev].slice(0, 50));
  }

  //Upload logic
  async function initUpload(fileToUpload, totalChunks) {
    const res = await fetch(`${API}/upload/init`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filename: fileToUpload.name,
        totalChunks,
        mime: fileToUpload.type || "application/octet-stream",
      }),
    });
    if (!res.ok) throw new Error("init failed");
    return res.json();
  }

  async function uploadChunk(fileId, chunkIndex, totalChunks, chunk) {
    const form = new FormData();
    form.append("fileId", fileId);
    form.append("chunkIndex", String(chunkIndex));
    form.append("totalChunks", String(totalChunks));
    form.append("chunk", chunk);
    const res = await fetch(`${API}/upload/chunk`, {
      method: "POST",
      body: form,
    });
    if (!res.ok) throw new Error("chunk failed");
    return res.json();
  }

  async function uploadChunkWithRetry(
    fileId,
    chunkIndex,
    totalChunks,
    chunk,
    tries = 3,
  ) {
    let lastErr;
    for (let i = 0; i < tries; i += 1) {
      try {
        return await uploadChunk(fileId, chunkIndex, totalChunks, chunk);
      } catch (err) {
        lastErr = err;
        await new Promise((r) => setTimeout(r, 500 * (i + 1)));
      }
    }
    throw lastErr;
  }

  async function handleUpload() {
    if (!file) return;

    const chunkSize = Math.max(1, Number(chunkSizeMb || 5)) * 1024 * 1024;
    const totalChunks = Math.ceil(file.size / chunkSize);

    setUploadStatus("Initializing...");
    setUploadProgress(0);

    try {
      const { fileId } = await initUpload(file, totalChunks);
      setUploadStatus(`Uploading ${fileId}`);

      for (let i = 0; i < totalChunks; i += 1) {
        const start = i * chunkSize;
        const chunk = file.slice(start, start + chunkSize);
        await uploadChunkWithRetry(fileId, i, totalChunks, chunk, 3);
        setUploadProgress(Math.round(((i + 1) / totalChunks) * 100));
      }

      await completeUpload(fileId);
      setUploadStatus(`Queued ${fileId}`);
      setDownloadId(fileId);
    } catch (err) {
      setUploadStatus("Upload failed");
    }
  }

  async function completeUpload(fileId) {
    const res = await fetch(`${API}/upload/complete`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fileId }),
    });
    if (!res.ok) throw new Error("complete failed");
    return res.json();
  }

  //Download logic
  async function downloadChunk(fileId, index) {
    const res = await fetch(`${API}/files/${fileId}/chunk/${index}`);
    if (!res.ok) throw new Error("chunk download failed");
    return res.arrayBuffer();
  }

  async function downloadFile(fileId) {
    const metaRes = await fetch(`${API}/files/${fileId}/meta`);
    if (!metaRes.ok) throw new Error("meta not found");
    const meta = await metaRes.json();
    const total = Number(meta.totalChunks || 0);

    const parts = [];
    for (let i = 0; i < total; i += 1) {
      const buf = await downloadChunk(fileId, i);
      parts.push(buf);
      setDownloadProgress(Math.round(((i + 1) / total) * 100));
    }

    const blob = new Blob(parts, {
      type: meta.mime || "application/octet-stream",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = meta.filename || "file";
    a.click();
    URL.revokeObjectURL(url);
  }

  async function handleDownload() {
    const fileId = downloadId.trim();
    if (!fileId) return;
    setDownloadStatus("Downloading...");
    setDownloadProgress(0);
    try {
      await downloadFile(fileId);
      setDownloadStatus("Done");
    } catch (err) {
      setDownloadStatus("Download failed");
    }
  }

  return (
    <div className="wrap">
      <h1>File Processor Demo</h1>

      <div className="card">
        <div className="label">Choose file</div>
        <input type="file" onChange={(e) => setFile(e.target.files[0])} />

        <div className="row">
          <div className="label">Chunk size (MB)</div>
          <input
            type="number"
            min="1"
            value={chunkSizeMb}
            onChange={(e) => setChunkSizeMb(e.target.value)}
          />
        </div>

        <div className="row">
          <button onClick={handleUpload}>Upload</button>
        </div>

        <div className="row">
          <div className="progress-wrap">
            <div
              className="progress-bar"
              style={{ width: `${uploadProgress}%` }}
            />
          </div>
          <div className="progress-label">
            <span className="status">{uploadStatus}</span>
            <span>{uploadProgress}%</span>
          </div>
        </div>
      </div>

      <div className="card">
        <div className="label">Download by fileId</div>
        <input
          type="text"
          placeholder="fileId"
          value={downloadId}
          onChange={(e) => setDownloadId(e.target.value)}
        />
        <div className="row">
          <button onClick={handleDownload}>Download</button>
        </div>
        <div className="row">
          <div className="progress-wrap">
            <div
              className="progress-bar"
              style={{ width: `${downloadProgress}%` }}
            />
          </div>
          <div className="progress-label">
            <span className="status">{downloadStatus}</span>
            <span>{downloadProgress}%</span>
          </div>
        </div>
      </div>

      <div className="card">
        <div className="label">Live events</div>
        <div className="events">
          {events.map((e, i) => (
            <div key={`${e}-${i}`}>{e}</div>
          ))}
        </div>
      </div>
    </div>
  );
}
