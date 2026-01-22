const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const si = require("systeminformation");
const { spawn } = require("child_process");

const app = express();
const server = http.createServer(app);

const PORT = 8080;

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

// Add a simple HTTP endpoint to verify server is running
app.get("/", (req, res) => {
  res.send("Agent is running.");
});

let latestGPUData = [];
let lastGPUUpdate = Date.now();

// Start Python GPU fetcher process with proper error handling
const pythonProcess = spawn("python", ["gpu_fetcher.py"], {
  cwd: __dirname,
  stdio: ["pipe", "pipe", "pipe"],
});

pythonProcess.stdout.on("data", (data) => {
  const lines = data.toString().trim().split("\n");

  lines.forEach((line) => {
    if (!line) return;

    try {
      const parsed = JSON.parse(line);

      // Handle status messages
      if (parsed.status) {
        if (parsed.status === "ready") {
          console.log("GPU Fetcher: ready - GPU monitoring active");
        } else if (parsed.status === "ready_no_gpu") {
          console.log(
            "GPU Fetcher: ready - No GPU detected (continuing without GPU metrics)",
          );
        } else {
          console.log(`GPU Fetcher: ${parsed.status}`);
        }
        return;
      }

      // Update GPU data (array of GPUs or empty array)
      if (Array.isArray(parsed)) {
        latestGPUData = parsed;
        lastGPUUpdate = Date.now();
      }
    } catch (e) {
      console.error("Failed to parse GPU data:", e.message, "- Raw:", line);
    }
  });
});

pythonProcess.stderr.on("data", (data) => {
  try {
    const parsed = JSON.parse(data.toString().trim());
    if (parsed.status === "no_gpu") {
      console.warn("GPU Fetcher:", parsed.message);
    } else if (parsed.error) {
      console.error("GPU Fetcher Error:", parsed.error);
    }
  } catch (e) {
    // Not JSON, just log as-is
    console.error("GPU Fetcher Error:", data.toString());
  }
});

pythonProcess.on("error", (err) => {
  console.error("Failed to start GPU fetcher:", err);
});

pythonProcess.on("exit", (code) => {
  if (code !== 0) {
    console.error(`GPU fetcher exited with code ${code} - Restarting...`);

    setTimeout(() => {
      console.log("Attempting to restart GPU fetcher...");
    }, 5000);
  }
});

// Monitor GPU data staleness
setInterval(() => {
  if (Date.now() - lastGPUUpdate > 5000) {
    console.warn("GPU data not updating - Python process may be hung");
  }
}, 5000);

io.on("connection", (socket) => {
  console.log(`Dashboard connected: ${socket.id}`);

  socket.on("ping_request", () => {
    socket.emit("pong_response", { time: Date.now() });
  });

  const intervalId = setInterval(async () => {
    try {
      const cpu = await si.currentLoad();
      const mem = await si.mem();

      const payload = {
        ts: Date.now(),
        cpu: { percent: Number(cpu.currentLoad).toFixed(2) },
        ram: {
          percent: Number(((mem.active / mem.total) * 100).toFixed(2)),
          used: Number((mem.active / 1024 ** 3).toFixed(2)),
          total: Number((mem.total / 1024 ** 3).toFixed(2)),
        },
        gpu: [...latestGPUData],
      };

      socket.volatile.emit("metrics", payload);
    } catch (err) {
      console.error(err);
    }
  }, 250);

  socket.on("disconnect", () => {
    clearInterval(intervalId);
    console.log(`Dashboard disconnected: ${socket.id}`);
  });
});

// Start server on all interfaces
server.listen(PORT, () => {
  console.log(`Socket.io is running on port ${PORT}`);
});

// Graceful shutdown
const cleanup = () => {
  console.log("\nShutting down gracefully...");
  pythonProcess.kill("SIGINT");
  io.close(() => {
    console.log("Server closed");
    process.exit(0);
  });

  setTimeout(() => {
    console.error("Force exiting...");
    process.exit(1);
  }, 2000);
};

process.on("SIGINT", cleanup);
process.on("SIGTERM", cleanup);
