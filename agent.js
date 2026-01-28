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

// --- Static Info Store ---
let staticInfo = {
  cpu: {},
  os: {},
  mem: {},
  gpus: [],
  storage: [],
};
let latestGPUData = [];
let lastGPUUpdate = Date.now();
let gpuStaticInfoReceived = false;

// --- Collect static system information once at startup ---
const gatherStaticInfo = async () => {
  try {
    const [cpu, os, memLayout, memInfo, fs] = await Promise.all([
      si.cpu(),
      si.osInfo(),
      si.memLayout(),
      si.mem(),
      si.fsSize(),
    ]);

    console.log("Raw OS Info:", os);
    console.log("Memory Info:", memInfo);

    staticInfo.cpu = {
      manufacturer: cpu.manufacturer,
      brand: cpu.brand,
      speed: Number(cpu.speed) || 0,
      cores: Number(cpu.cores) || 0,
      physicalCores: Number(cpu.physicalCores) || 0,
    };
    staticInfo.os = {
      platform: os.platform,
      distro: os.distro,
      release: os.release,
      arch: os.arch,
    };

    // Set memory info using the awaited memInfo result
    staticInfo.mem = {
      total: memInfo.total,
      layout: memLayout.map((bank) => ({
        size: bank.size,
        type: bank.type,
        clockSpeed: bank.clockSpeed,
      })),
    };

    staticInfo.storage = fs
      .filter(
        (f) =>
          !["tmpfs", "devtmpfs", "overlay", "squashfs", "efivarfs"].includes(
            f.type,
          ) && f.size > 0,
      )
      .map((f) => ({
        name: f.fs,
        type: f.type,
        total: f.size,
        used: f.used,
      }));
    console.log("Static system info collected.");
    console.log(
      "Total RAM:",
      (staticInfo.mem.total / 1024 ** 3).toFixed(2),
      "GB",
    );
  } catch (e) {
    console.error("Failed to collect static system info:", e);
  }
};

gatherStaticInfo();

// --- Python GPU Fetcher Process ---
const pythonProcess = spawn("./metricVenv/bin/python", ["gpu_fetcher.py"], {
  cwd: __dirname,
  stdio: ["pipe", "pipe", "pipe"],
});

pythonProcess.stdout.on("data", (data) => {
  const lines = data.toString().trim().split("\n");

  lines.forEach((line) => {
    if (!line) return;

    try {
      const parsed = JSON.parse(line);

      // First message from Python script is the static GPU info
      if (parsed.status && !gpuStaticInfoReceived) {
        if (parsed.status === "ready" || parsed.status === "ready_no_gpu") {
          console.log(
            `GPU Fetcher: ${parsed.status} - ${parsed.gpus.length} GPUs detected.`,
          );
          staticInfo.gpus = parsed.gpus; // Store static GPU info
          gpuStaticInfoReceived = true;
        }
        return;
      }

      // Subsequent messages are arrays of dynamic GPU metrics
      if (Array.isArray(parsed)) {
        latestGPUData = parsed;
        lastGPUUpdate = Date.now();
      }
    } catch (e) {
      console.error(
        "Failed to parse data from Python:",
        e.message,
        "- Raw:",
        line,
      );
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
    console.error(`GPU fetcher exited with code ${code}.`);
  }
});

// Monitor GPU data staleness
setInterval(() => {
  if (gpuStaticInfoReceived && Date.now() - lastGPUUpdate > 5000) {
    console.warn("GPU data not updating - Python process may be hung");
  }
}, 5000);

// --- Socket.io Connection Handling ---
io.on("connection", (socket) => {
  console.log(`Dashboard connected: ${socket.id}`);

  // Send the collected static info to the newly connected client
  socket.emit("static-info", staticInfo);

  socket.on("ping_request", () => {
    socket.emit("pong_response", { time: Date.now() });
  });

  const intervalId = setInterval(async () => {
    try {
      const [cpu, mem, temp, procs] = await Promise.all([
        si.currentLoad(),
        si.mem(),
        si.cpuTemperature(),
        si.processes(), // Fetch process data
      ]);

      // Process and filter top services
      const topProcesses = procs.list
        .sort((a, b) => b.cpu - a.cpu) // Sort by CPU usage (descending)
        .slice(0, 10) 
        .map((p) => ({
          pid: p.pid,
          name: p.name,
          cpu: Number(p.cpu.toFixed(2)),
          mem: Number((p.memRss / (1024 * 1024)).toFixed(2)), // Convert to MB
          command: p.command,
        }));

      const payload = {
        ts: Date.now(),
        cpu: {
          percent: Number(cpu.currentLoad).toFixed(2),
          // Adding per-core load
          cores: cpu.cpus.map((c) => Number(c.load).toFixed(2)),
          temperature: temp.main ?? null,
        },
        ram: {
          percent: Number(
            (((mem.total - mem.available) / mem.total) * 100).toFixed(2),
          ),
          used: Number(((mem.total - mem.available) / 1024 ** 3).toFixed(2)),
          total: Number((mem.total / 1024 ** 3).toFixed(2)),
        },
        gpu: latestGPUData,
        processes: topProcesses,
      };

      socket.volatile.emit("metrics", payload);
    } catch (err) {
      console.error("Metric collection error:", err);
    }
  }, 250); // High frequency for dynamic data

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
