const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const si = require("systeminformation");
const { spawn } = require("child_process");
const { Worker } = require("worker_threads");

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

<<<<<<< HEAD
// Monitor GPU data staleness
setInterval(() => {
  if (gpuStaticInfoReceived && Date.now() - lastGPUUpdate > 5000) {
    console.warn("GPU data not updating - Python process may be hung");
  }
}, 5000);
=======
// Global variables for process monitoring
let cachedProcessData = [];
let lastProcessUpdate = 0;
const PROCESS_UPDATE_INTERVAL = 5000; // Update process data every 5 seconds

// Function to get process data on demand
const getProcessData = () => {
  return new Promise((resolve, reject) => {
    const worker = new Worker("./process-worker.js");

    worker.on("message", (result) => {
      resolve(result);
    });

    worker.on("error", (error) => {
      console.error("Process worker error:", error);
      reject(error);
    });

    worker.on("exit", (code) => {
      if (code !== 0) {
        console.error(`Process worker exited with code ${code}`);
      }
    });
  });
};
>>>>>>> 69d4cfeb7e8a4faf23328250e051255208b8f048

// --- Socket.io Connection Handling ---
io.on("connection", (socket) => {
  console.log(`Dashboard connected: ${socket.id}`);

  // Send the collected static info to the newly connected client
  socket.emit("static-info", staticInfo);

  socket.on("ping_request", () => {
    socket.emit("pong_response", { time: Date.now() });
  });

  // Cache for process data to reduce CPU-intensive calls
  let cachedProcessData = [];
  let lastProcessUpdate = 0;
  const PROCESS_UPDATE_INTERVAL = 2000; // Update process data every 2 seconds instead of every 250ms

  const intervalId = setInterval(async () => {
    try {
<<<<<<< HEAD
      // Collect basic metrics (CPU, RAM, temp) every 250ms
      const [cpu, mem, temp] = await Promise.all([
        si.currentLoad(),
        si.mem(),
        si.cpuTemperature()
      ]);
=======
      // Collect critical metrics (CPU and RAM) every 1000ms
      const [cpu, mem] = await Promise.all([si.currentLoad(), si.mem()]);
>>>>>>> 69d4cfeb7e8a4faf23328250e051255208b8f048

      // Update process data only when needed using worker thread
      const now = Date.now();
      if (now - lastProcessUpdate > PROCESS_UPDATE_INTERVAL) {
        // Create a worker thread to collect process data without blocking the main thread
        const worker = new Worker('./process-worker.js');

        worker.on('message', (result) => {
          cachedProcessData = result;
          lastProcessUpdate = now;
        });

        worker.on('error', (error) => {
          console.error('Process worker error:', error);
        });

        // Ensure worker is terminated after processing
        worker.on('exit', (code) => {
          if (code !== 0) {
            console.error(`Process worker exited with code ${code}`);
          }
        });
      }

      // Update process data only when needed
      if (now - lastProcessUpdate > PROCESS_UPDATE_INTERVAL) {
        try {
          const processData = await getProcessData();
          cachedProcessData = processData;
          lastProcessUpdate = now;
        } catch (error) {
          console.error("Error getting process data:", error);
        }
      }

      const payload = {
        ts: Date.now(),
        cpu: {
          percent: Number(Number(cpu.currentLoad).toFixed(2)),
          // Adding per-core load
          cores: cpu.cpus.map((c) => Number(Number(c.load).toFixed(2))),
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
        processes: [],
      };

      socket.emit("metrics", payload);
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
