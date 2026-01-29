const { parentPort } = require("worker_threads");
const si = require("systeminformation");

// Function to get top processes in worker thread
const getTopProcesses = async () => {
  try {
    const procs = await si.processes();

    // Process and filter top services
    const topProcesses = procs.list
      .sort((a, b) => b.cpu - a.cpu) 
      .slice(0, 10)
      .map((p) => ({
        pid: p.pid,
        name: p.name,
        cpu: Number(p.cpu.toFixed(2)), 
        mem: Number((p.memRss / (1024 * 1024)).toFixed(2)), 
        command: p.command,
      }));

    return topProcesses;
  } catch (error) {
    console.error("Error in process worker:", error);
    return [];
  }
};

// Continuous process monitoring
const PROCESS_UPDATE_INTERVAL = 5000; // 5 seconds

const sendProcessUpdate = async () => {
  try {
    const topProcesses = await getTopProcesses();
    parentPort.postMessage(topProcesses);
  } catch (error) {
    console.error("Error sending process update:", error);
    parentPort.postMessage([]);
  }
};

// Send initial update
sendProcessUpdate();

// Set up interval to send updates
const intervalId = setInterval(sendProcessUpdate, PROCESS_UPDATE_INTERVAL);

// Listen for messages from the main thread
parentPort.on("message", (message) => {
  if (message === "STOP") {
    clearInterval(intervalId);
    parentPort.close();
  }
});

// Handle worker exit
parentPort.on("close", () => {
  clearInterval(intervalId);
});
