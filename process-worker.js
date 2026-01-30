const { parentPort } = require('worker_threads');
const si = require('systeminformation');

let lastKnownGpus = [];


// Function to get top processes in worker thread
const getTopProcesses = async () => {
  try {
    const procs = await si.processes();
    
    // Process and filter top services
    const topProcesses = procs.list
      .sort((a, b) => b.cpu - a.cpu) // Sort by CPU usage (descending)
      .slice(0, 10)
      .map((p) => ({
        pid: p.pid,
        name: p.name,
        cpu: Number(p.cpu.toFixed(2)), // CPU is already a percentage
        mem: Number((p.memRss / (1024 * 1024)).toFixed(2)), // Convert to MB
        command: p.command,
      }));

    return topProcesses;
  } catch (error) {
    console.error('Error in process worker:', error);
    return [];
  }
};

// Execute the function and send result back to main thread
getTopProcesses()
  .then(result => {
    parentPort.postMessage(result);
  })
  .catch(error => {
    console.error('Worker error:', error);
    parentPort.postMessage([]);
<<<<<<< HEAD
  });
=======
  });

// Exit the worker after sending the result
process.exit(0);
>>>>>>> 69d4cfeb7e8a4faf23328250e051255208b8f048
