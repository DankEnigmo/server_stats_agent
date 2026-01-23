import json
import os
import signal
import sys
import time

import pynvml

POLL_INTERVAL = float(os.getenv("GPU_POLL_INTERVAL", "0.5"))

start_time = time.time()
iteration = 0


def signal_handler(sig, frame):
    """shutdown on SIGINT/SIGTERM"""
    print(json.dumps({"status": "shutdown"}), file=sys.stderr, flush=True)
    try:
        pynvml.nvmlShutdown()
    except:
        pass
    sys.exit(0)


def get_GPU_data():
    """Initializes NVML, sends a single static GPU info payload, then enters a loop to send dynamic metrics."""
    global iteration

    handles = []
    try:
        pynvml.nvmlInit()
        device_count = pynvml.nvmlDeviceGetCount()

        if device_count == 0:
            # Consistent ready message even with no GPUs
            print(json.dumps({"status": "ready_no_gpu", "gpus": []}), flush=True)
            # Keep sending empty arrays so the agent doesn't hang
            while True:
                print(json.dumps([]), flush=True)
                time.sleep(POLL_INTERVAL)

        # Get static info for all GPUs
        static_gpu_info = []
        for i in range(device_count):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            handles.append(handle)
            mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
            static_gpu_info.append(
                {
                    "id": i,
                    "name": pynvml.nvmlDeviceGetName(handle),
                    "uuid": pynvml.nvmlDeviceGetUUID(handle),
                    "memoryTotal": round(mem_info.total / (1024**2), 2),  # MB
                }
            )

        # Send one-time static info payload
        print(json.dumps({"status": "ready", "gpus": static_gpu_info}), flush=True)
        time.sleep(0.1)  # Brief pause to ensure agent processes this message

    except pynvml.NVMLError_LibraryNotFound:
        # No NVIDIA drivers installed
        print(
            json.dumps({"status": "no_gpu", "message": "NVIDIA drivers not found"}),
            file=sys.stderr,
            flush=True,
        )
        print(json.dumps({"status": "ready_no_gpu", "gpus": []}), flush=True)
        while True:
            print(json.dumps([]), flush=True)
            time.sleep(POLL_INTERVAL)
    except Exception as e:
        # Other errors during initialization
        print(
            json.dumps({"error": f"NVML init failed: {str(e)}"}),
            file=sys.stderr,
            flush=True,
        )
        print(json.dumps({"status": "ready_no_gpu", "gpus": []}), flush=True)
        while True:
            print(json.dumps([]), flush=True)
            time.sleep(POLL_INTERVAL)

    # Main loop for dynamic metrics
    while True:
        try:
            dynamic_metrics = []
            iteration += 1

            for i, handle in enumerate(handles):
                utilization = pynvml.nvmlDeviceGetUtilizationRates(handle)
                mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                temp = pynvml.nvmlDeviceGetTemperature(
                    handle, pynvml.NVML_TEMPERATURE_GPU
                )

                dynamic_metrics.append(
                    {
                        "id": int(i),  # ID to link with static info
                        "load": round(utilization.gpu / 100.0, 3),  # 0-1 range
                        "memoryUtil": round(
                            mem_info.used / mem_info.total, 3
                        ),  # 0-1 range
                        "memoryUsed": round(mem_info.used / (1024**2), 2),  # MB
                        "temperature": round(temp, 1),
                    }
                )

            # Print the list of dynamic metrics
            print(json.dumps(dynamic_metrics), flush=True)

        except Exception as e:
            # Errors during the loop (e.g., GPU reset)
            print(
                json.dumps({"error": f"Metric collection failed: {str(e)}"}),
                file=sys.stderr,
                flush=True,
            )
            # We can try to re-initialize or just wait
            time.sleep(5)

        time.sleep(POLL_INTERVAL)

    # Cleanup (this part is unlikely to be reached in the current structure)
    try:
        pynvml.nvmlShutdown()
    except:
        pass


if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    get_GPU_data()
