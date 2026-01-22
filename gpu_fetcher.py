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
    global iteration

    try:
        pynvml.nvmlInit()
        print(json.dumps({"status": "ready"}), flush=True)
    except pynvml.NVMLError_LibraryNotFound:
        # No NVIDIA drivers installed - gracefully send empty GPU data
        error_msg = {"status": "no_gpu", "message": "NVIDIA drivers not found"}
        print(json.dumps(error_msg), file=sys.stderr, flush=True)

        # Send ready status as a proper status message (not mixed with array)
        status_msg = {"status": "ready_no_gpu"}
        print(json.dumps(status_msg), flush=True)
        time.sleep(0.1)

        # Keep sending empty GPU arrays
        while True:
            print(json.dumps([]), flush=True)
            time.sleep(POLL_INTERVAL)
    except Exception as e:
        # Other errors
        error_msg = {"error": f"NVML init failed: {str(e)}"}
        print(json.dumps(error_msg), file=sys.stderr, flush=True)

        # Send ready status
        status_msg = {"status": "ready_no_gpu"}
        print(json.dumps(status_msg), flush=True)
        time.sleep(0.1)  # Small delay to ensure status is processed

        # Keep sending empty GPU arrays instead of crashing
        while True:
            print(json.dumps([]), flush=True)
            time.sleep(POLL_INTERVAL)

    # Get GPU count and handles
    try:
        device_count = pynvml.nvmlDeviceGetCount()
        if device_count == 0:
            print(
                json.dumps({"error": "No GPUs detected"}), file=sys.stderr, flush=True
            )
            # Keep sending empty arrays
            while True:
                print(json.dumps([]), flush=True)
                time.sleep(POLL_INTERVAL)

        handles = [pynvml.nvmlDeviceGetHandleByIndex(i) for i in range(device_count)]
    except Exception as e:
        print(
            json.dumps({"error": f"Failed to get GPU handles: {str(e)}"}),
            file=sys.stderr,
            flush=True,
        )
        pynvml.nvmlShutdown()
        sys.exit(1)

    # Main loop
    while True:
        try:
            gpu_list = []
            iteration += 1

            for i, handle in enumerate(handles):
                # Get utilization rates
                utilization = pynvml.nvmlDeviceGetUtilizationRates(handle)

                # Get memory info
                mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)

                # Get temperature
                temp = pynvml.nvmlDeviceGetTemperature(
                    handle, pynvml.NVML_TEMPERATURE_GPU
                )

                # Get fan speed (may not be available on all GPUs)
                try:
                    fan_speed = pynvml.nvmlDeviceGetFanSpeed(handle)
                except pynvml.NVMLError:
                    fan_speed = None

                # Get power info
                try:
                    power_draw = (
                        pynvml.nvmlDeviceGetPowerUsage(handle) / 1000.0
                    )  # Convert mW to W
                    power_limit = (
                        pynvml.nvmlDeviceGetPowerManagementLimit(handle) / 1000.0
                    )
                except pynvml.NVMLError:
                    power_draw = None
                    power_limit = None

                # Get clock speeds
                try:
                    clock_graphics = pynvml.nvmlDeviceGetClockInfo(
                        handle, pynvml.NVML_CLOCK_GRAPHICS
                    )
                    clock_memory = pynvml.nvmlDeviceGetClockInfo(
                        handle, pynvml.NVML_CLOCK_MEM
                    )
                    clock_sm = pynvml.nvmlDeviceGetClockInfo(
                        handle, pynvml.NVML_CLOCK_SM
                    )
                except pynvml.NVMLError:
                    clock_graphics = clock_memory = clock_sm = None

                # Get GPU name and UUID
                name = pynvml.nvmlDeviceGetName(handle)
                uuid = pynvml.nvmlDeviceGetUUID(handle)

                gpu_list.append(
                    {
                        "id": i,
                        "uuid": uuid,
                        "name": name,
                        "load": round(
                            utilization.gpu / 100.0, 3
                        ),  # Convert to 0-1 range
                        "memoryUtil": round(
                            mem_info.used / mem_info.total, 3
                        ),  # 0-1 range
                        "memoryTotal": round(
                            mem_info.total / (1024**2), 2
                        ),  # Convert to MB
                        "memoryFree": round(mem_info.free / (1024**2), 2),
                        "memoryUsed": round(mem_info.used / (1024**2), 2),
                        "temperature": round(temp, 1),
                        "fanspeed": fan_speed,
                        "powerDraw": round(power_draw, 2)
                        if power_draw is not None
                        else None,
                        "powerLimit": round(power_limit, 2)
                        if power_limit is not None
                        else None,
                        "clocks": {
                            "graphics": clock_graphics,
                            "memory": clock_memory,
                            "sm": clock_sm,
                        },
                        "_metadata": {
                            "timestamp": time.time(),
                            "uptime": round(time.time() - start_time, 2),
                            "iteration": iteration,
                        },
                    }
                )

            print(json.dumps(gpu_list))
            sys.stdout.flush()

        except Exception as e:
            print(json.dumps({"error": str(e)}), file=sys.stderr, flush=True)

        time.sleep(POLL_INTERVAL)

    # Cleanup
    pynvml.nvmlShutdown()


if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    get_GPU_data()
