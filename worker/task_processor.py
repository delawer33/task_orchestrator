import time
import hashlib


def process_task(task: dict) -> dict:
    task_type = task["type"]
    if task_type == "fibonacci":
        n = task["parameters"].get("n", 10)
        result = fibonacci(n)
        return {"result": result}
    elif task_type == "hash":
        data = task["parameters"].get("data", "")
        return {"hash": hashlib.sha256(data.encode()).hexdigest()}
    elif task_type == "io_task":
        duration = task["parameters"].get("duration", 1)
        time.sleep(duration)
        return {"status": "completed"}
    elif task_type == "error_task":
        raise ValueError("Simulated processing error")
    else:
        raise ValueError(f"Unknown task type: {task_type}")


def fibonacci(n: int) -> int:
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)
