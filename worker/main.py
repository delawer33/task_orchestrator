import json
import os
import asyncio
import aio_pika
import argparse
import logging
from concurrent.futures import ProcessPoolExecutor
from config.settings import get_settings
from config.utils.rabbit import get_rabbit_connection
from task_processor import process_task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()
CPU_WORKERS = int(os.cpu_count() * 0.8)
IO_CONCURRENCY = 100
PREFETCH_MAP = {
    "cpu_intensive_tasks": 1,
    "io_bound_tasks": 50,
    "urgent_tasks": 5,
}


async def setup_queues(channel: aio_pika.abc.AbstractChannel):
    # exchange = await channel.declare_exchange(
    #     "orchestrator_exchange", aio_pika.ExchangeType.DIRECT, durable=True
    # )
    exchange = await channel.declare_exchange(
            "orchestrator_exchange",
            type="x-delayed-message",
            arguments={
                "x-delayed-type": aio_pika.ExchangeType.DIRECT
            },
            durable=True
        )
    cpu_queue = await channel.declare_queue(
        "cpu_intensive_tasks",
        durable=True,
        arguments={
            "x-max-priority": 10,
            "x-dead-letter-exchange": "dlx_exchange",
            "x-single-active-consumer": True,
        },
    )
    await cpu_queue.bind(exchange, routing_key="cpu_intensive_tasks")
    io_queue = await channel.declare_queue(
        "io_bound_tasks",
        durable=True,
        arguments={
            "x-max-priority": 10,
            "x-dead-letter-exchange": "dlx_exchange",
            "x-max-length": 10000,
        },
    )
    await io_queue.bind(exchange, routing_key="io_bound_tasks")
    urgent_queue = await channel.declare_queue(
        "urgent_tasks",
        durable=True,
        arguments={
            "x-max-priority": 10,
            "x-dead-letter-exchange": "dlx_exchange",
        },
    )
    await urgent_queue.bind(exchange, routing_key="urgent_tasks")
    dlx_exchange = await channel.declare_exchange(
        "dlx_exchange", aio_pika.ExchangeType.FANOUT, durable=True
    )
    delayed_queue = await channel.declare_queue(
        "delayed_tasks_queue",
        durable=True,
        arguments={
            "x-dead-letter-exchange": "orchestrator_exchange",
            "x-dead-letter-routing-key": "io_bound_tasks",
        },
    )
    await delayed_queue.bind(exchange, routing_key="delayed_tasks")

    dlx_queue = await channel.declare_queue("dead_letter_queue", durable=True)
    await dlx_queue.bind(dlx_exchange)
    return {
        "exchange": exchange,
        "cpu_queue": cpu_queue,
        "io_queue": io_queue,
        "urgent_queue": urgent_queue,
        "delayed_queue": delayed_queue,
    }


async def process_cpu_task(task: dict) -> dict:
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=CPU_WORKERS) as executor:
        return await loop.run_in_executor(executor, process_task, task)


async def process_io_task(task: dict) -> dict:
    return await asyncio.to_thread(process_task, task)


async def process_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    semaphore: asyncio.Semaphore,
    queue_name: str,
):
    print("!!!!!!!!!!!!!!!!!!!!!111")
    async with semaphore:
        try:
            async with message.process(requeue=False):
                task = json.loads(message.body.decode())
                logger.info(f"Processing task: {task['task_id']}")
                try:
                    task = json.loads(message.body.decode())
                    logger.info(
                        f"Processing {task['task_id']} from {queue_name}"
                    )
                    if queue_name == "cpu_intensive_tasks":
                        result = await asyncio.wait_for(
                            process_cpu_task(task), timeout=300.0
                        )
                    elif queue_name == "io_bound_tasks":
                        result = await process_io_task(task)
                    else:
                        result = await asyncio.wait_for(
                            process_io_task(task), timeout=30.0
                        )
                    logger.info(f"Task completed: {task['task_id']}")
                except asyncio.TimeoutError:
                    logger.error(f"Task timed out: {task['task_id']}")
                    await message.reject(requeue=False)
                except Exception as e:
                    logger.error(
                        f"Task failed: {task['task_id']}, error: {str(e)}"
                    )
                    if "metadata" not in task:
                        task["metadata"] = {}
                    task["metadata"]["retry_count"] = (
                        task["metadata"].get("retry_count", 0) + 1
                    )
                    updated_message_body = json.dumps(task).encode()
                    await message.reject(requeue=False)
        except Exception as e:
            logger.error(f"Message processing error: {str(e)}")


async def consume_queues(
    connection: aio_pika.RobustConnection,
    queues_to_consume: list[str],
    concurrency_settings: dict,
):
    PREFETCH_CONFIG = {
        "cpu_intensive_tasks": 1,
        "io_bound_tasks": 50,
        "urgent_tasks": 5,
    }
    channel = await connection.channel()
    await setup_queues(channel)
    async with connection:
        consumers = []
        for queue_name in queues_to_consume:
            channel = await connection.channel()
            prefetch = PREFETCH_CONFIG.get(queue_name, 1)
            await channel.set_qos(prefetch_count=prefetch, global_=False)
            queue = await channel.get_queue(queue_name)
            print(queue, "<<<<<<<<<<")
            concurrency = concurrency_settings.get(queue_name, 1)
            semaphore = asyncio.Semaphore(concurrency)
            consumer = queue.consume(
                lambda message, qn=queue_name, sem=semaphore: process_message(
                    message, sem, qn
                )
            )
            consumers.append(consumer)
            logger.info(
                f"Started consumer for '{queue_name}' "
                f"(prefetch={prefetch}, concurrency={concurrency})"
            )
        await asyncio.gather(*consumers)
        await asyncio.Future()


async def main():
    parser = argparse.ArgumentParser(description="Optimized worker")
    parser.add_argument(
        "--queues", type=str, default="cpu_intensive_tasks,io_bound_tasks"
    )
    parser.add_argument(
        "--concurrency",
        type=str,
        default="cpu_intensive_tasks:2,io_bound_tasks:50",
    )
    args = parser.parse_args()
    queues = [q.strip() for q in args.queues.split(",")]
    print("!!!!!!!!!!!!!!!!!!!!!", queues)
    concurrency_settings = {}
    for item in args.concurrency.split(","):
        if ":" in item:
            q, c = item.split(":")
            concurrency_settings[q.strip()] = int(c)
    if (
        "cpu_intensive_tasks" in queues
        and "cpu_intensive_tasks" not in concurrency_settings
    ):
        concurrency_settings["cpu_intensive_tasks"] = CPU_WORKERS
    while True:
        try:
            async with get_rabbit_connection() as connection:
                await consume_queues(connection, queues, concurrency_settings)
        except Exception as e:
            logger.error(f"Error: {str(e)}, restarting in 5s")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
