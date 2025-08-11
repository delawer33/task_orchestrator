import json
import os
import asyncio
import aio_pika
import argparse
import logging
from concurrent.futures import ProcessPoolExecutor
from config.settings import get_settings
from config.utils.rabbit import get_rabbit_connection, setup_queues
from task_processor import process_task

from config.db.base import get_async_db_session
from config.db.models import Task as DBTask
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from datetime import datetime, timezone


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
    async with semaphore:
        task_data = None
        task_id_uuid = None
        try:
            async with message.process(requeue=False):
                task_data = json.loads(message.body.decode())
                task_id_str = task_data.get("task_id")
                if not task_id_str:
                    logger.error(f"Received message without task_id: {task_data}")
                    return
                
                try:
                    task_id_uuid = UUID(task_id_str)
                except ValueError:
                    logger.error(f"Invalid UUID received for task_id: {task_id_str}")
                    return

                logger.info(f"Processing {task_id_str} from {queue_name}")

                async for session in get_async_db_session():
                    db_task = await session.get(DBTask, task_id_uuid)
                    if db_task:
                        db_task.status = "processing"
                        db_task.started_at = datetime.now(timezone.utc)
                        db_task.updated_at = datetime.now(timezone.utc)
                        session.add(db_task)
                        await session.commit()
                        await session.refresh(db_task)
                    else:
                        logger.warning(f"Task {task_id_str} not found in DB during processing start.")

                result = None
                try:
                    if queue_name == "cpu_intensive_tasks":
                        result = await asyncio.wait_for(
                            process_cpu_task(task_data), timeout=300.0
                        )
                    elif queue_name == "io_bound_tasks":
                        result = await process_io_task(task_data)
                    else:
                        result = await asyncio.wait_for(
                            process_io_task(task_data), timeout=30.0
                        )
                    logger.info(f"Task completed: {task_id_str}")

                    async for session in get_async_db_session():
                        db_task = await session.get(DBTask, task_id_uuid)
                        if db_task:
                            db_task.status = "completed"
                            db_task.completed_at = datetime.now(timezone.utc)
                            db_task.updated_at = datetime.now(timezone.utc)
                            db_task.result = result
                            session.add(db_task)
                            await session.commit()
                            await session.refresh(db_task)

                except asyncio.TimeoutError:
                    logger.error(f"Task timed out: {task_id_str}")
                    async for session in get_async_db_session():
                        db_task = await session.get(DBTask, task_id_uuid)
                        if db_task:
                            db_task.status = "failed"
                            db_task.error_message = "Task timed out."
                            db_task.updated_at = datetime.now(timezone.utc)
                            session.add(db_task)
                            await session.commit()
                            await session.refresh(db_task)
                    await message.reject(requeue=False)

                except Exception as e:
                    logger.error(
                        f"Task failed: {task_id_str}, error: {str(e)}"
                    )
                    async for session in get_async_db_session():
                        db_task = await session.get(DBTask, task_id_uuid)
                        if db_task:
                            db_task.status = "failed"
                            db_task.error_message = str(e)
                            db_task.updated_at = datetime.now(timezone.utc)
                            task_data_metadata = task_data.get("metadata", {})
                            db_task.retry_count = task_data_metadata.get("retry_count", 0) + 1
                            session.add(db_task)
                            await session.commit()
                            await session.refresh(db_task)
                    await message.reject(requeue=False)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in message: {message.body.decode()}")
            pass
        except Exception as e:
            logger.error(f"Message processing error outside task logic: {str(e)} - Task data: {task_data}")


async def consume_queues(
    connection: aio_pika.RobustConnection,
    queues_to_consume: list[str],
    concurrency_settings: dict,
):
    PREFETCH_CONFIG = {
        "cpu_intensive_tasks": concurrency_settings.get("cpu_intensive_tasks", 1),
        "io_bound_tasks": 50,
        "urgent_tasks": 5,
    }
    await setup_queues()
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
