import json
import os
import asyncio
import aio_pika
import logging
from config.utils.rabbit import get_rabbit_connection
from config.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


async def setup_dlx_queues(channel: aio_pika.abc.AbstractChannel):
    dlx_exchange = await channel.declare_exchange(
        "dlx_exchange", aio_pika.ExchangeType.FANOUT, durable=True
    )
    dead_letter_queue = await channel.declare_queue(
        "dead_letter_queue", durable=True
    )
    await dead_letter_queue.bind(dlx_exchange)
    logger.info("DLX and Dead Letter Queue setup complete.")
    return dead_letter_queue


async def handle_dead_letter_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    semaphore: asyncio.Semaphore,
    connection: aio_pika.RobustConnection,
):
    async with semaphore:
        try:
            async with message.process(requeue=False):
                body = message.body.decode()
                headers = message.headers
                logger.warning(f"Received dead letter message: {body[:200]}...")
                logger.warning(f"Headers: {headers}")
                task_data = json.loads(body)
                task_id = task_data.get("task_id", "N/A")
                death_info = headers.get("x-death", [])
                retry_count = 0
                original_queue = "N/A"
                if death_info:
                    for death_entry in death_info:
                        if "count" in death_entry:
                            retry_count = death_entry["count"]
                        if "queue" in death_entry:
                            original_queue = death_entry["queue"]
                logger.warning(
                    f"Dead Letter Task {task_id} from '{original_queue}', retry attempt {retry_count}"
                )
                current_app_retry_count = task_data["metadata"].get(
                    "retry_count", 0
                )
                max_app_retries = 3
                logger.warning(
                    f"Internal retry count for task {task_id}: {current_app_retry_count}/{max_app_retries}"
                )
                if current_app_retry_count < max_app_retries:
                    logger.info(
                        f"Attempting to re-process task {task_id} (retry {current_app_retry_count}/{max_app_retries})..."
                    )
                    async with connection.channel() as publish_channel:
                        try:
                            exchange_obj = (
                                await publish_channel.declare_exchange(
                                    "orchestrator_exchange",
                                    aio_pika.ExchangeType.DIRECT,
                                    durable=True,
                                )
                            )
                            task_data["metadata"]["retry_count"] = (
                                current_app_retry_count + 1
                            )
                            updated_message_body = json.dumps(
                                task_data
                            ).encode()
                            await exchange_obj.publish(
                                aio_pika.Message(
                                    body=updated_message_body,
                                    delivery_mode=message.delivery_mode,
                                    priority=message.priority,
                                ),
                                routing_key=original_queue,
                            )
                            logger.info(
                                f"Task {task_id} re-published to '{original_queue}' for retry."
                            )
                        except Exception as publish_error:
                            logger.error(
                                f"Failed to re-publish task {task_id}: {str(publish_error)}"
                            )
                else:
                    logger.error(
                        f"Task {task_id} exceeded max retries. Moving to permanent storage/alerting."
                    )
                    pass
        except json.JSONDecodeError:
            logger.error(
                f"Invalid JSON in dead letter message: {message.body.decode()}"
            )
            pass
        except Exception as e:
            logger.error(
                f"Error handling dead letter message: {str(e)} - Message: {message.body.decode()}"
            )
            pass


async def consume_dlq(
    connection: aio_pika.RobustConnection, max_concurrent_dlq_tasks: int = 2
):
    semaphore = asyncio.Semaphore(max_concurrent_dlq_tasks)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max_concurrent_dlq_tasks)
        dlq = await setup_dlx_queues(channel)
        logger.info(
            f"DLX Handler started. Waiting for dead letters (concurrency: {max_concurrent_dlq_tasks})..."
        )
        await dlq.consume(
            lambda m: handle_dead_letter_message(m, semaphore, connection)
        )
        await asyncio.Future()


async def main():
    while True:
        try:
            async with get_rabbit_connection() as connection:
                await consume_dlq(
                    connection,
                    max_concurrent_dlq_tasks=settings.DLX_CONCURRENCY,
                )
        except KeyboardInterrupt:
            logger.info("DLX Handler stopped by user")
            break
        except Exception as e:
            logger.error(
                f"DLX Handler error: {str(e)}, reconnecting in 10 seconds..."
            )
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
