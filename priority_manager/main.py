import json
import os
import asyncio
import aio_pika
import logging
from contextlib import asynccontextmanager
from config.utils.rabbit import get_rabbit_connection
from config.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


async def setup_priority_queues(channel: aio_pika.abc.AbstractChannel):
    exchange = await channel.declare_exchange(
        "orchestrator_exchange", aio_pika.ExchangeType.DIRECT, durable=True
    )
    await channel.declare_queue(
        "default_tasks",
        durable=True,
        arguments={
            "x-max-priority": 10,
            "x-dead-letter-exchange": "dlx_exchange",
        },
    )
    await channel.declare_queue(
        "urgent_tasks",
        durable=True,
        arguments={
            "x-max-priority": 10,
            "x-dead-letter-exchange": "dlx_exchange",
        },
    )
    logger.info("Priority Manager queues setup complete.")
    return exchange


async def process_message_for_priority(
    message: aio_pika.abc.AbstractIncomingMessage,
    publish_exchange: aio_pika.abc.AbstractExchange,
):
    try:
        async with message.process():
            body = message.body.decode()
            task_data = json.loads(body)
            task_id = task_data.get("task_id", "N/A")
            current_priority = task_data.get("priority", 0)
            is_urgent_flag = task_data.get("metadata", {}).get(
                "is_urgent", False
            )
            logger.info(
                f"Priority Manager: Analyzing task {task_id} (current priority: {current_priority}, urgent_flag: {is_urgent_flag})"
            )
            if is_urgent_flag or current_priority >= 5:
                logger.info(
                    f"Priority Manager: Re-prioritizing task {task_id} to 'urgent_tasks'."
                )
                republished_message = aio_pika.Message(
                    body=message.body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=10,
                )
                await publish_exchange.publish(
                    republished_message, routing_key="urgent_tasks"
                )
                logger.info(
                    f"Priority Manager: Task {task_id} re-published as urgent."
                )
            else:
                logger.info(
                    f"Priority Manager: Task {task_id} does not require re-prioritization."
                )
    except json.JSONDecodeError:
        logger.error(
            f"Priority Manager: Invalid JSON in message: {message.body.decode()}"
        )
        await message.reject(requeue=False)
    except Exception as e:
        logger.error(
            f"Priority Manager: Error processing message: {str(e)} - Message: {message.body.decode()}"
        )
        await message.reject(requeue=True)


async def consume_default_queue(connection: aio_pika.RobustConnection):
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=settings.WORKER_CONCURRENCY)
        publish_exchange = await setup_priority_queues(channel)
        default_queue = await channel.get_queue("default_tasks")
        logger.info(
            "Priority Manager started. Waiting for tasks to prioritize..."
        )
        await default_queue.consume(
            lambda m: process_message_for_priority(m, publish_exchange)
        )
        await asyncio.Future()


async def main():
    while True:
        try:
            async with get_rabbit_connection() as connection:
                await consume_default_queue(connection)
        except KeyboardInterrupt:
            logger.info("Priority Manager stopped by user")
            break
        except Exception as e:
            logger.error(
                f"Priority Manager error: {str(e)}, reconnecting in 5 seconds..."
            )
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
