import os
import logging
import aio_pika
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from config.settings import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_rabbit_connection() -> (
    AsyncGenerator[aio_pika.RobustConnection, None]
):
    connection = None
    try:
        connection = await aio_pika.connect_robust(
            host=settings.RABBITMQ_HOST,
            login=settings.RABBITMQ_USER,
            password=settings.RABBITMQ_PASS,
            heartbeat=30,
            client_properties={"connection_name": "shared_rabbit_connection"},
        )
        logger.info("Connected to RabbitMQ")
        yield connection
    except Exception as e:
        logger.error(f"RabbitMQ connection error: {str(e)}")
        raise
    finally:
        if connection:
            await connection.close()
            logger.info("RabbitMQ connection closed")


@asynccontextmanager
async def get_rabbit_channel() -> (
    AsyncGenerator[aio_pika.abc.AbstractChannel, None]
):
    async with get_rabbit_connection() as connection:
        channel = await connection.channel()
        await channel.declare_exchange(
            "orchestrator_exchange",
            type="x-delayed-message",
            arguments={
                "x-delayed-type": aio_pika.ExchangeType.DIRECT
            },
            durable=True
        )
        yield channel


async def setup_queues():
    async with get_rabbit_channel() as channel:
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
                "x-dead-letter-exchange": "dlx_exchange"
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
