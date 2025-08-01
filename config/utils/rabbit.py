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
