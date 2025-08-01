import json
import os
import asyncio
import aio_pika
import logging
import argparse
from contextlib import asynccontextmanager
from config.utils.rabbit import get_rabbit_connection
from config.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


async def handle_rpc_request(
    message: aio_pika.abc.AbstractIncomingMessage,
    semaphore: asyncio.Semaphore,
    connection: aio_pika.RobustConnection,
):
    async with semaphore:
        try:
            async with message.process():
                request_data = json.loads(message.body.decode())
                a = request_data.get("a")
                b = request_data.get("b")
                logger.info(
                    f"RPC Worker: Processing multiplication for a={a}, b={b}, correlation_id={message.correlation_id}"
                )
                if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                    result = {"product": a * b}
                    status_code = 200
                else:
                    result = {
                        "error": "Invalid input: a and b must be numbers."
                    }
                    status_code = 400
                if message.reply_to:
                    response_message = aio_pika.Message(
                        body=json.dumps(result).encode(),
                        correlation_id=message.correlation_id,
                        content_type="application/json",
                    )
                    async with connection.channel() as publish_channel:
                        await publish_channel.default_exchange.publish(
                            response_message, routing_key=message.reply_to
                        )
                    logger.info(
                        f"RPC Worker: Sent response for correlation_id={message.correlation_id} to {message.reply_to} (status: {status_code})"
                    )
                else:
                    logger.warning(
                        f"RPC Worker: No reply_to queue specified for correlation_id={message.correlation_id}"
                    )
        except json.JSONDecodeError:
            logger.error(
                f"RPC Worker: Invalid JSON request: {message.body.decode()}"
            )
            await message.reject(requeue=False)
        except Exception as e:
            logger.error(
                f"RPC Worker: Error handling request: {str(e)} - Message: {message.body.decode()}"
            )
            await message.reject(requeue=True)


async def consume_rpc_queue(
    connection: aio_pika.RobustConnection, max_concurrent_tasks: int
):
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max_concurrent_tasks)
        rpc_queue = await channel.declare_queue("rpc_requests", durable=True)
        logger.info(
            f"RPC Worker started. Waiting for RPC requests (concurrency: {max_concurrent_tasks})..."
        )
        await rpc_queue.consume(
            lambda m: handle_rpc_request(m, semaphore, connection)
        )
        await asyncio.Future()


async def main():
    parser = argparse.ArgumentParser(
        description="RPC Worker for task processing."
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=settings.WORKER_CONCURRENCY_RPC,
        help="Maximum number of concurrent RPC requests.",
    )
    args = parser.parse_args()
    max_concurrent_rpc_tasks = args.concurrency
    while True:
        try:
            async with get_rabbit_connection() as connection:
                await consume_rpc_queue(connection, max_concurrent_rpc_tasks)
        except KeyboardInterrupt:
            logger.info("RPC Worker stopped by user")
            break
        except Exception as e:
            logger.error(
                f"RPC Worker error: {str(e)}, reconnecting in 5 seconds..."
            )
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
