import os
import json
import uuid
import aio_pika
import asyncio
import logging
from fastapi import FastAPI, status, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from config.utils.rabbit import get_rabbit_connection, get_rabbit_channel
from config.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskRequest(BaseModel):
    task_type: str
    parameters: dict = {}
    priority: int = 0
    is_urgent: bool = False


class TaskBase(BaseModel):
    priority: int = 0
    is_urgent: bool = False
    parameters: dict = {}


class DelayedTaskRequest(TaskRequest):
    delay_seconds: int = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
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
        rpc_queue = await channel.declare_queue("rpc_requests", durable=True)
        await rpc_queue.bind(exchange, routing_key="rpc_requests")

        await channel.declare_exchange(
            "dlx_exchange", type=aio_pika.ExchangeType.FANOUT, durable=True
        )
        dlx_queue = await channel.declare_queue(
            "dead_letter_queue", durable=True
        )

        await dlx_queue.bind("dlx_exchange")
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/cpu_intensive_task", description="Fibonacci")
async def create_cpu_task(request: TaskRequest):
    return await _publish_task_to_rabbitmq(request, "cpu")


@app.post("/io_bound_task", status_code=status.HTTP_202_ACCEPTED)
async def create_task(request: TaskRequest):
    return await _publish_task_to_rabbitmq(request, "io")


@app.post("/delayed_cpu_intensive_task", status_code=status.HTTP_202_ACCEPTED)
async def create_delayed_task(request: DelayedTaskRequest):
    return await _publish_task_to_rabbitmq(
        request, delay_seconds=request.delay_seconds
    )


async def _publish_task_to_rabbitmq(
    request: TaskRequest, task_type: str = None, delay_seconds: int = 0
):
    async with get_rabbit_channel() as channel:
        task = {
            "type": request.task_type,
            "task_id": str(uuid.uuid4()),
            "status": "queued",
            "priority": request.priority,
            "parameters": request.parameters,
            "metadata": {"retry_count": 0, "is_urgent": request.is_urgent},
        }
        routing_key = ""
        if request.is_urgent:
            routing_key = "urgent_tasks"
        else:
            if task_type == "cpu":
                routing_key = "cpu_intensive_tasks"
            elif task_type == "io":
                routing_key = "io_bound_tasks"
        message = aio_pika.Message(
            body=json.dumps(task).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority=request.priority,
        )
        exchange = await channel.get_exchange("orchestrator_exchange")
        if delay_seconds > 0:
            message.headers["x-delay"] = delay_seconds * 1000
            
            await exchange.publish(message, routing_key="cpu_intensive_tasks")
            
            logger.info(
                f"Published delayed task {task['task_id']} with delay {delay_seconds}s to delayed_tasks_queue"
            )
        else:
            await exchange.publish(message, routing_key=routing_key)
            logger.info(f"Published task {task['task_id']} to {routing_key}")
        return {"task_id": task["task_id"], "status": "queued"}


@app.post("/rpc_multiply", status_code=status.HTTP_200_OK)
async def rpc_multiply(request: TaskRequest):
    correlation_id = str(uuid.uuid4())
    async with get_rabbit_connection() as connection:
        channel = await connection.channel()
        reply_queue = await channel.declare_queue(
            None, exclusive=True, auto_delete=True
        )
        logger.info(
            f"RPC Client: Declared reply queue: {reply_queue.name} for correlation_id: {correlation_id}"
        )
        response_future = asyncio.Future()

        async def on_response(message: aio_pika.abc.AbstractIncomingMessage):
            async with message.process():
                if message.correlation_id == correlation_id:
                    response_future.set_result(
                        json.loads(message.body.decode())
                    )
                    logger.info(
                        f"RPC Client: Received response for correlation_id: {correlation_id}"
                    )

        await channel.set_qos(prefetch_count=1)
        consumer_tag = await reply_queue.consume(on_response)
        exchange = await channel.declare_exchange(
            "orchestrator_exchange",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        
        rpc_message = aio_pika.Message(
            body=json.dumps(
                request.model_dump().get("parameters", {})
            ).encode(),
            correlation_id=correlation_id,
            reply_to=reply_queue.name,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await exchange.publish(rpc_message, routing_key="rpc_requests")
        logger.info(
            f"RPC Client: Sent request with correlation_id: {correlation_id}"
        )
        try:
            response_data = await asyncio.wait_for(
                response_future, timeout=10.0
            )
            return {"result": response_data}
        except asyncio.TimeoutError:
            logger.error(
                f"RPC Client: Timeout waiting for response for correlation_id: {correlation_id}"
            )
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="RPC request timed out",
            )
        finally:
            await reply_queue.cancel(consumer_tag)
            await channel.close()
