import os
import json
import uuid
import aio_pika
import asyncio
import logging
from fastapi import FastAPI, status, HTTPException, Depends
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, List
from config.utils.rabbit import (
    get_rabbit_connection,
    get_rabbit_channel,
    setup_queues
)
from config.settings import get_settings
from config.db.base import get_async_db_session
from config.db.models import Task as DBTask
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from datetime import datetime, timezone

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


class TaskResponse(TaskBase):
    id: UUID
    task_type: str
    status: str
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int
    result: Optional[dict] = None

    class Config:
        from_attributes = True


class DelayedTaskRequest(TaskRequest):
    delay_seconds: int = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    await setup_queues()
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/cpu_intensive_task", description="Fibonacci", response_model=TaskResponse)
async def create_cpu_task(request: TaskRequest, db: AsyncSession = Depends(get_async_db_session)):
    return await _publish_task_to_rabbitmq(request, "cpu", db=db)


@app.post("/io_bound_task", status_code=status.HTTP_202_ACCEPTED, response_model=TaskResponse)
async def create_task(request: TaskRequest, db: AsyncSession = Depends(get_async_db_session)):
    return await _publish_task_to_rabbitmq(request, "io", db=db)


@app.post("/delayed_cpu_intensive_task", status_code=status.HTTP_202_ACCEPTED, response_model=TaskResponse)
async def create_delayed_task(request: DelayedTaskRequest, db: AsyncSession = Depends(get_async_db_session)):
    return await _publish_task_to_rabbitmq(
        request, delay_seconds=request.delay_seconds, db=db
    )


async def _publish_task_to_rabbitmq(
    request: TaskRequest, task_type: str = None, delay_seconds: int = 0, db: AsyncSession = Depends(get_async_db_session)
):
    task_uuid = uuid.uuid4()
    task_id_str = str(task_uuid)

    db_task = DBTask(
        id=task_uuid,
        task_type=request.task_type,
        parameters=request.parameters,
        priority=request.priority,
        is_urgent=request.is_urgent,
        status="queued",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc)
    )

    try:
        db.add(db_task)
        await db.commit()
        await db.refresh(db_task)
        logger.info(f"Task {task_id_str} saved to DB with status 'queued'.")
    except Exception as e:
        logger.error(f"Failed to save task {task_id_str} to DB: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save task to database")

    async with get_rabbit_channel() as channel:
        task_message = {
            "type": request.task_type,
            "task_id": task_id_str,
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
            body=json.dumps(task_message).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority=request.priority,
        )
        exchange = await channel.get_exchange("orchestrator_exchange")
        if delay_seconds > 0:
            message.headers["x-delay"] = delay_seconds * 1000
            
            await exchange.publish(message, routing_key="cpu_intensive_tasks")
            
            logger.info(
                f"Published delayed task {task_id_str} with delay {delay_seconds}s to delayed_tasks_queue"
            )
        else:
            await exchange.publish(message, routing_key=routing_key)
            logger.info(f"Published task {task_id_str} to {routing_key}")
        
        return db_task


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


@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task_status(task_id: UUID, db: AsyncSession = Depends(get_async_db_session)):
    result = await db.execute(select(DBTask).where(DBTask.id == task_id))
    task = result.scalars().first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@app.get("/tasks", response_model=List[TaskResponse])
async def list_tasks(
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_async_db_session)
):
    query = select(DBTask)
    if status:
        query = query.where(DBTask.status == status)
    if task_type:
        query = query.where(DBTask.task_type == task_type)
        
    tasks = (await db.execute(query.offset(skip).limit(limit))).scalars().all()
    return tasks
