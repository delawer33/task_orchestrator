from pydantic import Field
from pydantic_settings import BaseSettings


class BaseConfig(BaseSettings):
    APP_NAME: str = "Task Orchestrator"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    RABBITMQ_HOST: str = "rabbitmq"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = Field("admin", env="RABBITMQ_USER")
    RABBITMQ_PASS: str = Field("secret", env="RABBITMQ_PASS")

    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    DB_HOST: str = "postgres"
    DB_PORT: int = 5432
    DB_NAME: str = "task_orchestrator"
    
    RABBITMQ_VHOST: str = "/"
    DEFAULT_QUEUE: str = "default_tasks"
    PRIORITY_QUEUE: str = "urgent_tasks"
    DLX_QUEUE: str = "dead_letter_queue"
    DLX_CONCURRENCY: int = 4
    MAX_PRIORITY: int = 10
    WORKER_CONCURRENCY: int = 4
    WORKER_CONCURRENCY_RPC: int = 4
    MAX_RETRY_COUNT: int = 3
    RETRY_DELAY: int = 30000

    class Config:
        env_file = ".env"
        case_sensitive = True
