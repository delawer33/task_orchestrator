from .base import BaseConfig


class DevelopmentConfig(BaseConfig):
    DEBUG: bool = True
    LOG_LEVEL: str = "DEBUG"
    RABBITMQ_HOST: str = "localhost"

    class Config(BaseConfig.Config):
        env_file = ".env.development"
