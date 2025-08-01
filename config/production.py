from .base import BaseConfig


class ProductionConfig(BaseConfig):
    DEBUG: bool = False
    LOG_LEVEL: str = "WARNING"
    RABBITMQ_USER: str = "prod_user"
    RABBITMQ_PASS: str = "secure_password_here"

    class Config(BaseConfig.Config):
        env_file = ".env.production"
