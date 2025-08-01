import os
from .base import BaseConfig
from .development import DevelopmentConfig
from .production import ProductionConfig


def get_settings() -> BaseConfig:
    env = os.getenv("APP_ENV", "development").lower()
    config_mapping = {
        "development": DevelopmentConfig,
        "production": ProductionConfig,
        "testing": DevelopmentConfig,
    }
    config_class = config_mapping.get(env, DevelopmentConfig)
    return config_class()
