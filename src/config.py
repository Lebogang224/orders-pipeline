"""
Configuration loader.

Reads config.yaml, interpolates ${VAR:-default} placeholders from environment,
and returns a typed Config object.

Usage:
    from src.config import load_config
    cfg = load_config()          # uses config.yaml + .env
    cfg = load_config("my.yaml") # custom path
"""
import os
import re
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, field_validator

# Load .env file if present (does NOT override real env vars)
load_dotenv()


class DBConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def conninfo(self) -> str:
        """psycopg conninfo string."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


class FilesConfig(BaseModel):
    customers: Path
    orders: Path
    order_items: Path


class ETLConfig(BaseModel):
    batch_size: int = 10000
    allow_null_country_code: bool = True
    default_currency: str = "ZAR"


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s"


class AgentConfig(BaseModel):
    enabled: bool = True
    provider: str = "groq"
    model: str = "llama-3.3-70b-versatile"


class Config(BaseModel):
    database: DBConfig
    files: FilesConfig
    etl: ETLConfig
    logging: LoggingConfig
    agent: AgentConfig


def _interpolate(text: str) -> str:
    """
    Replace ${VAR:-default} and ${VAR} with environment values.
    Falls back to the default if the variable is not set.
    """
    def replacer(match: re.Match) -> str:
        var = match.group(1)
        default = match.group(3) or ""
        return os.environ.get(var, default)

    return re.sub(r"\$\{(\w+)(:-([^}]*))?\}", replacer, text)


def load_config(path: str = "config.yaml") -> Config:
    raw = Path(path).read_text(encoding="utf-8")
    raw = _interpolate(raw)
    data = yaml.safe_load(raw)
    return Config(**data)
