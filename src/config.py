"""Database connection configuration."""
import os
from dataclasses import dataclass, field


@dataclass
class ConnectionConfig:
    """Unified database connection configuration."""
    db_type: str          # "mysql", "postgresql", "sqlite"
    host: str = "localhost"
    port: int = 3306
    user: str = ""
    password: str = ""
    database: str = ""
    ssl_ca: str | None = None

    @classmethod
    def from_env(cls) -> "ConnectionConfig":
        """Build config from environment variables."""
        db_type = os.environ.get("DB_TYPE", "mysql").lower()
        if db_type == "sqlite":
            return cls(
                db_type="sqlite",
                database=os.environ.get("DB_NAME", "agent_db.sqlite"),
            )
        return cls(
            db_type=db_type,
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT",
                                    5432 if db_type == "postgresql" else 3306)),
            user=os.environ.get("DB_USER", ""),
            password=os.environ.get("DB_PASSWORD", ""),
            database=os.environ.get("DB_NAME", ""),
            ssl_ca=os.environ.get("DB_SSL_CA") or None,
        )

    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy database URL."""
        if self.db_type == "sqlite":
            return f"sqlite:///{self.database}"
        if self.db_type == "postgresql":
            driver = "postgresql+psycopg2"
        else:
            driver = "mysql+pymysql"
        url = (
            f"{driver}://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        params = []
        if self.ssl_ca:
            params.append(f"ssl_ca={self.ssl_ca}")
        if self.db_type == "mysql":
            params.append("charset=utf8mb4")
        if params:
            url += "?" + "&".join(params)
        return url

    @property
    def display_name(self) -> str:
        """Human-readable name for UI display."""
        if self.db_type == "sqlite":
            return f"SQLite ({self.database})"
        return f"{self.db_type.upper()} ({self.host}:{self.port}/{self.database})"
