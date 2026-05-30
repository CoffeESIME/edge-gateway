"""
app/core/config.py — Configuración centralizada del microservicio Edge.

Carga variables de entorno desde un archivo .env usando pydantic-settings.
Todas las rutas a certificados mTLS son opcionales: si no se definen, el
cliente MQTT opera en modo cleartext (útil para desarrollo local).
"""
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuración del Edge Node.

    Variables de entorno requeridas (o en .env):
      MQTT_BROKER_IP   — IP o hostname del broker Mosquitto
      EDGE_ID          — Identificador único de esta instalación edge
                         (ej: "edge_planta_norte_01")

    Variables opcionales con defaults razonables:
      MQTT_PORT        — Puerto MQTT (default 8883 con TLS, 1883 sin)
      MQTT_USERNAME    — Usuario para autenticación broker
      MQTT_PASSWORD    — Contraseña para autenticación broker
      MQTT_KEEPALIVE   — Intervalo keepalive en segundos
      MQTT_USE_TLS     — Si True, habilita mTLS (default True)
      CERT_CA_PATH     — Ruta al CA certificate (ca.crt)
      CERT_CLIENT_PATH — Ruta al certificado del cliente (edge.crt)
      CERT_KEY_PATH    — Ruta a la clave privada del cliente (edge.key)
      DB_PATH          — Ruta al archivo SQLite local
      LOG_LEVEL        — Nivel de logging (DEBUG, INFO, WARNING, ERROR)
      POLL_RELOAD_INTERVAL_S — Cada cuántos segundos recarga tags desde SQLite
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Identificación del nodo ────────────────────────────────────────────
    edge_id: str = Field(
        default="edge_default",
        description="Identificador único de esta instalación EDGE.",
    )

    # ── Conexión MQTT ──────────────────────────────────────────────────────
    mqtt_broker_ip: str = Field(
        default="127.0.0.1",
        description="IP o hostname del broker Mosquitto.",
    )
    mqtt_port: int = Field(
        default=8883,
        ge=1,
        le=65535,
        description="Puerto TCP del broker (8883 con TLS, 1883 sin TLS).",
    )
    mqtt_username: Optional[str] = Field(default=None, description="Usuario del broker.")
    mqtt_password: Optional[str] = Field(default=None, description="Contraseña del broker.")
    mqtt_keepalive: int = Field(default=60, ge=5, description="Keepalive MQTT en segundos.")
    mqtt_client_id: str = Field(
        default="",
        description="Client ID MQTT (se auto-genera si está vacío).",
    )
    mqtt_reconnect_delay: float = Field(
        default=5.0, ge=1.0, description="Segundos entre intentos de reconexión."
    )

    # ── TLS / mTLS ─────────────────────────────────────────────────────────
    mqtt_use_tls: bool = Field(
        default=True,
        description="Si True, configura el SSLContext con los certs de abajo.",
    )
    cert_ca_path: Path = Field(
        default=Path("/certs/ca.crt"),
        description="Ruta al CA que firmó el certificado del broker.",
    )
    cert_client_path: Optional[Path] = Field(
        default=Path("/certs/edge.crt"),
        description="Certificado del cliente Edge (mTLS). None para TLS simple.",
    )
    cert_key_path: Optional[Path] = Field(
        default=Path("/certs/edge.key"),
        description="Clave privada del cliente Edge (mTLS). None para TLS simple.",
    )

    # ── Persistencia local ─────────────────────────────────────────────────
    db_path: Path = Field(
        default=Path("/data/edge_db.sqlite"),
        description="Ruta al archivo SQLite de configuración local.",
    )

    # ── Motor de adquisición ───────────────────────────────────────────────
    poll_reload_interval_s: int = Field(
        default=30,
        ge=5,
        description="Cada cuántos segundos el poller recarga la lista de tags.",
    )

    # ── Logging ────────────────────────────────────────────────────────────
    log_level: str = Field(default="INFO", description="Nivel de log (DEBUG/INFO/WARNING/ERROR).")

    @field_validator("mqtt_client_id", mode="before")
    @classmethod
    def _default_client_id(cls, v: str) -> str:
        """Si no se configura client_id, genera uno único en tiempo de ejecución."""
        if not v:
            import uuid
            return f"scada-edge-{uuid.uuid4().hex[:8]}"
        return v


# Singleton cargado una sola vez al importar el módulo.
settings = Settings()
