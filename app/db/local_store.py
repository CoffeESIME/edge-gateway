"""
app/db/local_store.py — Persistencia local del Diccionario de Tags (SQLite).

El Edge Node no se conecta a TimescaleDB ni PostgreSQL.
Esta base de datos SQLite local cumple un único propósito: sobrevivir a
reinicios y pérdidas de conectividad conservando el "Diccionario de Tags"
que debe leer este nodo.

Estructura de la tabla `tags`:
  - tag_id      INTEGER PRIMARY KEY  → ID del tag en el SCADA central
  - tag_name    TEXT UNIQUE          → Nombre de la variable (ej: "TNK_NIVEL_01")
  - protocol    TEXT                 → Protocolo: "modbus"|"opcua"|"simulated"
  - config_json TEXT                 → JSON con connection_config del tag
  - scan_rate_ms INTEGER             → Intervalo de lectura en milisegundos
  - mqtt_topic  TEXT                 → Tópico destino de publicación
  - is_enabled  INTEGER              → 1=activo, 0=inactivo
  - updated_at  TEXT                 → Timestamp ISO8601 de la última actualización
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

from app.core.config import settings

logger = logging.getLogger(__name__)

# DDL de inicialización de la tabla.
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tags (
    tag_id      INTEGER PRIMARY KEY,
    tag_name    TEXT    NOT NULL UNIQUE,
    protocol    TEXT    NOT NULL,
    config_json TEXT    NOT NULL DEFAULT '{}',
    scan_rate_ms INTEGER NOT NULL DEFAULT 1000,
    mqtt_topic  TEXT    NOT NULL,
    is_enabled  INTEGER NOT NULL DEFAULT 1,
    updated_at  TEXT    NOT NULL
);
"""

# ──────────────────────────────────────────────────────────────────────────────
# Modelo de datos en memoria
# ──────────────────────────────────────────────────────────────────────────────

class TagConfig:
    """Representación en memoria de la configuración de un Tag."""

    __slots__ = (
        "tag_id", "tag_name", "protocol",
        "connection_config", "scan_rate_ms", "mqtt_topic",
        "is_enabled", "updated_at",
    )

    def __init__(
        self,
        tag_id: int,
        tag_name: str,
        protocol: str,
        connection_config: Dict[str, Any],
        scan_rate_ms: int,
        mqtt_topic: str,
        is_enabled: bool = True,
        updated_at: Optional[str] = None,
    ) -> None:
        self.tag_id = tag_id
        self.tag_name = tag_name
        self.protocol = protocol
        self.connection_config = connection_config
        self.scan_rate_ms = max(scan_rate_ms, 100)
        self.mqtt_topic = mqtt_topic
        self.is_enabled = is_enabled
        self.updated_at = updated_at or datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tag_id": self.tag_id,
            "tag_name": self.tag_name,
            "protocol": self.protocol,
            "connection_config": self.connection_config,
            "scan_rate_ms": self.scan_rate_ms,
            "mqtt_topic": self.mqtt_topic,
            "is_enabled": self.is_enabled,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_row(cls, row: aiosqlite.Row) -> "TagConfig":
        return cls(
            tag_id=row["tag_id"],
            tag_name=row["tag_name"],
            protocol=row["protocol"],
            connection_config=json.loads(row["config_json"]),
            scan_rate_ms=row["scan_rate_ms"],
            mqtt_topic=row["mqtt_topic"],
            is_enabled=bool(row["is_enabled"]),
            updated_at=row["updated_at"],
        )


# ──────────────────────────────────────────────────────────────────────────────
# Store
# ──────────────────────────────────────────────────────────────────────────────

class LocalTagStore:
    """
    Manejador de persistencia local para el Diccionario de Tags.

    Uso:
        store = LocalTagStore()
        await store.init()                      # Crea tabla si no existe
        await store.save_tags_config(tags_list) # Actualiza/inserta tags
        tags = await store.get_active_tags()    # Devuelve lista TagConfig
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._db_path = str(db_path or settings.db_path)
        self._lock = asyncio.Lock()

    async def init(self) -> None:
        """Crea la base de datos y la tabla si no existen."""
        # Asegurar que el directorio padre existe.
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(self._db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute(_CREATE_TABLE_SQL)
            await db.commit()
        logger.info("[LocalStore] Base de datos inicializada en: %s", self._db_path)

    async def save_tags_config(self, tags: List[Dict[str, Any]]) -> int:
        """
        Inserta o actualiza (UPSERT) una lista de tags en la base local.

        Cada diccionario en `tags` debe contener al mínimo:
          - tag_id      (int)
          - tag_name    (str)
          - protocol    (str)   "modbus"|"opcua"|"simulated"
          - connection_config (dict)
          - scan_rate_ms (int)
          - mqtt_topic  (str)
          - is_enabled  (bool, opcional, default True)

        Retorna el número de filas afectadas.
        """
        if not tags:
            return 0

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for t in tags:
            rows.append((
                int(t["tag_id"]),
                str(t["tag_name"]),
                str(t["protocol"]).lower(),
                json.dumps(t.get("connection_config", {})),
                int(t.get("scan_rate_ms", 1000)),
                str(t.get("mqtt_topic", f"scada/tags/{t['tag_name']}")),
                int(bool(t.get("is_enabled", True))),
                now,
            ))

        upsert_sql = """
            INSERT INTO tags
                (tag_id, tag_name, protocol, config_json, scan_rate_ms,
                 mqtt_topic, is_enabled, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tag_id) DO UPDATE SET
                tag_name    = excluded.tag_name,
                protocol    = excluded.protocol,
                config_json = excluded.config_json,
                scan_rate_ms= excluded.scan_rate_ms,
                mqtt_topic  = excluded.mqtt_topic,
                is_enabled  = excluded.is_enabled,
                updated_at  = excluded.updated_at
        """
        async with self._lock:
            async with aiosqlite.connect(self._db_path) as db:
                db.row_factory = aiosqlite.Row
                await db.executemany(upsert_sql, rows)
                await db.commit()

        logger.info("[LocalStore] %d tags actualizados/insertados.", len(rows))
        return len(rows)

    async def get_active_tags(self) -> List[TagConfig]:
        """
        Devuelve la lista de tags con is_enabled=1 ordenados por tag_id.
        """
        async with aiosqlite.connect(self._db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM tags WHERE is_enabled = 1 ORDER BY tag_id"
            )
            rows = await cursor.fetchall()

        tags = [TagConfig.from_row(row) for row in rows]
        logger.debug("[LocalStore] %d tags activos encontrados.", len(tags))
        return tags

    async def get_all_tags(self) -> List[TagConfig]:
        """Devuelve todos los tags (activos e inactivos)."""
        async with aiosqlite.connect(self._db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM tags ORDER BY tag_id")
            rows = await cursor.fetchall()
        return [TagConfig.from_row(row) for row in rows]

    async def delete_tag(self, tag_id: int) -> bool:
        """Elimina un tag por su ID. Retorna True si fue eliminado."""
        async with self._lock:
            async with aiosqlite.connect(self._db_path) as db:
                cursor = await db.execute(
                    "DELETE FROM tags WHERE tag_id = ?", (tag_id,)
                )
                await db.commit()
                return cursor.rowcount > 0

    async def count_tags(self) -> int:
        """Retorna el total de tags registrados."""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM tags")
            row = await cursor.fetchone()
            return row[0] if row else 0


# Singleton global.
local_store = LocalTagStore()
