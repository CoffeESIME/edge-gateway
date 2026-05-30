"""
app/services/edge_engine.py — Motor de adquisición principal del Edge Node.

Este módulo orquesta dos tareas asíncronas concurrentes:

┌─────────────────────────────────────────────────────────────────────┐
│ Tarea 1: _poller_task                                               │
│   - Lee la lista de tags activos desde SQLite local.                │
│   - Por cada tag, instancia el driver correspondiente (Factory).    │
│   - Lee el valor físico del dispositivo (Modbus/OPC UA/Sim).        │
│   - Publica el resultado JSON en scada/tags/{tag_name} vía MQTT.    │
│   - Cada tag corre con su propio asyncio.Task a su scan_rate_ms.    │
│   - Recarga el listado de tags cada POLL_RELOAD_INTERVAL_S segundos.│
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Tarea 2: _listener_task                                             │
│   - Se suscribe (a través del mqtt_client) a dos tópicos de control:│
│                                                                     │
│   scada/edge/{EDGE_ID}/config/upsert                                │
│     → Recibe un JSON con lista de tags, actualiza SQLite y RAM.     │
│       El poller detecta los cambios en el próximo ciclo de recarga. │
│                                                                     │
│   scada/edge/{EDGE_ID}/commands/write                               │
│     → Recibe una orden de escritura, usa el driver para escribir    │
│       directamente en el dispositivo físico y publica el ACK.       │
└─────────────────────────────────────────────────────────────────────┘
"""
import asyncio
import json
import logging
from typing import Any, Dict

from app.core.config import settings
from app.core.mqtt_client import mqtt_client
from app.db.local_store import TagConfig, local_store
from app.drivers.factory import DriverFactory

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Estado interno del motor
# ──────────────────────────────────────────────────────────────────────────────

# Diccionario de tareas activas de polling: {tag_id: asyncio.Task}
_active_poll_tasks: Dict[int, asyncio.Task] = {}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers de publicación MQTT
# ──────────────────────────────────────────────────────────────────────────────

async def _publish_tag_value(tag: TagConfig, read_result: Any) -> None:
    """Serializa y publica un ReadResult al broker MQTT."""
    payload = json.dumps({
        "edge_id":   settings.edge_id,
        "tag_id":    tag.tag_id,
        "tag_name":  tag.tag_name,
        "value":     read_result.value,
        "quality":   read_result.quality,
        "timestamp": read_result.timestamp,
    })
    topic = tag.mqtt_topic or f"scada/tags/{tag.tag_name}"
    await mqtt_client.publish(topic, payload, qos=0)
    logger.debug("[POLLER] Publicado %s → %s (q=%s)", tag.tag_name, read_result.value, read_result.quality)


async def _publish_write_ack(tag_name: str, value: Any, success: bool, error: str = "") -> None:
    """Publica el resultado de un comando de escritura."""
    from datetime import datetime, timezone
    payload = json.dumps({
        "edge_id":   settings.edge_id,
        "tag_name":  tag_name,
        "value":     value,
        "success":   success,
        "error":     error,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    topic = f"scada/edge/{settings.edge_id}/commands/write/ack"
    await mqtt_client.publish(topic, payload, qos=1)


# ──────────────────────────────────────────────────────────────────────────────
# Tarea 1: Poller por Tag
# ──────────────────────────────────────────────────────────────────────────────

async def _poll_single_tag(tag: TagConfig) -> None:
    """
    Corrutina de polling para un único tag.
    Se ejecuta en un bucle infinito con su propio intervalo (scan_rate_ms).
    Se cancela automáticamente cuando el tag ya no está en la lista activa.
    """
    interval = max(tag.scan_rate_ms, 100) / 1000.0
    logger.info(
        "[POLLER] Iniciado → tag='%s' (id=%d) protocol=%s interval=%.2fs",
        tag.tag_name, tag.tag_id, tag.protocol, interval,
    )

    while True:
        try:
            driver = DriverFactory.get_driver(tag.protocol, tag.connection_config)
            await driver.connect()
            result = await driver.read_tag(tag.connection_config)
            await driver.disconnect()

            if result.quality == "GOOD":
                await _publish_tag_value(tag, result)
            else:
                logger.warning(
                    "[POLLER] Lectura BAD para tag='%s' — publicando calidad BAD.", tag.tag_name
                )
                await _publish_tag_value(tag, result)  # También publicamos BAD para alertar al SCADA.

        except asyncio.CancelledError:
            logger.info("[POLLER] Task cancelada → tag='%s'", tag.tag_name)
            return
        except Exception as exc:
            logger.error("[POLLER] Error en tag='%s': %s", tag.tag_name, exc)

        await asyncio.sleep(interval)


async def _poller_task() -> None:
    """
    Tarea maestra de polling.

    Cada POLL_RELOAD_INTERVAL_S segundos:
      1. Lee la lista de tags activos desde SQLite.
      2. Cancela las tareas de tags que ya no están activos.
      3. Lanza nuevas tareas para tags nuevos o que se habían caído.
    """
    logger.info("[POLLER] Motor de polling iniciado.")

    while True:
        try:
            active_tags = await local_store.get_active_tags()
            current_ids = {t.tag_id for t in active_tags}

            # Cancelar tareas de tags que ya no están en la lista activa.
            for tid in list(_active_poll_tasks.keys()):
                if tid not in current_ids:
                    _active_poll_tasks[tid].cancel()
                    del _active_poll_tasks[tid]
                    logger.info("[POLLER] Task removida → tag_id=%d (ya no activo)", tid)

            # Lanzar tareas para tags nuevos o que terminaron inesperadamente.
            for tag in active_tags:
                existing_task = _active_poll_tasks.get(tag.tag_id)
                if existing_task is None or existing_task.done():
                    task = asyncio.create_task(
                        _poll_single_tag(tag),
                        name=f"poll_tag_{tag.tag_id}_{tag.tag_name}",
                    )
                    _active_poll_tasks[tag.tag_id] = task
                    logger.info(
                        "[POLLER] Nueva task → tag='%s' (id=%d)", tag.tag_name, tag.tag_id
                    )

            if not active_tags:
                logger.warning("[POLLER] No hay tags activos en la base local.")

        except Exception as exc:
            logger.error("[POLLER] Error recargando tags: %s", exc)

        await asyncio.sleep(settings.poll_reload_interval_s)


# ──────────────────────────────────────────────────────────────────────────────
# Tarea 2: Listener MQTT de Control
# ──────────────────────────────────────────────────────────────────────────────

async def _handle_config_upsert(topic: str, payload: bytes) -> None:
    """
    Handler para: scada/edge/{EDGE_ID}/config/upsert

    Payload esperado:
    {
        "tags": [
            {
                "tag_id": 1,
                "tag_name": "NIVEL_TK01",
                "protocol": "modbus",
                "connection_config": {"host": "192.168.1.10", "register": 40001, ...},
                "scan_rate_ms": 1000,
                "mqtt_topic": "scada/tags/NIVEL_TK01",
                "is_enabled": true
            },
            ...
        ]
    }

    Al recibir este mensaje:
      1. Actualiza la base SQLite local (save_tags_config).
      2. Cancela las tareas de polling existentes para forzar recarga inmediata.
         El _poller_task detectará los cambios en su próximo ciclo.
    """
    try:
        data: Dict[str, Any] = json.loads(payload.decode("utf-8"))
        tags_list = data.get("tags", [])

        if not isinstance(tags_list, list) or not tags_list:
            logger.warning("[LISTENER] config/upsert recibido con lista vacía o inválida.")
            return

        count = await local_store.save_tags_config(tags_list)
        logger.info(
            "[LISTENER] ✅ config/upsert: %d tags actualizados en base local. "
            "El poller recargará en el próximo ciclo.", count,
        )

        # Forzar recarga inmediata: cancelar todas las tasks de polling activas.
        # El _poller_task las relanzará en su próximo ciclo (que es cada 30 s,
        # pero se puede ajustar con POLL_RELOAD_INTERVAL_S).
        for tid, task in list(_active_poll_tasks.items()):
            task.cancel()
        _active_poll_tasks.clear()
        logger.info("[LISTENER] Todas las tasks de polling reiniciadas para aplicar nueva config.")

    except json.JSONDecodeError:
        logger.error("[LISTENER] Payload inválido en config/upsert (no es JSON).")
    except Exception as exc:
        logger.error("[LISTENER] Error procesando config/upsert: %s", exc)


async def _handle_write_command(topic: str, payload: bytes) -> None:
    """
    Handler para: scada/edge/{EDGE_ID}/commands/write

    Payload esperado:
    {
        "tag_name":         "NIVEL_TK01",     // Nombre del tag a escribir
        "value":            45.5,              // Valor a enviar al PLC
        "protocol":         "modbus",          // Protocolo a usar
        "connection_config": { ... }           // Configuración de conexión (completa)
    }

    Al recibir este mensaje:
      1. Instancia el driver correspondiente.
      2. Escribe el valor físicamente en el dispositivo.
      3. Publica el ACK en scada/edge/{EDGE_ID}/commands/write/ack.
    """
    tag_name = "unknown"
    value = None
    try:
        data: Dict[str, Any] = json.loads(payload.decode("utf-8"))
        tag_name = str(data.get("tag_name", "unknown"))
        value = data.get("value")
        protocol = str(data.get("protocol", ""))
        connection_config: Dict[str, Any] = data.get("connection_config", {})

        if value is None:
            raise ValueError("Campo 'value' requerido en el comando de escritura.")
        if not protocol:
            raise ValueError("Campo 'protocol' requerido en el comando de escritura.")

        logger.info(
            "[LISTENER] Comando de escritura → tag='%s' value=%s protocol=%s",
            tag_name, value, protocol,
        )

        driver = DriverFactory.get_driver(protocol, connection_config)
        await driver.connect()
        success = await driver.write_tag(connection_config, value)
        await driver.disconnect()

        await _publish_write_ack(tag_name, value, success)

        if success:
            logger.info("[LISTENER] ✅ Escritura exitosa → tag='%s' value=%s", tag_name, value)
        else:
            logger.warning("[LISTENER] ⚠️ Escritura fallida → tag='%s'", tag_name)

    except json.JSONDecodeError:
        logger.error("[LISTENER] Payload inválido en commands/write (no es JSON).")
        await _publish_write_ack(tag_name, value, False, error="Payload JSON inválido.")
    except ValueError as exc:
        logger.error("[LISTENER] Error de validación en commands/write: %s", exc)
        await _publish_write_ack(tag_name, value, False, error=str(exc))
    except Exception as exc:
        logger.error("[LISTENER] Error inesperado en commands/write: %s", exc)
        await _publish_write_ack(tag_name, value, False, error=str(exc))


async def _listener_task() -> None:
    """
    Tarea de registro de handlers para los tópicos de control del Edge.

    El Edge escucha en DOS niveles de tópico para cada acción:

      1. Broadcast (sin EDGE_ID): enviado por el backend a TODOS los Edges.
           scada/edge/config/upsert
           scada/edge/commands/write

      2. Específico (con EDGE_ID): para provisioning dirigido a un nodo concreto.
           scada/edge/{EDGE_ID}/config/upsert
           scada/edge/{EDGE_ID}/commands/write

    Ambos niveles mapean al mismo handler — el efecto es idéntico.
    """
    edge_id = settings.edge_id

    # ── Configuración: broadcast (backend → todos los Edges) ──────────────
    broadcast_config_topic = "scada/edge/config/upsert"
    mqtt_client.subscribe(broadcast_config_topic, _handle_config_upsert)
    logger.info("[LISTENER] Suscrito a: %s  (broadcast)", broadcast_config_topic)

    # ── Configuración: específico (backend → este Edge concreto) ──────────
    specific_config_topic = f"scada/edge/{edge_id}/config/upsert"
    mqtt_client.subscribe(specific_config_topic, _handle_config_upsert)
    logger.info("[LISTENER] Suscrito a: %s  (específico)", specific_config_topic)

    # ── Comandos de escritura: broadcast ──────────────────────────────────
    broadcast_write_topic = "scada/edge/commands/write"
    mqtt_client.subscribe(broadcast_write_topic, _handle_write_command)
    logger.info("[LISTENER] Suscrito a: %s  (broadcast)", broadcast_write_topic)

    # ── Comandos de escritura: específico ─────────────────────────────────
    specific_write_topic = f"scada/edge/{edge_id}/commands/write"
    mqtt_client.subscribe(specific_write_topic, _handle_write_command)
    logger.info("[LISTENER] Suscrito a: %s  (específico)", specific_write_topic)

    logger.info("[LISTENER] Handlers registrados. Esperando mensajes de control...")
    # Mantener la tarea viva para que asyncio.gather no la descarte.
    while True:
        await asyncio.sleep(3600)



# ──────────────────────────────────────────────────────────────────────────────
# Punto de entrada del motor
# ──────────────────────────────────────────────────────────────────────────────

async def run_engine() -> None:
    """
    Arranca el motor del Edge Node.

    Se debe llamar después de inicializar el mqtt_client y el local_store.
    Ejecuta ambas tareas de forma concurrente e indefinida.
    """
    logger.info("🚀 [ENGINE] Iniciando motor Edge Node (id='%s')", settings.edge_id)

    await asyncio.gather(
        _poller_task(),
        _listener_task(),
    )
