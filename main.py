"""
main.py — Punto de entrada del microservicio scada-edge.

Secuencia de arranque:
  1. Configurar logging estructurado.
  2. Inicializar la base de datos SQLite local (crea tabla si no existe).
  3. Arrancar el cliente MQTT (conexión persistente con mTLS).
  4. Lanzar el motor de adquisición (poller + listener).
  5. En cierre limpio (Ctrl+C / SIGTERM): apagar MQTT y salir.
"""
import asyncio
import logging
import signal
import sys

from app.core.config import settings
from app.core.mqtt_client import mqtt_client
from app.db.local_store import local_store
from app.services.edge_engine import run_engine


def _configure_logging() -> None:
    """Configura logging con nivel y formato estandarizados."""
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    # Silenciar loggers muy verbosos de librerías de terceros.
    logging.getLogger("asyncua").setLevel(logging.WARNING)
    logging.getLogger("aiomqtt").setLevel(logging.WARNING)


async def _main() -> None:
    logger = logging.getLogger("main")
    logger.info("=" * 60)
    logger.info("  scada-edge — Microservicio de Adquisición de Borde")
    logger.info("  Edge ID : %s", settings.edge_id)
    logger.info("  Broker  : %s:%s  TLS=%s", settings.mqtt_broker_ip, settings.mqtt_port, settings.mqtt_use_tls)
    logger.info("  DB Path : %s", settings.db_path)
    logger.info("=" * 60)

    # ── Fase 1: Inicializar la base local ────────────────────────────────
    await local_store.init()
    tag_count = await local_store.count_tags()
    logger.info("Base local lista. Tags almacenados: %d", tag_count)

    # ── Fase 2: Iniciar cliente MQTT ──────────────────────────────────────
    await mqtt_client.startup()

    # ── Fase 3: Lanzar motor (bloqueante hasta cancelación) ───────────────
    try:
        await run_engine()
    except asyncio.CancelledError:
        logger.info("Motor cancelado — iniciando cierre limpio...")
    finally:
        await mqtt_client.shutdown()
        logger.info("scada-edge detenido limpiamente. ¡Hasta pronto! 👋")


def main() -> None:
    _configure_logging()
    logger = logging.getLogger("main")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Manejo de señales de sistema para cierre graceful (Linux/Mac/Docker).
    main_task: asyncio.Task | None = None

    def _shutdown_handler():
        logger.info("Señal de apagado recibida (SIGTERM/SIGINT). Cerrando...")
        if main_task and not main_task.done():
            main_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_handler)
        except NotImplementedError:
            # Windows no soporta add_signal_handler en asyncio.
            pass

    try:
        main_task = loop.create_task(_main())
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        logger.info("Interrupción por teclado detectada.")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
