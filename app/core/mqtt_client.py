"""
app/core/mqtt_client.py — Cliente MQTT persistente del Edge Node.

Implementa el patrón Singleton con:
  - Conexión TCP de larga vida usando aiomqtt.
  - Soporte completo de mTLS (CA + certificado de cliente).
  - Cola de publicación no bloqueante (asyncio.Queue).
  - Reconexión automática con backoff exponencial.
  - Registro de handlers para recibir mensajes en tópicos suscritos.

Topología de puertos:
  Puerto 1883 → Cleartext (desarrollo local, mqtt_use_tls=False)
  Puerto 8883 → TLS / mTLS estricto (producción EDGE)
"""
import asyncio
import logging
import ssl
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import aiomqtt

from app.core.config import settings

logger = logging.getLogger(__name__)

# Tipo del handler de mensajes: recibe (topic: str, payload: bytes)
MessageHandler = Callable[[str, bytes], Coroutine[Any, Any, None]]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers TLS
# ──────────────────────────────────────────────────────────────────────────────

def _build_tls_context() -> Optional[ssl.SSLContext]:
    """
    Construye un SSLContext según la configuración:

    - mqtt_use_tls=False  → None (sin cifrado, sólo para desarrollo)
    - mqtt_use_tls=True, sin cert cliente → TLS server-only
    - mqtt_use_tls=True, con cert cliente → mTLS bidireccional estricto
    """
    if not settings.mqtt_use_tls:
        logger.warning(
            "[MQTT] TLS deshabilitado — conexión en cleartext (¡sólo para desarrollo!)"
        )
        return None

    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED

    # Carga el CA que firmó el certificado del broker.
    ca_path = settings.cert_ca_path
    if ca_path and ca_path.exists():
        ctx.load_verify_locations(cafile=str(ca_path))
        logger.debug("[MQTT] TLS: CA cargada desde %s", ca_path)
    else:
        logger.warning(
            "[MQTT] CA no encontrada en %s — el servidor no será verificado", ca_path
        )
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    # mTLS: carga el certificado y clave del cliente Edge.
    client_cert = settings.cert_client_path
    client_key = settings.cert_key_path
    if client_cert and client_key and client_cert.exists() and client_key.exists():
        ctx.load_cert_chain(certfile=str(client_cert), keyfile=str(client_key))
        logger.info("[MQTT] mTLS habilitado: cert=%s", client_cert)
    else:
        logger.info("[MQTT] Modo TLS simple (sin certificado de cliente).")

    return ctx


# ──────────────────────────────────────────────────────────────────────────────
# Cliente MQTT Singleton
# ──────────────────────────────────────────────────────────────────────────────

class EdgeMqttClient:
    """
    Cliente MQTT Singleton para el Edge Node.

    Ciclo de vida:
      1. `startup()` se llama desde main() y arranca el loop de conexión.
      2. `_connection_loop()` mantiene la sesión TCP indefinidamente.
      3. `publish()` encola mensajes de forma no bloqueante.
      4. `subscribe()` registra handlers para tópicos entrantes.
      5. `shutdown()` cierra la conexión limpiamente al terminar.
    """

    _instance: Optional["EdgeMqttClient"] = None

    def __new__(cls) -> "EdgeMqttClient":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        self._tls_ctx: Optional[ssl.SSLContext] = _build_tls_context()

        # Estado interno
        self._client: Optional[aiomqtt.Client] = None
        self._connected: bool = False
        self._task: Optional[asyncio.Task] = None

        # Cola de publicación (topic, payload, qos, retain)
        self._publish_queue: asyncio.Queue[
            Tuple[str, str | bytes, int, bool]
        ] = asyncio.Queue(maxsize=2_000)

        # Suscripciones registradas: {topic_pattern: [handler, ...]}
        self._subscriptions: Dict[str, List[MessageHandler]] = {}

        logger.info(
            "[MQTT] EdgeMqttClient listo | broker=%s:%s  tls=%s  id=%s",
            settings.mqtt_broker_ip,
            settings.mqtt_port,
            settings.mqtt_use_tls,
            settings.mqtt_client_id,
        )

    # ── API pública ────────────────────────────────────────────────────────

    async def startup(self) -> None:
        """Arranca el loop de conexión persistente como tarea de fondo."""
        if self._task and not self._task.done():
            logger.warning("[MQTT] startup() llamado pero el loop ya está activo.")
            return
        self._task = asyncio.create_task(
            self._connection_loop(), name="edge-mqtt-connection-loop"
        )
        logger.info("[MQTT] Background task 'edge-mqtt-connection-loop' iniciado.")

    async def shutdown(self) -> None:
        """Cancela el loop y espera cierre limpio (máx 5 s)."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        self._connected = False
        logger.info("[MQTT] Cliente detenido limpiamente.")

    async def publish(
        self,
        topic: str,
        payload: str | bytes,
        qos: int = 1,
        retain: bool = False,
    ) -> bool:
        """
        Encola un mensaje para publicación asíncrona.

        No bloquea el hilo de adquisición: inserta en la cola y regresa.
        Retorna False si la cola está llena (se descarta el mensaje).
        """
        try:
            self._publish_queue.put_nowait((topic, payload, qos, retain))
            return True
        except asyncio.QueueFull:
            logger.error(
                "[MQTT] Cola llena (%s msgs). Mensaje descartado → %s",
                self._publish_queue.maxsize,
                topic,
            )
            return False

    def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """
        Registra un handler asíncrono para un tópico (soporta wildcards MQTT).

        El handler se invoca con (topic: str, payload: bytes) por cada
        mensaje que coincida con el patrón.

        Llamar antes de startup() es válido; las suscripciones se envían al
        broker en cuanto se establece la primera conexión.
        """
        if topic not in self._subscriptions:
            self._subscriptions[topic] = []
        self._subscriptions[topic].append(handler)
        logger.debug("[MQTT] Handler registrado para tópico: %s", topic)

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Loop de conexión ───────────────────────────────────────────────────

    async def _connection_loop(self) -> None:
        """
        Mantiene la sesión MQTT activa indefinidamente.

        Al desconectarse, espera `mqtt_reconnect_delay` segundos antes de
        reintentar, con backoff progresivo hasta 60 s.
        """
        delay = settings.mqtt_reconnect_delay
        max_delay = 60.0

        while True:
            try:
                logger.info(
                    "[MQTT] Conectando a %s:%s ...",
                    settings.mqtt_broker_ip,
                    settings.mqtt_port,
                )
                async with aiomqtt.Client(
                    hostname=settings.mqtt_broker_ip,
                    port=settings.mqtt_port,
                    identifier=settings.mqtt_client_id,
                    username=settings.mqtt_username,
                    password=settings.mqtt_password,
                    keepalive=settings.mqtt_keepalive,
                    tls_context=self._tls_ctx,
                ) as client:
                    self._client = client
                    self._connected = True
                    delay = settings.mqtt_reconnect_delay  # Reset backoff
                    logger.info("[MQTT] ✓ Conectado al broker.")

                    # Suscribirse a todos los tópicos registrados.
                    for topic in self._subscriptions:
                        await client.subscribe(topic)
                        logger.info("[MQTT] Suscrito a: %s", topic)

                    # Procesar publicaciones y mensajes entrantes de forma concurrente.
                    await asyncio.gather(
                        self._drain_publish_queue(client),
                        self._receive_messages(client),
                    )

            except asyncio.CancelledError:
                logger.info("[MQTT] Loop cancelado — saliendo.")
                break
            except aiomqtt.MqttError as exc:
                self._connected = False
                self._client = None
                logger.warning(
                    "[MQTT] Error MQTT: %s — reintentando en %.1fs", exc, delay
                )
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, max_delay)
            except Exception as exc:
                self._connected = False
                self._client = None
                logger.exception("[MQTT] Error inesperado: %s — reintentando.", exc)
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, max_delay)
            finally:
                self._connected = False
                self._client = None

    async def _drain_publish_queue(self, client: aiomqtt.Client) -> None:
        """Vacía la cola de publicación mientras hay conexión activa."""
        while True:
            try:
                topic, payload, qos, retain = await asyncio.wait_for(
                    self._publish_queue.get(), timeout=1.0
                )
                await client.publish(topic, payload, qos=qos, retain=retain)
                self._publish_queue.task_done()
                logger.debug("[MQTT] Publicado → %s (qos=%s)", topic, qos)
            except asyncio.TimeoutError:
                continue
            except aiomqtt.MqttError:
                raise  # Sube al connection_loop para reconectar.
            except asyncio.CancelledError:
                break

    async def _receive_messages(self, client: aiomqtt.Client) -> None:
        """Recibe mensajes del broker y despacha a los handlers registrados."""
        async for message in client.messages:
            topic_str = str(message.topic)
            payload_bytes = message.payload  # type: ignore[arg-type]

            # Buscar handlers cuyo patrón coincida con el tópico recibido.
            matched = False
            for pattern, handlers in self._subscriptions.items():
                if _topic_matches(pattern, topic_str):
                    matched = True
                    for handler in handlers:
                        try:
                            await handler(topic_str, payload_bytes)
                        except Exception as exc:
                            logger.error(
                                "[MQTT] Error en handler para %s: %s", topic_str, exc
                            )
            if not matched:
                logger.debug("[MQTT] Mensaje sin handler registrado: %s", topic_str)


# ──────────────────────────────────────────────────────────────────────────────
# Utilidades
# ──────────────────────────────────────────────────────────────────────────────

def _topic_matches(pattern: str, topic: str) -> bool:
    """
    Comprueba si un tópico MQTT coincide con un patrón que puede contener
    wildcards '+' (un nivel) o '#' (múltiples niveles finales).
    """
    pattern_parts = pattern.split("/")
    topic_parts = topic.split("/")

    for i, pp in enumerate(pattern_parts):
        if pp == "#":
            return True  # Coincide el resto
        if i >= len(topic_parts):
            return False
        if pp != "+" and pp != topic_parts[i]:
            return False

    return len(pattern_parts) == len(topic_parts)


# Singleton global.
mqtt_client = EdgeMqttClient()
