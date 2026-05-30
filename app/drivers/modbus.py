"""
app/drivers/modbus.py — Driver Modbus TCP para el Edge Node.

Adaptado del monolito original con las siguientes mejoras:
  - read_tag() devuelve ReadResult en lugar de un valor crudo.
  - Pool de conexiones por clave "IP:Puerto:UnitID" para reutilizar sockets.
  - Manejo explícito de los tipos de registro: holding, input, coil, discrete.
  - Escritura soportada para registros holding (int) y coils (bool).
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional

from pyModbusTCP.client import ModbusClient

from app.drivers.base import EdgeDriver, ReadResult

logger = logging.getLogger(__name__)


class ModbusDriver(EdgeDriver):
    """
    Driver Modbus TCP usando pyModbusTCP.

    connection_config esperado:
    {
        "host":          "192.168.1.10",   # IP del PLC
        "port":          502,              # Puerto (default 502)
        "slave_id":      1,                # Unit ID / Slave ID (default 1)
        "register":      40001,            # Dirección del registro
        "register_type": "holding",        # holding | input | coil | discrete
        "count":         1,                # Nro de registros a leer (default 1)
    }
    """

    # Pool compartido de clientes Modbus: evita abrir una conexión TCP nueva
    # por cada lectura cuando múltiples tags apuntan al mismo dispositivo.
    _pool: Dict[str, ModbusClient] = {}

    def __init__(self, connection_config: Dict[str, Any]) -> None:
        super().__init__(connection_config)
        self.ip: str = connection_config.get("host") or connection_config.get("ip", "")
        self.port: int = int(connection_config.get("port", 502))
        self.unit_id: int = int(
            connection_config.get("slave_id") or connection_config.get("unit_id", 1)
        )
        self._key = f"{self.ip}:{self.port}:{self.unit_id}"
        self.client: Optional[ModbusClient] = None

    async def connect(self) -> bool:
        if not self.ip:
            logger.error("[MODBUS] IP no configurada en connection_config.")
            return False

        # Reutilizar conexión del pool si sigue abierta.
        if self._key in ModbusDriver._pool:
            cached = ModbusDriver._pool[self._key]
            if cached.is_open:
                self.client = cached
                self.connected = True
                return True
            else:
                del ModbusDriver._pool[self._key]

        try:
            client = ModbusClient(
                host=self.ip,
                port=self.port,
                unit_id=self.unit_id,
                auto_open=True,
                timeout=3.0,
            )
            ok = await asyncio.to_thread(client.open)
            if ok:
                ModbusDriver._pool[self._key] = client
                self.client = client
                self.connected = True
                logger.info("[MODBUS] Conectado a %s:%s (unit=%s)", self.ip, self.port, self.unit_id)
            else:
                logger.warning("[MODBUS] No se pudo conectar a %s:%s", self.ip, self.port)
                self.connected = False
            return self.connected
        except Exception as exc:
            logger.error("[MODBUS] Error de conexión: %s", exc)
            self.connected = False
            return False

    async def disconnect(self) -> None:
        # Mantenemos la conexión abierta en el pool para reutilizarla.
        # Solo marcamos el estado local como desconectado.
        self.connected = False

    async def read_tag(self, tag_config: Dict[str, Any]) -> ReadResult:
        if not self.connected:
            await self.connect()

        if not self.client or not self.client.is_open:
            return ReadResult.bad()

        register = int(tag_config.get("register", 0))
        count = int(tag_config.get("count", 1))
        reg_type = str(tag_config.get("register_type", "holding")).lower()

        try:
            raw: Optional[List[int]] = None

            if reg_type == "holding":
                raw = await asyncio.to_thread(
                    self.client.read_holding_registers, register, count
                )
            elif reg_type == "input":
                raw = await asyncio.to_thread(
                    self.client.read_input_registers, register, count
                )
            elif reg_type == "coil":
                raw = await asyncio.to_thread(
                    self.client.read_coils, register, count
                )
            elif reg_type == "discrete":
                raw = await asyncio.to_thread(
                    self.client.read_discrete_inputs, register, count
                )
            else:
                logger.warning("[MODBUS] Tipo de registro desconocido: %s", reg_type)
                return ReadResult.bad()

            if raw is None:
                logger.warning("[MODBUS] Lectura nula en reg=%s tipo=%s", register, reg_type)
                return ReadResult.bad()

            value = raw[0] if count == 1 else raw
            return ReadResult(value=float(value) if count == 1 else value, raw=raw)

        except Exception as exc:
            logger.error("[MODBUS] Error de lectura: %s", exc)
            # Invalidar la conexión del pool para forzar reconexión la próxima vez.
            ModbusDriver._pool.pop(self._key, None)
            self.connected = False
            return ReadResult.bad()

    async def write_tag(self, tag_config: Dict[str, Any], value: Any) -> bool:
        if not self.connected:
            await self.connect()

        if not self.client or not self.client.is_open:
            return False

        register = int(tag_config.get("register", 0))
        reg_type = str(tag_config.get("register_type", "holding")).lower()

        try:
            if reg_type == "holding":
                ok = await asyncio.to_thread(
                    self.client.write_single_register, register, int(value)
                )
            elif reg_type == "coil":
                ok = await asyncio.to_thread(
                    self.client.write_single_coil, register, bool(value)
                )
            else:
                logger.warning("[MODBUS] Escritura no soportada para tipo: %s", reg_type)
                return False

            if ok:
                logger.debug("[MODBUS] Escrito %s en reg=%s", value, register)
            else:
                logger.warning("[MODBUS] Escritura fallida en reg=%s", register)

            return bool(ok)

        except Exception as exc:
            logger.error("[MODBUS] Error de escritura: %s", exc)
            ModbusDriver._pool.pop(self._key, None)
            self.connected = False
            return False
