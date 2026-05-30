"""
app/drivers/opcua.py — Driver OPC UA para el Edge Node.

Adaptado del monolito original con las siguientes mejoras:
  - read_tag() devuelve ReadResult con timestamp generado en el Edge.
  - Gestión de sesión robusta: reconecta automáticamente si la sesión cae.
  - Soporte para escritura con variante de tipo opcional.

connection_config esperado:
{
    "url":     "opc.tcp://192.168.1.5:4840",  # Endpoint del servidor OPC UA
    "node_id": "ns=2;i=1001",                  # NodeID del nodo a leer
}
"""
import logging
from typing import Any, Dict, Optional

from asyncua import Client, Node

from app.drivers.base import EdgeDriver, ReadResult

logger = logging.getLogger(__name__)


class OpcUaDriver(EdgeDriver):
    """
    Driver OPC UA usando la librería asyncua.

    Se instancia por tag (un cliente por conexión única URL), pero internamente
    la librería asyncua reutiliza la sesión subyacente si la URL es idéntica.
    """

    def __init__(self, connection_config: Dict[str, Any]) -> None:
        super().__init__(connection_config)
        self.url: str = connection_config.get("url", "opc.tcp://localhost:4840")
        self._client: Optional[Client] = None

    async def connect(self) -> bool:
        if self.connected and self._client is not None:
            return True
        try:
            self._client = Client(url=self.url)
            await self._client.connect()
            self.connected = True
            logger.info("[OPCUA] Conectado a %s", self.url)
            return True
        except Exception as exc:
            logger.error("[OPCUA] Error de conexión a %s: %s", self.url, exc)
            self._client = None
            self.connected = False
            return False

    async def disconnect(self) -> None:
        if self._client and self.connected:
            try:
                await self._client.disconnect()
                logger.debug("[OPCUA] Desconectado de %s", self.url)
            except Exception:
                pass
        self._client = None
        self.connected = False

    async def read_tag(self, tag_config: Dict[str, Any]) -> ReadResult:
        if not self.connected:
            await self.connect()

        node_id: Optional[str] = tag_config.get("node_id")
        if not node_id:
            logger.error("[OPCUA] node_id no configurado en tag_config.")
            return ReadResult.bad()

        if not self._client:
            return ReadResult.bad()

        try:
            node: Node = self._client.get_node(node_id)
            raw_value = await node.read_value()
            return ReadResult(value=float(raw_value), raw=raw_value)
        except Exception as exc:
            logger.error("[OPCUA] Error de lectura (node=%s): %s", node_id, exc)
            # La sesión puede haber expirado: forzar reconexión en el próximo ciclo.
            await self.disconnect()
            return ReadResult.bad()

    async def write_tag(self, tag_config: Dict[str, Any], value: Any) -> bool:
        if not self.connected:
            await self.connect()

        node_id: Optional[str] = tag_config.get("node_id")
        if not node_id or not self._client:
            return False

        try:
            node: Node = self._client.get_node(node_id)
            # asyncua puede inferir el tipo de dato automáticamente.
            await node.write_value(value)
            logger.debug("[OPCUA] Escrito %s en node=%s", value, node_id)
            return True
        except Exception as exc:
            logger.error("[OPCUA] Error de escritura (node=%s): %s", node_id, exc)
            await self.disconnect()
            return False
