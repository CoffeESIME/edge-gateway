"""
app/drivers/base.py — Interfaz abstracta para todos los drivers industriales.

Diferencias respecto al monolito original:
  - read_tag() ahora devuelve un diccionario tipado `ReadResult` en lugar de
    un valor crudo. Esto desacopla al motor de la lógica de parsing de drivers.
  - El timestamp se genera AQUÍ (en el Edge), no en el backend central.
  - Se agrega el campo `quality` para indicar la confiabilidad de la lectura.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional


@dataclass
class ReadResult:
    """
    Resultado estandarizado de la lectura de un tag.

    Attributes:
        value:     Valor leído del dispositivo (float, int, bool).
        timestamp: Momento de la lectura en formato ISO 8601 UTC (generado en el Edge).
        quality:   Calidad de la lectura: "GOOD" si fue exitosa, "BAD" si falló.
        raw:       Valor crudo antes de cualquier conversión (opcional, para debug).
    """
    value: Any
    quality: Literal["GOOD", "BAD", "UNCERTAIN"] = "GOOD"
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    raw: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "quality": self.quality,
            "timestamp": self.timestamp,
        }

    @classmethod
    def bad(cls) -> "ReadResult":
        """Factory para resultados de lectura fallida."""
        return cls(value=None, quality="BAD")


class EdgeDriver(ABC):
    """
    Clase base abstracta para todos los drivers industriales del Edge Node.

    Cada driver concreto DEBE implementar:
      - connect()    → Establecer sesión con el dispositivo.
      - disconnect() → Cerrar sesión limpiamente.
      - read_tag()   → Leer un valor y retornar un ReadResult.
      - write_tag()  → Escribir un valor al dispositivo.
    """

    def __init__(self, connection_config: Dict[str, Any]) -> None:
        self.config = connection_config
        self.connected: bool = False

    @abstractmethod
    async def connect(self) -> bool:
        """Establece la conexión con el dispositivo físico. Retorna True si exitosa."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Cierra la conexión limpiamente."""

    @abstractmethod
    async def read_tag(self, tag_config: Dict[str, Any]) -> ReadResult:
        """
        Lee un valor del dispositivo.

        Args:
            tag_config: Parámetros específicos del tag (registro Modbus, NodeID OPC UA, etc.)

        Returns:
            ReadResult con value, quality y timestamp generado en el Edge.
        """

    @abstractmethod
    async def write_tag(self, tag_config: Dict[str, Any], value: Any) -> bool:
        """
        Escribe un valor al dispositivo.

        Args:
            tag_config: Parámetros específicos del tag.
            value:      Valor a escribir (ya escalado si aplica).

        Returns:
            True si la escritura fue confirmada por el dispositivo.
        """

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} connected={self.connected}>"
