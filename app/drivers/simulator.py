"""
app/drivers/simulator.py — Driver de simulación para pruebas sin hardware.

Genera señales matemáticas reproducibles con timestamp del Edge.
Útil para validar el pipeline completo sin necesidad de un PLC real.

signal_type options:
  - "sine"    → Onda senoidal entre min y max (ciclo de 60 s)
  - "random"  → Valor aleatorio entre min y max
  - "static"  → Valor fijo (configurable con campo "value")
  - "ramp"    → Rampa ascendente que vuelve a min al superar max
"""
import math
import random
import time
from typing import Any, Dict

from app.drivers.base import EdgeDriver, ReadResult


class SimulatorDriver(EdgeDriver):
    """Driver de simulación que no requiere hardware."""

    async def connect(self) -> bool:
        self.connected = True
        return True

    async def disconnect(self) -> None:
        self.connected = False

    async def read_tag(self, tag_config: Dict[str, Any]) -> ReadResult:
        signal_type = str(tag_config.get("signal_type", "random")).lower()
        v_min: float = float(tag_config.get("min", 0.0))
        v_max: float = float(tag_config.get("max", 100.0))
        amplitude = (v_max - v_min) / 2.0
        center = v_min + amplitude

        if signal_type == "sine":
            # Ciclo completo cada 60 segundos.
            value = center + amplitude * math.sin(time.time() * (2 * math.pi / 60))

        elif signal_type == "random":
            value = random.uniform(v_min, v_max)

        elif signal_type == "static":
            value = float(tag_config.get("value", center))

        elif signal_type == "ramp":
            # Sube de min a max en 60 segundos, luego reinicia.
            period = float(tag_config.get("period_s", 60.0))
            value = v_min + (v_max - v_min) * ((time.time() % period) / period)

        else:
            value = 0.0

        return ReadResult(value=round(value, 4))

    async def write_tag(self, tag_config: Dict[str, Any], value: Any) -> bool:
        # En simulación, la escritura siempre "tiene éxito".
        return True
