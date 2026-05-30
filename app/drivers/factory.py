"""
app/drivers/factory.py — Factory Method para instanciar drivers industriales.

Sigue el mismo patrón del monolito original pero adaptado al namespace del Edge.
Extender: agregar un nuevo elif aquí + crear el archivo del driver.
"""
from typing import Any, Dict

from app.drivers.base import EdgeDriver
from app.drivers.modbus import ModbusDriver
from app.drivers.opcua import OpcUaDriver
from app.drivers.simulator import SimulatorDriver


class DriverFactory:
    """
    Fábrica de drivers industriales.

    Uso:
        driver = DriverFactory.get_driver("modbus", connection_config)
        await driver.connect()
        result = await driver.read_tag(tag_config)
    """

    @staticmethod
    def get_driver(protocol: str, connection_config: Dict[str, Any]) -> EdgeDriver:
        """
        Retorna la instancia correcta de EdgeDriver según el protocolo.

        Args:
            protocol:          "modbus" | "opcua" | "simulated"
            connection_config: Diccionario con parámetros de conexión del tag.

        Raises:
            ValueError: Si el protocolo no está soportado.
        """
        p = str(protocol).lower().strip()

        # Normalizar variantes comunes.
        if "protocoltype." in p:
            p = p.split(".")[-1]

        if p == "modbus":
            return ModbusDriver(connection_config)
        elif p == "opcua":
            return OpcUaDriver(connection_config)
        elif p in ("simulated", "simulator", "sim"):
            return SimulatorDriver(connection_config)
        else:
            raise ValueError(
                f"Protocolo '{protocol}' no soportado por DriverFactory. "
                f"Opciones disponibles: modbus, opcua, simulated."
            )
