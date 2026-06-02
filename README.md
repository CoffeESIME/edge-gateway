# SCADA Edge Gateway (Adquisición y Control)

Este módulo es el responsable de la interacción directa con la planta física. Ha sido diseñado utilizando el framework FastAPI bajo el paradigma asíncrono (`asyncio`) para garantizar un alto rendimiento en I/O de red, operando bajo una arquitectura descentralizada (Edge-to-Cloud).

---

## 🌐 Ecosistema SCADA Completo

Este Gateway de Adquisición Edge es solo una parte de un sistema distribuido. Para entender la arquitectura completa y levantar el sistema, revisa los otros repositorios:

1. **[dockerfiles-scada-iiot](https://github.com/CoffeESIME/dockerfiles-scada-iiot)**: Repositorio central de infraestructura que contiene los `docker-compose` para desplegar el broker MQTT, la base de datos (TimescaleDB) y generar los certificados de seguridad mTLS.
2. **[react-scada-backend](https://github.com/CoffeESIME/react-scada-backend)**: El núcleo del sistema central, encargado del CRUD de Tags, persistencia de históricos, alarmas y gestión de pantallas.
3. **[react-scada-hmi](https://github.com/CoffeESIME/react-scada-hmi)**: Interfaz gráfica web (Next.js) con un diseñador de diagramas SCADA (React Flow) para visualización en tiempo real.
4. **[edge-gateway](https://github.com/CoffeESIME/edge-gateway)**: *(Este repositorio)* Instalado en la planta física (Edge) para extraer datos de PLCs/sensores y subirlos de forma segura al broker en la nube.

## 1. Lógica de Adquisición (Backend Edge)

A diferencia de un diseño monolítico, el motor de adquisición del Edge orquesta el muestreo de datos mediante **tareas independientes por cada Tag**.

### Orquestación Concurrente (`edge_engine.py`)
El motor de este módulo implementa dos ciclos asíncronos vitales:

1. **La Tarea Maestra de Polling (`_poller_task`):** 
   Esta tarea consulta periódicamente la base de datos local SQLite para obtener la configuración de los tags activos. En lugar de iterar secuencialmente, genera de manera dinámica una sub-tarea (`asyncio.Task`) aislada para cada tag.
   
2. **Corrutina de Adquisición Individual (`_poll_single_tag`):** 
   Cada tag posee su propio bucle infinito con su tiempo de muestreo específico (`scan_rate_ms`). En cada iteración:
   - Extrae el controlador (Driver) desde la fábrica de protocolos.
   - Establece la conexión física.
   - Lee el valor del hardware y dictamina la calidad de la lectura (`GOOD` o `BAD`).
   - Envía el dato empaquetado (JSON) vía MQTT hacia el servidor central.
   - Cede el control del hilo con `asyncio.sleep`, permitiendo la concurrencia masiva.

> [!IMPORTANT]
> **Estampado de Tiempo (Timestamping):** Es vital que el Edge Node adjunte su propio `timestamp` exacto a cada mensaje MQTT. Esto evita la pérdida de correlación y la deriva de reloj en caso de caídas de red al llegar al histórico en el Backend central.

## 2. Integración de Protocolos y Hardware Industrial

Con el objetivo de garantizar la escalabilidad del sistema y cumplir con los principios SOLID (específicamente Abierto/Cerrado), se emplea el patrón **Factory Method** a través de la clase `DriverFactory` en `factory.py`.

### DriverFactory
La fábrica aisla la lógica de instanciación. Actualmente soporta:
- `ModbusDriver` (Modbus TCP)
- `OpcUaDriver` (OPC UA)
- `SimulatorDriver` (Generación de telemetría de prueba)

Cualquier nuevo protocolo industrial se puede incorporar agregando un nuevo módulo sin afectar el motor principal. Todos deben heredar e implementar el contrato de la clase base `EdgeDriver`.

### Modbus TCP y el Enlazador de Hilos (Thread Wrapper)
Integrar Modbus TCP fue un reto ya que la librería subyacente `pyModbusTCP` es de naturaleza bloqueante y síncrona. Si esta operara en el lazo asíncrono principal, detendría el Gateway por completo durante cada comunicación con la planta.

Se resolvieron estos problemas de la siguiente forma:

- **Delegación Asíncrona (`asyncio.to_thread`):** Las instrucciones que bloquean I/O, como `read_holding_registers` o `read_coils`, se ejecutan en un grupo de hilos separados provistos de forma nativa por Python. Así, el Event Loop general permanece fluido y reactivo en todo momento.
- **Pool de Conexiones (`_pool`):** En lugar de crear y destruir conexiones TCP para cada lectura, el `ModbusDriver` mantiene una caché de sockets activos indexados por la firma del PLC (`IP:Puerto:UnitID`). Esto reduce enormemente el overhead de red y previene la saturación del puerto TCP/IP de los equipos de control.
- **Resultados Estructurados (`ReadResult`):** En lugar de devolver variables crudas, devuelve un objeto completo que incluye estado de error, valor procesado y calidad intrínseca del dato.
