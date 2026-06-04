# Tarea 2 - Sistemas Distribuidos

Esta version extiende la Tarea 1 hacia una arquitectura asíncrona con Apache Kafka, reintentos y DLQ.

## Componentes

- `traffic-generator`: genera consultas Q1-Q5 y las publica en Kafka.
- `kafka-consumer`: consume desde el topic principal y los topics de reintento.
- `cache-service`: mantiene la caché y consulta al `response-service` ante miss.
- `response-service`: calcula las respuestas en memoria y permite simular fallas temporales.
- `metrics-service`: almacena eventos y calcula throughput, latencia, retry rate, recovery rate, DLQ rate y backlog.
- `dashboard`: muestra métricas en Streamlit.
- `redis`: caché.
- `zookeeper` + `kafka`: broker Kafka.

## Flujo

1. El generador publica la consulta en `queries.main`.
2. Un consumidor del mismo grupo toma el mensaje.
3. Consulta la caché.
4. Si hay miss, deriva al generador de respuestas.
5. Si hay falla temporal, publica en el topic de reintento.
6. Tras agotar reintentos, la consulta pasa a `queries.dlq`.

## Ejecución

```bash
docker compose up --build
```

Para escalar consumidores:

```bash
docker compose up --scale kafka-consumer=3
```

Dashboard:

- `http://localhost:8501`

Métricas:

- `http://localhost:8002/summary`
- `http://localhost:8002/events`

## Simular fallas

```bash
curl -X POST "http://localhost:8001/faults?enabled=true&failure_rate=0.5&delay_ms=1000"
```

Desactivar:

```bash
curl -X POST "http://localhost:8001/faults?enabled=false&failure_rate=0&delay_ms=0"
```

## Benchmarks

Modo síncrono:

```bash
python scripts/benchmark.py --mode sync --url http://localhost:8000/query --distribution zipf --n 200
```

Modo Kafka:

```bash
python scripts/benchmark.py --mode kafka --distribution zipf --n 200
```

## Archivos de entrega

- Código fuente completo
- `docker-compose.yml`
- Informe técnico en PDF
- Video de demostración


## Correcciones de entrega

- El servicio de métricas registra `generated`, `processed`, `recovered`, `retry`, `failure` y `dlq`, para que las tasas de reintento, recuperación y DLQ queden calculadas de forma consistente.
- El consumidor publica mensajes de fallo antes de reencolar o enviar a DLQ, lo que permite auditar correctamente el comportamiento durante fallas temporales.
- El benchmark crea la carpeta de salida automáticamente y espera a que el flujo Kafka termine antes de guardar el resumen.
