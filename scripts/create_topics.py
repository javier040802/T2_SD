from __future__ import annotations

import json
import time

from kafka.errors import NoBrokersAvailable

from shared.config import settings
from shared.kafka_bus import create_topics


def main():
    topics = [
        (settings.kafka_main_topic, 3, 1),
        (settings.kafka_dlq_topic, 1, 1),
    ]
    for t in settings.kafka_retry_topics:
        topics.append((t, 3, 1))

    last_error = None
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            created = create_topics(topics, timeout_s=20)
            print(json.dumps({"status": "ok", "topics": created}))
            return
        except NoBrokersAvailable as exc:
            last_error = exc
            time.sleep(2)
        except Exception as exc:
            last_error = exc
            time.sleep(2)

    raise SystemExit(f"Could not create topics: {last_error}")


if __name__ == "__main__":
    main()
