import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.exceptions import RolledBackError
from kazoo.retry import KazooRetry

ROOT = Path(__file__).resolve().parent
HOSTS = os.getenv("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode()


def connect():
    logging.getLogger("kazoo").setLevel(logging.ERROR)
    client = KazooClient(hosts=HOSTS, timeout=10.0,
                         connection_retry=KazooRetry(max_tries=-1, max_delay=1),
                         command_retry=KazooRetry(max_tries=1))
    client.start(timeout=15)
    return client


def log(path, event, **values):
    entry = {"time": datetime.now().astimezone().isoformat(timespec="milliseconds"),
             "monotonic": time.monotonic(), "event": event, **values}
    line = json.dumps(entry, ensure_ascii=False, sort_keys=True)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as file:
        file.write(line + "\n")
    print(line, flush=True)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(encode(value))
    temporary.replace(path)


def check_transaction(results):
    for result in results:
        if isinstance(result, Exception) and not isinstance(result, RolledBackError):
            raise result
