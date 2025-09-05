# app/events.py
from __future__ import annotations
from collections import deque
from threading import Lock
from time import time

class EventBus:
  
    def __init__(self, maxlen: int = 1000):
        self._q = deque(maxlen=maxlen)
        self._lock = Lock()

    def publish(self, ev: dict) -> None:
        ev.setdefault("ts_ms", int(time() * 1000))
        with self._lock:
            self._q.append(ev)

    def tail(self, n: int = 50) -> list:
        with self._lock:
            return list(self._q)[-n:]

bus = EventBus()
