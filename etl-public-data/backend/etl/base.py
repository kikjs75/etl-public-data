import logging
import time
from abc import ABC, abstractmethod
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Base class for all data extractors with retry and rate limiting."""

    def __init__(self, api_key: str = "", max_retries: int = 3, rate_limit_delay: float = 1.0):
        self.api_key = api_key
        self.max_retries = max_retries
        self.rate_limit_delay = rate_limit_delay
        self.client = httpx.Client(timeout=30.0)

    @property
    @abstractmethod
    def source_name(self) -> str:
        ...

    @abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        ...

    @abstractmethod
    def mock_extract(self) -> list[dict[str, Any]]:
        ...

    def fetch(self, url: str, params: dict | None = None) -> dict:
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            t0 = time.perf_counter()
            try:
                resp = self.client.get(url, params=params)
                resp.raise_for_status()
                duration_ms = int((time.perf_counter() - t0) * 1000)
                time.sleep(self.rate_limit_delay)
                logger.info(f"[{self.source_name}] HTTP GET success attempt={attempt} duration_ms={duration_ms}")
                return resp.json()
            except Exception as e:
                duration_ms = int((time.perf_counter() - t0) * 1000)
                last_error = e
                logger.warning(f"[{self.source_name}] Attempt {attempt} failed: {e} duration_ms={duration_ms}")
                time.sleep(2 ** attempt)
        raise RuntimeError(f"[{self.source_name}] All {self.max_retries} attempts failed: {last_error}")

    def close(self):
        self.client.close()


class BaseTransformer(ABC):
    """Base class for data transformers."""

    @abstractmethod
    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ...
