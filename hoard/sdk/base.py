from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator, List, Set, Tuple

from hoard.sdk.types import ChunkInput, DiscoverResult, EntityInput


class ConnectorV1(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique connector identifier."""

    @property
    @abstractmethod
    def version(self) -> str:
        """Semantic version string."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Source identifier used in entities."""

    @property
    def capabilities(self) -> Set[str]:
        return {"scan"}

    @abstractmethod
    def discover(self, config: dict) -> DiscoverResult:
        """Validate configuration and check source accessibility."""

    @abstractmethod
    def scan(self, config: dict) -> Iterator[Tuple[EntityInput, List[ChunkInput]]]:
        """Yield all entities and their chunks."""

    def cleanup(self) -> None:
        """Called after sync completes."""
        return None
