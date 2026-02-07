from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple


@dataclass(frozen=True)
class AnnResult:
    item_id: str
    score: float


AnnCandidates = Sequence[Tuple[str, Sequence[float]]]


class AnnBackend:
    def search(
        self,
        *,
        query_vector: Sequence[float],
        candidates: AnnCandidates,
        limit: int,
        ef_search: int,
        m: int,
        ef_construction: int,
    ) -> List[AnnResult]:
        raise NotImplementedError

