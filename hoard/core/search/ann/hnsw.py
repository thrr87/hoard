from __future__ import annotations

from typing import List, Sequence, Tuple

from hoard.core.search.ann.base import AnnBackend, AnnCandidates, AnnResult


class HnswAnnBackend(AnnBackend):
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
        import numpy as np

        try:
            import hnswlib
        except ImportError as exc:  # pragma: no cover - optional dependency path
            raise RuntimeError("hnswlib is not installed") from exc

        if not candidates:
            return []
        dims = len(query_vector)
        if dims <= 0:
            return []

        index = hnswlib.Index(space="cosine", dim=dims)
        index.init_index(max_elements=len(candidates), ef_construction=max(ef_construction, 10), M=max(m, 4))
        index.set_ef(max(ef_search, limit, 10))

        vectors = np.array([list(vec) for _, vec in candidates], dtype=np.float32)
        labels = np.arange(len(candidates), dtype=np.int32)
        index.add_items(vectors, labels)

        q = np.array([list(query_vector)], dtype=np.float32)
        k = min(max(limit, 1), len(candidates))
        labels_out, distances = index.knn_query(q, k=k)
        label_row = labels_out[0].tolist()
        dist_row = distances[0].tolist()

        results: List[AnnResult] = []
        for label, dist in zip(label_row, dist_row):
            item_id = candidates[int(label)][0]
            # cosine distance -> similarity
            similarity = 1.0 - float(dist)
            results.append(AnnResult(item_id=item_id, score=similarity))
        return results

