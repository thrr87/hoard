from __future__ import annotations

from typing import List


class EmbeddingError(Exception):
    pass


class EmbeddingModel:
    def __init__(self, model_name: str) -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise EmbeddingError(
                "sentence-transformers is not installed. Install with `pip install \"hoard[vectors]\"`."
            ) from exc

        self.model = SentenceTransformer(model_name)
        self.model_name = model_name
        self.dims = self.model.get_sentence_embedding_dimension()

    def encode(self, texts: List[str], batch_size: int = 32) -> List[List[float]]:
        embeddings = self.model.encode(
            texts,
            batch_size=batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return embeddings.tolist()
