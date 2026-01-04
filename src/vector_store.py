import os
import hnswlib
import numpy as np


class VectorStore:
    def __init__(self, data_dir: str, dimensions: int = 768):
        self.data_dir = data_dir
        self.dimensions = dimensions
        self.index_path = os.path.join(data_dir, "vectors.index")
        self.index = None
        self.current_id = 0
        self._load_or_create()

    def _load_or_create(self):
        self.index = hnswlib.Index(space="cosine", dim=self.dimensions)
        if os.path.exists(self.index_path):
            self.index.load_index(self.index_path)
            self.current_id = self.index.get_current_count()
        else:
            self.index.init_index(max_elements=10000, ef_construction=200, M=16)
        self.index.set_ef(50)

    def add(self, embedding: list[float]) -> int:
        vector = np.array([embedding], dtype=np.float32)
        item_id = self.current_id
        self.index.add_items(vector, np.array([item_id]))
        self.current_id += 1
        self._save()
        return item_id

    def delete(self, item_id: int):
        """Mark a vector as deleted."""
        try:
            self.index.mark_deleted(item_id)
            self._save()
        except RuntimeError:
            pass  # Item may not exist or already deleted

    def search(self, embedding: list[float], k: int = 5) -> list[tuple[int, float]]:
        if self.index.get_current_count() == 0:
            return []
        vector = np.array([embedding], dtype=np.float32)
        labels, distances = self.index.knn_query(vector, k=min(k, self.index.get_current_count()))
        return list(zip(labels[0].tolist(), distances[0].tolist()))

    def _save(self):
        os.makedirs(self.data_dir, exist_ok=True)
        self.index.save_index(self.index_path)
