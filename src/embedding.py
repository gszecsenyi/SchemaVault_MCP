import os
from openai import OpenAI


class EmbeddingService:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("EMBEDDING_API_KEY", "your-secret-token"),
            base_url=os.getenv("EMBEDDING_API_URL", "http://localhost:8000/v1")
        )
        self.model = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
        self.dimensions = 768

    def embed(self, text: str) -> list[float]:
        response = self.client.embeddings.create(
            input=text,
            model=self.model
        )
        return response.data[0].embedding

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        response = self.client.embeddings.create(
            input=texts,
            model=self.model
        )
        return [item.embedding for item in response.data]
