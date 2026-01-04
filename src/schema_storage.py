import os
import json
import hashlib
from pydantic import BaseModel


class Column(BaseModel):
    name: str
    type: str
    primary: bool = False
    nullable: bool = True
    description: str | None = None


class TableSchema(BaseModel):
    table: str
    columns: list[Column]
    description: str | None = None


class SchemaStorage:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.metadata_path = os.path.join(data_dir, "schemas.json")
        self.schemas: dict[int, TableSchema] = {}
        self.hashes: dict[int, str] = {}  # vector_id -> hash
        self._load()

    def _load(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "r") as f:
                data = json.load(f)
                for k, v in data.items():
                    vector_id = int(k)
                    self.schemas[vector_id] = TableSchema(**v["schema"])
                    self.hashes[vector_id] = v.get("hash", "")

    def _save(self):
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.metadata_path, "w") as f:
            data = {
                k: {"schema": v.model_dump(), "hash": self.hashes.get(k, "")}
                for k, v in self.schemas.items()
            }
            json.dump(data, f, indent=2)

    def add(self, vector_id: int, schema: TableSchema, schema_hash: str = ""):
        self.schemas[vector_id] = schema
        self.hashes[vector_id] = schema_hash
        self._save()

    def remove(self, vector_id: int):
        """Remove a schema by vector ID."""
        if vector_id in self.schemas:
            del self.schemas[vector_id]
        if vector_id in self.hashes:
            del self.hashes[vector_id]
        self._save()

    def get(self, vector_id: int) -> TableSchema | None:
        return self.schemas.get(vector_id)

    def get_by_name(self, table_name: str) -> TableSchema | None:
        for schema in self.schemas.values():
            if schema.table.lower() == table_name.lower():
                return schema
        return None

    def get_vector_id_by_name(self, table_name: str) -> int | None:
        """Get the vector ID for a table by name."""
        for vector_id, schema in self.schemas.items():
            if schema.table.lower() == table_name.lower():
                return vector_id
        return None

    def get_hash_by_name(self, table_name: str) -> str | None:
        """Get the stored hash for a table by name."""
        vector_id = self.get_vector_id_by_name(table_name)
        if vector_id is not None:
            return self.hashes.get(vector_id)
        return None

    def list_all(self) -> list[str]:
        return [s.table for s in self.schemas.values()]

    def to_text(self, schema: TableSchema) -> str:
        """Convert schema to text for embedding."""
        cols = ", ".join([f"{c.name} ({c.type})" for c in schema.columns])
        text = f"Table: {schema.table}. Columns: {cols}."
        if schema.description:
            text += f" Description: {schema.description}"
        return text

    @staticmethod
    def compute_hash(schema: TableSchema) -> str:
        """Compute a hash for a schema to detect changes."""
        schema_json = schema.model_dump_json(exclude_none=False)
        return hashlib.md5(schema_json.encode()).hexdigest()
