import os
import json
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
        self._load()

    def _load(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "r") as f:
                data = json.load(f)
                self.schemas = {int(k): TableSchema(**v) for k, v in data.items()}

    def _save(self):
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.metadata_path, "w") as f:
            json.dump({k: v.model_dump() for k, v in self.schemas.items()}, f, indent=2)

    def add(self, vector_id: int, schema: TableSchema):
        self.schemas[vector_id] = schema
        self._save()

    def get(self, vector_id: int) -> TableSchema | None:
        return self.schemas.get(vector_id)

    def get_by_name(self, table_name: str) -> TableSchema | None:
        for schema in self.schemas.values():
            if schema.table.lower() == table_name.lower():
                return schema
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
