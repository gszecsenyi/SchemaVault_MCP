import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo, ColumnInfo
from .schema_storage import TableSchema, Column


class DatabricksLoader:
    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.catalogs_filter = os.getenv("DATABRICKS_CATALOGS", "main")
        self.schemas_filter = os.getenv("DATABRICKS_SCHEMAS", "")

        if not self.host or not self.token:
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

        self.client = WorkspaceClient(
            host=self.host,
            token=self.token
        )

    def _get_catalog_list(self) -> list[str] | None:
        """Parse DATABRICKS_CATALOGS env var. Returns None for '*' (all catalogs)."""
        if self.catalogs_filter == "*":
            return None
        return [c.strip() for c in self.catalogs_filter.split(",")]

    def _get_schema_list(self) -> list[str] | None:
        """Parse DATABRICKS_SCHEMAS env var. Returns None if not set or '*' (all schemas)."""
        if not self.schemas_filter or self.schemas_filter == "*":
            return None
        return [s.strip() for s in self.schemas_filter.split(",")]

    def load_catalog_schemas(self) -> list[TableSchema]:
        """Load tables from the specified catalogs and schemas."""
        results = []
        catalog_filter = self._get_catalog_list()
        schema_filter = self._get_schema_list()

        # Get catalogs to process
        if catalog_filter is None:
            catalogs = [c.name for c in self.client.catalogs.list(include_browse=True)]
        else:
            catalogs = catalog_filter

        for catalog_name in catalogs:
            # List all schemas in the catalog (include_browse for browse-only access)
            for schema_info in self.client.schemas.list(
                catalog_name=catalog_name,
                include_browse=True
            ):
                schema_name = schema_info.name

                # Apply schema filter if set
                if schema_filter is not None and schema_name not in schema_filter:
                    continue

                # List all tables in the schema (include_browse for browse-only access)
                for table_info in self.client.tables.list(
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    include_browse=True
                ):
                    table_schema = self._convert_table_info(table_info)
                    if table_schema:
                        results.append(table_schema)

        return results

    def _convert_table_info(self, table_info: TableInfo) -> TableSchema | None:
        """Convert Databricks TableInfo to our TableSchema format."""
        if not table_info.columns:
            return None

        columns = []
        for col in table_info.columns:
            columns.append(Column(
                name=col.name,
                type=col.type_name.value if col.type_name else "unknown",
                primary=self._is_primary_key(col),
                nullable=col.nullable if col.nullable is not None else True,
                description=col.comment
            ))

        # Build comprehensive description
        desc_parts = []
        if table_info.comment:
            desc_parts.append(table_info.comment)
        if table_info.table_type:
            desc_parts.append(f"Type: {table_info.table_type.value}")
        if table_info.storage_location:
            desc_parts.append(f"Location: {table_info.storage_location}")

        description = " | ".join(desc_parts) if desc_parts else None

        full_name = f"{table_info.catalog_name}.{table_info.schema_name}.{table_info.name}"

        return TableSchema(
            table=full_name,
            columns=columns,
            description=description
        )

    def _is_primary_key(self, col: ColumnInfo) -> bool:
        """Check if column is part of primary key."""
        # Note: Unity Catalog doesn't always expose PK info directly
        # This is a simplified check
        return False  # Can be enhanced if constraint info is available
