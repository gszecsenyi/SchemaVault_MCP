"""HTTP/SSE MCP server for LM Studio and other HTTP-based clients."""
import os
import json
import logging
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent

from .embedding import EmbeddingService
from .vector_store import VectorStore
from .schema_storage import SchemaStorage, TableSchema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.getenv("DATA_DIR", "/app/data")


def cleanup_data():
    """Delete existing vectors and schemas on startup."""
    files_to_delete = [
        os.path.join(DATA_DIR, "vectors.index"),
        os.path.join(DATA_DIR, "schemas.json"),
    ]
    for filepath in files_to_delete:
        if os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Deleted {filepath}")


cleanup_data()

# Initialize services
embedding_service = EmbeddingService()
vector_store = VectorStore(DATA_DIR)
schema_storage = SchemaStorage(DATA_DIR)

# Create MCP server
mcp_server = Server("schemavault")

# SSE transport - path is for the POST messages endpoint
sse = SseServerTransport("/mcp/messages")


@mcp_server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="add_schema",
            description="Store a database table schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name"},
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "type": {"type": "string"},
                                "primary": {"type": "boolean"},
                                "nullable": {"type": "boolean"},
                                "description": {"type": "string"}
                            },
                            "required": ["name", "type"]
                        }
                    },
                    "description": {"type": "string", "description": "Table description"}
                },
                "required": ["table", "columns"]
            }
        ),
        Tool(
            name="query_model",
            description="Get schema info for a table/model by name or semantic search",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Table name or search query"}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="list_models",
            description="List all stored table schemas",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        )
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "add_schema":
        schema = TableSchema(**arguments)
        text = schema_storage.to_text(schema)
        embedding = embedding_service.embed(text)
        vector_id = vector_store.add(embedding)
        schema_storage.add(vector_id, schema)
        return [TextContent(type="text", text=f"Stored schema for table '{schema.table}' with ID {vector_id}")]

    elif name == "query_model":
        query = arguments["query"]
        exact = schema_storage.get_by_name(query)
        if exact:
            results = [exact]
        else:
            embedding = embedding_service.embed(query)
            matches = vector_store.search(embedding, k=3)
            results = []
            for vector_id, score in matches:
                schema = schema_storage.get(vector_id)
                if schema:
                    results.append(schema)

        if not results:
            return [TextContent(type="text", text=f"No schemas found for '{query}'")]

        output = []
        for schema in results:
            cols = "\n".join([f"  - {c.name}: {c.type}{' (PK)' if c.primary else ''}" for c in schema.columns])
            desc = f"\n  Description: {schema.description}" if schema.description else ""
            output.append(f"Table: {schema.table}{desc}\n  Columns:\n{cols}")

        return [TextContent(type="text", text="\n\n".join(output))]

    elif name == "list_models":
        tables = schema_storage.list_all()
        if not tables:
            return [TextContent(type="text", text="No schemas stored yet")]
        return [TextContent(type="text", text="Stored tables:\n" + "\n".join(f"  - {t}" for t in tables))]

    return [TextContent(type="text", text=f"Unknown tool: {name}")]


def load_databricks_schemas():
    """Load schemas from Databricks Unity Catalog on startup."""
    if not os.getenv("DATABRICKS_HOST") or not os.getenv("DATABRICKS_TOKEN"):
        logger.info("Databricks env vars not set. Skipping auto-load.")
        return

    try:
        from .databricks_loader import DatabricksLoader

        logger.info("Loading schemas from Databricks Unity Catalog...")
        loader = DatabricksLoader()
        schemas = loader.load_catalog_schemas()

        logger.info(f"Found {len(schemas)} tables in catalogs '{loader.catalogs_filter}'")

        for schema in schemas:
            text = schema_storage.to_text(schema)
            embedding = embedding_service.embed(text)
            vector_id = vector_store.add(embedding)
            schema_storage.add(vector_id, schema)
            logger.info(f"Stored: {schema.table}")

        logger.info("Databricks schema load complete.")

    except Exception as e:
        logger.error(f"Failed to load Databricks schemas: {e}")


# Load schemas on module import
load_databricks_schemas()


async def app(scope, receive, send):
    """Raw ASGI application."""
    if scope["type"] != "http":
        return

    path = scope["path"]
    method = scope["method"]

    # Health check endpoint
    if path == "/health" and method == "GET":
        body = json.dumps({"status": "ok", "tables": len(schema_storage.list_all())})
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/json"]],
        })
        await send({
            "type": "http.response.body",
            "body": body.encode(),
        })
        return

    # SSE endpoint for MCP
    if path == "/mcp/sse" and method == "GET":
        async with sse.connect_sse(scope, receive, send) as streams:
            await mcp_server.run(
                streams[0], streams[1], mcp_server.create_initialization_options()
            )
        return

    # Messages endpoint for MCP
    if path.startswith("/mcp/messages") and method == "POST":
        await sse.handle_post_message(scope, receive, send)
        return

    # 404 for unknown routes
    await send({
        "type": "http.response.start",
        "status": 404,
        "headers": [[b"content-type", b"text/plain"]],
    })
    await send({
        "type": "http.response.body",
        "body": b"Not Found",
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
