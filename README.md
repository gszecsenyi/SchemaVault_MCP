# SchemaVault

MCP server for storing and retrieving database schema information for LLMs.

## Features

- **Auto-load Databricks Unity Catalog** schemas on startup
- **Vector-based semantic search** with configurable embedding service
- **File-based storage** (no external database required)
- **MCP interface** via HTTP/SSE for LLM integration
- **LM Studio compatible**

## Quick Start

1. Copy `.env.example` to `.env` and configure:
```bash
cp .env.example .env
```

2. Configure your `.env`:
```env
# Embedding API (default: local embedding service)
EMBEDDING_API_URL=http://localhost:8000/v1
EMBEDDING_API_KEY=your-secret-token
EMBEDDING_MODEL=nomic-embed-text

# Databricks (optional)
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token
DATABRICKS_CATALOGS=main
```

3. Build and run:
```bash
docker-compose up --build
```

Server runs on `http://localhost:8001`

## MCP Tools

| Tool | Description |
|------|-------------|
| `add_schema` | Store a table schema |
| `query_model` | Semantic search for table info |
| `list_models` | List all stored tables |

## Endpoints

- `GET /mcp/sse` - SSE connection for MCP
- `POST /mcp/messages` - MCP message handler
- `GET /health` - Health check

## LM Studio Integration

Add to `~/.lmstudio/mcp.json`:
```json
{
  "mcpServers": {
    "schemavault": {
      "url": "http://localhost:8001/mcp/sse"
    }
  }
}
```

## Claude Desktop Integration

Add to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "schemavault": {
      "command": "docker",
      "args": ["exec", "-i", "schemavault-schemavault-1", "python", "-m", "src.server"]
    }
  }
}
```

## How It Works

1. On startup, cleans existing data and reloads schemas
2. Loads all schemas from Databricks Unity Catalog (if configured)
3. Embeds schemas using configured embedding service
4. Stores embeddings in Hnswlib vector index
5. LLM queries via MCP for semantic schema search

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `EMBEDDING_API_URL` | `http://localhost:8000/v1` | Embedding service URL |
| `EMBEDDING_API_KEY` | `your-secret-token` | Embedding API key |
| `EMBEDDING_MODEL` | `nomic-embed-text` | Embedding model name |
| `DATABRICKS_HOST` | - | Databricks workspace URL |
| `DATABRICKS_TOKEN` | - | Databricks PAT |
| `DATABRICKS_CATALOGS` | `main` | Catalogs to load (`main`, `a,b`, or `*`) |

## Storage

Data stored in `./data/` (refreshed on each startup):
- `vectors.index` - Hnswlib vector index (768 dimensions)
- `schemas.json` - Table metadata

## Requirements

- Docker
- Embedding service (OpenAI-compatible API)
- (Optional) Databricks workspace with Unity Catalog access
