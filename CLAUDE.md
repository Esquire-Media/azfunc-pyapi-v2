# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Azure Functions Python v2 API for enterprise advertising technology. Multiple Function Apps are deployed from a single codebase using dynamic blueprint loading.

**Entry Point:** `function_app.py` - Uses `FunctionApp` with `DecoratorApi` blueprints loaded from `deployment.py`

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
func start

# Debug: Attach to localhost:9091 (VS Code launch.json configured)
```

**Azurite** (local storage emulator): `${command:azurite.start}`

## Architecture

### Blueprint Routing Pattern

Blueprints are loaded dynamically based on `WEBSITE_SITE_NAME` or `FUNCTION_NAME` env var (`deployment.py`):

- `esquire-auto-audience` - Data lake, Postgres, Synapse, Audiences, Meta, OnSpot, S3
- `esquire-campaign-proposal` - Campaign proposal reporting
- `esquire-location-insights` - Location-based reporting
- `esquire-dashboard-data` - Dashboard endpoints
- `esquire-docs` - OpenAPI documentation
- `esquire-oneview-tasks` - OneView task management
- `esquire-redshift-sync` - AWS Redshift sync
- `esquire-sales-uploader` - Sales data ingestion
- `esquire-callback-reader` / `esquire-sales-ingestion` - Callback/sales pipelines

### Source Organization

- `libs/azure/functions/blueprints/` - All Azure Function blueprints organized by provider
- `libs/azure/data/` - Data abstraction layer (KeyValue, Structured bindings)
- `libs/azure/sql.py` - SQL utilities
- `libs/utils/` - Utility modules (geometry, OAuth, logging)
- `blueprints/` - Root-level blueprints

### Azure Functions Patterns

```python
from azure.durable_functions import Blueprint
bp = Blueprint()

@bp.timer_trigger("timer", schedule="0 */1 * * * *")
@bp.orchestration_trigger(context_name="context")
def function_name(context):
    # Use context.is_replaying check for deterministic orchestrators
    pass
```

**Custom data bindings** are registered in `libs/azure/data/` via `config.py.example` pattern.

## Dependencies

Core dependencies in `libs/requirements.txt`:
- Azure: azure-functions, azure-functions-durable, azure-identity, azure-storage-*
- Databases: boto3, pymssql, pyodbc, psycopg2, sqlalchemy
- Data: pandas, pydantic, marshmallow, fsspec, s3fs, adlfs, orjson, pyarrow
- APIs: httpx, facebook-business, googlemaps, smartystreets-python-sdk

## Configuration

- `host.json` - Function host config with Durable Task settings
- `local.settings.json` - Local credentials (DO NOT commit)
- `libs/requirements.txt` - Dependency anchor file
