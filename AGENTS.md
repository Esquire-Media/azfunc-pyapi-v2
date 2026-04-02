# AGENTS.md

This file provides guidance to agentic coding agents working in this repository.

## Project Overview

Azure Functions Python v2 API for enterprise advertising technology. Multiple Function Apps are deployed from a single codebase using dynamic blueprint loading.

**Entry Point:** `function_app.py` - Uses `FunctionApp` with `DecoratorApi` blueprints loaded from `deployment.py`

**Python Version:** 3.12

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
func start

# Debug: Attach to localhost:9091 (VS Code launch.json configured)
```

**Azurite** (local storage emulator): `${command:azurite.start}`

## Build/Lint/Test

This project uses **Black** for formatting (configured in `.vscode/settings.json`).

```bash
# Format all Python files with Black
black .

# Format specific file/directory
black libs/azure/functions/blueprints/esquire/

# Run with check only (no changes)
black --check .

# Install pre-commit hooks (if configured)
pre-commit run --all-files
```

### Testing

Tests are located in `libs/azure/functions/blueprints/esquire/audiences/utils/maids/starters/tests/`. To run a single test:

```bash
# Run pytest on specific test file
pytest libs/azure/functions/blueprints/esquire/audiences/utils/maids/starters/tests/fetch.py -v

# Run specific test function
pytest libs/azure/functions/blueprints/esquire/audiences/utils/maids/starters/tests/fetch.py::starter_esquireAudiencesMaidsTest_fetch -v
```

## Code Style

### Formatting

- **Formatter:** Black (line length: 88 characters default)
- **Quote Style:** Single quotes for strings, double quotes for multi-line strings or when containing single quotes
- **Line Length:** Max 88 characters (Black default)
- **Trailing Commas:** Use for collections that span multiple lines

### Imports

```python
# Standard library first, then third-party, then local
import os
import logging
from typing import Union

import orjson
import pandas as pd
from azure.functions import HttpRequest
from azure.durable_functions import Blueprint

from libs.azure.sql import GenerateAzSQLConnectionString
from libs.utils.geometry import geojson2shape
```

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Functions/Variables | snake_case | `get_instance_id`, `orchestration_function_name` |
| Classes | PascalCase | `ValidateMicrosoft`, `TokenValidationError` |
| Constants | UPPER_SNAKE | `MAX_RETRY_COUNT`, `CONNECTION_TIMEOUT` |
| Type Aliases | PascalCase | `GeoJsonType`, `WkbType` |
| Azure Functions | snake_case | `activity_salesIngestor_createStagingTable` |
| Blueprints | snake_case, verb prefix | `bp`, `starter_salesIngestor` |

### Type Hints

Use type hints for all function parameters and return values. Use `Union` from `typing` for union types:

```python
from typing import Union, Iterable

def process_data(data: dict, options: Union[list, None] = None) -> dict:
    ...

class ValidateMicrosoft:
    def __init__(
        self, 
        tenant_id: str, 
        client_id: Union[str, Iterable[str], None], 
        authority: str = "login.microsoftonline.com"
    ) -> None:
        ...
```

### Docstrings

Use numpy-style docstrings for all public functions and classes:

```python
def geojson2shape(data: Union[GeoJsonType, dict]) -> Union[GeometryCollection, Polygon, MultiPolygon]:
    """
    Converts GeoJSON format to Shapely geometry objects.

    Parameters
    ----------
    data : Union[GeoJsonType, dict]
        The GeoJSON geometry or dictionary representing the Geo_json geometry.

    Returns
    -------
    Union[GeometryCollection, Polygon, MultiPolygon]
        The converted Shapely geometry.

    Notes
    -----
    This function converts GeoJSON format to Shapely geometry objects...
    """
```

### Error Handling

Use specific exception types and structured error responses:

```python
# For validation errors, return early with HttpResponse
try:
    payload = json.loads(payload)
except Exception:
    logger.exception("Invalid JSON payload")
    return HttpResponse(status_code=400, body="Invalid JSON payload.")

# For expected errors, use custom exception classes
class TokenValidationError(Exception):
    """Custom exception for handling token validation errors."""
    pass

# For orchestrator errors, catch and handle with traceback
try:
    settings = context.get_input()
except Exception as e:
    logger.error(msg=e, extra={"context": {"PartitionKey": settings["metadata"]["upload_id"]}})
    full_trace = traceback.format_exc()
    raise e
```

### Logging

Use structured logging with context:

```python
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

# Log with context
logger.info(
    msg="started",
    extra={"context": {"PartitionKey": "salesIngestor", "RowKey": instance_id, ...}},
)

# Use logger.exception for exceptions (includes stack trace)
logger.exception("Invalid JSON payload")
```

## Architecture

### Blueprint Routing Pattern

Blueprints are loaded dynamically based on `WEBSITE_SITE_NAME` or `FUNCTION_NAME` env var (`deployment.py`):

| Function App | Blueprints |
|-------------|------------|
| `esquire-auto-audience` | Data lake, Postgres, Synapse, Audiences, Meta, OnSpot, S3 |
| `esquire-campaign-proposal` | Campaign proposal reporting |
| `esquire-location-insights` | Location-based reporting |
| `esquire-dashboard-data` | Dashboard endpoints |
| `esquire-docs` | OpenAPI documentation |
| `esquire-oneview-tasks` | OneView task management |
| `esquire-redshift-sync` | AWS Redshift sync |
| `esquire-sales-uploader` | Sales data ingestion |
| `esquire-callback-reader` / `esquire-sales-ingestion` | Callback/sales pipelines |

### Source Organization

- `libs/azure/functions/blueprints/` - All Azure Function blueprints organized by provider
- `libs/azure/data/` - Data abstraction layer (KeyValue, Structured bindings)
- `libs/azure/sql.py` - SQL utilities
- `libs/utils/` - Utility modules (geometry, OAuth, logging)
- `blueprints/` - Root-level blueprints

### Azure Functions Patterns

```python
from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from azure.functions import HttpRequest, HttpResponse, TimerRequest

bp = Blueprint()

# HTTP Trigger
@bp.route(route="esquire/sales_ingestor/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_salesIngestor(req: HttpRequest, client: DurableOrchestrationClient):
    ...

# Timer Trigger
@bp.timer_trigger("timer", schedule="0 */1 * * * *")
def keep_alive(timer: TimerRequest):
    ...

# Activity Trigger
@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_createStagingTable(settings: dict):
    ...

# Orchestrator
@bp.orchestration_trigger(context_name="context")
def orchestrator_salesIngestor(context: DurableOrchestrationContext):
    # Use context.is_replaying check for deterministic orchestrators
    # Use yield for await-like patterns in orchestrators
    yield context.call_activity_with_retry(...)
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
