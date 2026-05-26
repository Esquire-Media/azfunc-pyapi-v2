# Azure Functions Dynamic Python API

A mono-repo for large durable function projects deployed as multiple Azure Function Apps from a single codebase using dynamic blueprint loading. Developed with Python 3.12.

## Project Overview

Azure Functions Python v2 API for enterprise advertising technology. The codebase uses a dynamic blueprint loading pattern where Function Apps are resolved at runtime based on the `WEBSITE_SITE_NAME` or `FUNCTION_NAME` environment variable.

## Architecture

The `deployment.py` file maps Function App names to their blueprint paths. When a function is invoked, the system loads only the blueprints associated with the deployed Function App, enabling a single codebase to serve multiple deployments.

## Function Apps

| Function App | Blueprints |
|-------------|------------|
| `esquire-auto-audience` | Data lake, Postgres, Synapse, Audiences builder/activities, Microsoft Graph, Meta, OnSpot, S3 |
| `esquire-meta` | Meta egress, newest audience blob prefix, Synapse |
| `esquire-freewheel` | Freewheel egress, newest audience blob paths, Synapse |
| `esquire-autopolygon` | Rooftop polygons |
| `esquire-campaign-proposal` | Keep alive, Data lake, Campaign proposal reporting, Microsoft Graph |
| `esquire-location-insights` | Keep alive, Data lake, Synapse, Location insights, OnSpot, Microsoft Graph |
| `esquire-dashboard-data` | Data lake, Dashboard OnSpot, Keep alive, Meta creative preview, OnSpot, Synapse |
| `esquire-docs` | OpenAPI documentation |
| `esquire-oneview-tasks` | OneView tasks |
| `esquire-redshift-sync` | AWS Redshift sync |
| `esquire-sales-uploader` | Keep alive, Sales uploader matchback, Smarty |
| `esquire-callback-reader` | Callback reader |
| `esquire-sales-ingestion` | Keep alive, Sales ingestor, Microsoft Graph |
| `debug` | Keep alive, Logger |
| `debug_env` | Env (DO NOT enable in public environments) |

## Directory Structure

- `libs/azure/functions/blueprints/` - All Azure Function blueprints organized by provider
  - `azure/` - Azure-specific blueprints (datalake, postgres, synapse)
  - `esquire/` - Esquire-specific blueprints (audiences, reporting, dashboard, etc.)
  - `microsoft/` - Microsoft integrations (Graph API)
  - `meta/` - Meta/Facebook integrations
  - `onspot/` - OnSpot integrations
  - `s3/` - AWS S3 integrations
  - `aws/` - AWS integrations (redshift_sync)
  - `smarty/` - SmartyStreets integrations
- `libs/azure/data/` - Data abstraction layer (KeyValue, Structured bindings)
- `libs/azure/` - Azure utilities (sql.py, key_vault.py, credentials.py)
- `libs/utils/` - Utility modules (geometry, OAuth, logging, h3, polygon splitting)
- `blueprints/` - Root-level blueprints
- `example/` - Jupyter notebooks with examples

## Development Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
func start

# Debug: Attach to localhost:9091 (VS Code launch.json configured)
```

**Azurite** (local storage emulator): `${command:azurite.start}`

## Deployment

Deploy to a specific Function App using:

```func azure functionapp publish <FUNCTION_APP_NAME> --build remote```

Example:
```func azure functionapp publish esquire-campaign-proposal --build remote
```

## Key Dependencies

- **Azure**: azure-functions, azure-functions-durable, azure-identity, azure-storage-*
- **Databases**: boto3, pymssql, pyodbc, psycopg2, sqlalchemy
- **Data**: pandas, pydantic, marshmallow, fsspec, s3fs, adlfs, orjson, pyarrow
- **APIs**: httpx, facebook-business, googlemaps, smartystreets-python-sdk

## Entry Point

`function_app.py` - Uses `FunctionApp` with `DecoratorApi` blueprints loaded from `deployment.py`
