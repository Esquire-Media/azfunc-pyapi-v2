from __future__ import annotations

import logging
import os
from typing import Dict

import pandas as pd
from azure.durable_functions import Blueprint
from libs.data import from_bind
from libs.utils.azure_storage import get_blob_sas, init_blob_client
from sqlalchemy.orm import Session

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_azurePostgres_resultToBlob(ingress: Dict) -> str:
    """
    Executes a paged query and writes results to a deterministically named Azure Blob.

    ingress:
    {
        "source": {
            "bind": "BIND_HANDLE",
            "query": "SELECT * FROM table"
        },
        "destination": {
            "conn_str": "YOUR_AZURE_CONNECTION_STRING_ENV_VARIABLE",
            "container_name": "your-azure-blob-container",
            "blob_prefix": "combined-blob-name",
            "format": "CSV"  // normalized upper-case expected
        },
        "limit": 100,
        "offset": 0,
        "blob_name": "prefix/df-<instance>-<qhash>-o000000000000-l100.csv"  // required, deterministic
    }

    Idempotency strategy:
    - The orchestrator passes a deterministic blob_name.
    - If the blob already exists, we DO NOT rewrite it; we simply return its SAS.
      (Safe under retries; zero or many retries yield the same outcome.)
    """
    # Validate ingress shape
    if not ingress:
        raise ValueError("ingress is required.")

    source = ingress.get("source") or {}
    dest = ingress.get("destination") or {}

    for field in ("bind", "query"):
        if field not in source:
            raise ValueError(f"source.{field} is required.")

    for field in ("conn_str", "container_name"):
        if field not in dest:
            raise ValueError(f"destination.{field} is required.")

    blob_name: str = ingress.get("blob_name")
    if not blob_name or not isinstance(blob_name, str):
        raise ValueError("A deterministic 'blob_name' must be provided by the orchestrator.")

    limit = int(ingress.get("limit", 100))
    offset = int(ingress.get("offset", 0))
    if limit <= 0 or offset < 0:
        raise ValueError("'limit' must be > 0 and 'offset' must be >= 0.")

    fmt = (dest.get("format") or "CSV").upper()

    # Initialize blob client with deterministic name
    conn_env_key = dest["conn_str"]
    if conn_env_key not in os.environ:
        raise EnvironmentError(
            f"Environment variable '{conn_env_key}' not found for destination.conn_str."
        )

    container = dest["container_name"]

    blob_client = init_blob_client(
        conn_str=os.environ[conn_env_key],
        container_name=container,
        blob_name=blob_name,
    )

    # If blob already exists, short-circuit (idempotent retry).
    try:
        if blob_client.exists():
            logging.info(f"[activity] Blob already exists, returning existing SAS: {blob_name}")
            return get_blob_sas(blob_client)
    except Exception:
        # If exists() is not supported/throws for some reason, we fall through and use overwrite=True
        # on upload as a safe idempotent write.
        pass

    logging.info(f"[activity] Executing paged query (limit={limit}, offset={offset}) and writing to: {blob_name}")

    # Fetch page of results
    session: Session = from_bind(source["bind"]).connect()
    try:
        # Use pandas to execute the paginated query. Parameterize LIMIT/OFFSET via format() carefully;
        # if these originate from untrusted input, validate numeric types (we do above).
        paged_sql = f"{source['query']} LIMIT {limit} OFFSET {offset}"
        # Note: depending on your from_bind() return type, you may need .connection() or .engine.raw_connection()
        df = pd.read_sql_query(sql=paged_sql, con=session.connection())
    finally:
        try:
            session.close()
        except Exception:
            pass

    # Upload in the requested format. Use overwrite=True so retries are idempotent even if exists() check failed.
    match fmt:
        case "CSV":
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            blob_client.upload_blob(csv_bytes, overwrite=True)
        case _:
            raise ValueError(f"Format not supported: {fmt}")

    return get_blob_sas(blob_client)
