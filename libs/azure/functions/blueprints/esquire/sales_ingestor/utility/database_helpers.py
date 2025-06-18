import pandas as pd
import numpy as np
import orjson as json
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSON

def write_dataframe(conn, df: pd.DataFrame, table_name: str, schema: str = "sales", if_exists: str = "append"):
    """Write a DataFrame to a PostgreSQL table using an existing transaction."""
    if df.empty:
        # print(f"[SKIP] Table '{schema}.{table_name}' is empty.")
        return

    # make sure things will be treated correctly as NULLs
    df = clean_nans(df)

    try:
        df.to_sql(
            name=table_name,
            con=conn,  # use passed connection
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi"
        )
        # print(f"[SUCCESS] Wrote {len(df)} rows to '{schema}.{table_name}'")
    except SQLAlchemyError as e:
        # print(f"[ERROR] Failed to write to '{schema}.{table_name}': {e}")
        raise

def insert_upload_record(engine, upload_id: str, tenant_id: str, upload_timestamp: str, status: str = "Pending", source: str = "Uploader Backend testing", metadata: dict = None, schema: str = "sales"):
    """Insert a new row into sales.uploads."""
    query = text(f"""
        INSERT INTO {schema}.uploads (
            upload_id,
            tenant_id,
            timestamp,
            status,
            source, 
            metadata
        )
        VALUES (
            :upload_id,
            :tenant_id,
            :timestamp,
            :status,
            :source,
            :metadata
        )
        ON CONFLICT DO NOTHING
    """).bindparams(
        bindparam("metadata", type_=JSON)
    )
    
    with engine.begin() as conn:
        conn.execute(query, {
            "upload_id": upload_id,
            "tenant_id": tenant_id,
            "timestamp": upload_timestamp,
            "status": status,
            "source": source,
            "metadata": metadata
        })

    # print(f"[INSERT] Created uploads row for upload_id: {upload_id}")

def clean_nans(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: np.nan if isinstance(x, str) and x.strip().lower() in {"none", "nan"} else x
            )

            # Replace Python None with np.nan too
            df[col] = df[col].apply(lambda x: np.nan if x is None else x)

    return df

def upload_complete_check(engine, upload_id: str, schema: str) -> bool:
    query = text(f"""
        SELECT 1 FROM {schema}.uploads
        WHERE upload_id = :upload_id AND status = 'Done.'
    """)
    with engine.begin() as conn:
        return conn.execute(query, {"upload_id": upload_id}).first() is not None

