from azure.durable_functions import Blueprint
import pandas as pd, os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from psycopg2.extras import execute_values

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.database_helpers import write_dataframe, upload_complete_check
bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_writeDatabase(settings: dict):
    if upload_complete_check(
        engine, 
        settings['metadata']['upload_id'], 
        schema='sales'):
        return
    engine = create_engine(os.environ['DATABIND_SQL_KEYSTONE_DEV'])
    write_all_tables(
        engine  = engine,
        tables  = settings['table_data'],
        schema  = 'sales'
        )
    
    update_upload_status(
        engine      = engine, 
        upload_id   = settings['metadata']['upload_id'],
        state       = 'Done.',
        schema      = 'sales'
        )


def update_upload_status(engine, upload_id: str, state: str, schema: str = "sales"):
    """Update the status of a data upload."""
    query = text(f"""
        UPDATE {schema}.uploads
        SET status = :state
        WHERE upload_id = :upload_id
    """)
    with engine.begin() as conn:
        conn.execute(query, {"state": state, "upload_id": upload_id})
    # print(f"[INFO] Set status = '{state}' for upload_id = {upload_id}")

def insert_addresses_on_conflict(conn, df: pd.DataFrame, schema: str = "sales", table: str = "addresses"):
    """Insert addresses with ON CONFLICT DO NOTHING using psycopg2's execute_values."""
    if df.empty:
        # print("[SKIP] No addresses to insert.")
        return

    insert_query = f"""
    INSERT INTO {schema}.{table} (id, street, city, state, zip_code, country)
    VALUES %s
    ON CONFLICT (id) DO NOTHING;
    """

    values = [
        (
            row['id'],
            row.get('address', ''),
            row.get('city', ''),
            row.get('state', ''),
            row.get('zip', ''),
            row.get('country', '')
        )
        for _, row in df.iterrows()
    ]

    raw_conn = conn.connection  # get raw psycopg2 connection from SQLAlchemy connection
    try:
        with raw_conn.cursor() as cur:
            execute_values(cur, insert_query, values)
        # print(f"[SUCCESS] Inserted new addresses using ON CONFLICT DO NOTHING.")
    except Exception as e:
        # print(f"[ERROR] Address insert failed: {e}")
        raise

def write_all_tables(engine: Engine, tables: dict, schema: str = "sales"):
    for table_name, data in tables:
        tables[table_name] = pd.DataFrame(data)
    # do this transactionally so that we either write all or none
    with engine.begin() as conn:
        insert_addresses_on_conflict(conn, tables['addresses'], schema=schema)
        for table, df in tables.items():
            if table != "addresses":
                write_dataframe(conn, df, table_name=table, schema=schema)


