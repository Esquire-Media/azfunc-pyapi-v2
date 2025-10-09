from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from sqlalchemy import text
import logging, math

bp = Blueprint()
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_assignChunks(settings: dict):
    """
    Assigns chunk_id values to each row in the staging table, ensuring:
      • each distinct order_num is in exactly one chunk
      • chunks are roughly balanced by total rows
      • idempotent: can safely re-run; same upload_id yields same chunking

    Returns:
      list[int] — chunk_id values for subsequent fan-out
    """
    staging_table = qtbl(settings["staging_table"])
    order_col     = settings["fields"]["order_info"]["order_num"]
    target_rows   = int(settings.get("target_rows_per_chunk", 50_000))

    logger.info(f"[LOG] Assigning chunk_id for {staging_table}")

    # 1️⃣ add chunk_id column if missing
    ddl = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'sales'
              AND table_name = '{settings["staging_table"]}'
              AND column_name = 'chunk_id'
        ) THEN
            ALTER TABLE {staging_table} ADD COLUMN chunk_id integer;
        END IF;
    END $$;
    """

    with db() as conn:
        conn.execute(text(ddl))

        # 2️⃣ compute number of rows per order to balance chunks
        sql_order_counts = f"""
        SELECT s."{order_col}"::text AS order_key, COUNT(*)::bigint AS n
        FROM {staging_table} s
        GROUP BY s."{order_col}"
        ORDER BY n DESC;
        """
        orders = conn.execute(text(sql_order_counts)).mappings().all()

        # 3️⃣ assign sequential chunk ids (stable order)
        chunks, current_rows, chunk_id = {}, 0, 1
        for r in orders:
            n = int(r["n"])
            if current_rows + n > target_rows and current_rows > 0:
                chunk_id += 1
                current_rows = 0
            chunks[r["order_key"]] = chunk_id
            current_rows += n

        total_chunks = chunk_id
        logger.info(f"[LOG] Planned {total_chunks} chunks for {staging_table}")

        # 4️⃣ write assignments deterministically (idempotent UPSERT)
        #     Rows with same order_num always get the same chunk_id
        temp_table = f"tmp_chunk_assign_{settings['metadata']['upload_id'].replace('-', '')}"

        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
        conn.execute(text(f"CREATE TEMP TABLE {temp_table} (order_key text, chunk_id int);"))

        for key, cid in chunks.items():
            conn.execute(
                text(f"INSERT INTO {temp_table} VALUES (:k, :cid)"),
                {"k": key, "cid": cid}
            )

        index_sql = text(f"""        
            CREATE INDEX IF NOT EXISTS idx_chunk_id
            ON {staging_table}(chunk_id);
            """)
        conn.execute(index_sql)

        update_sql = f"""
        UPDATE {staging_table} s
        SET chunk_id = t.chunk_id
        FROM {temp_table} t
        WHERE s."{order_col}"::text = t.order_key
          AND (s.chunk_id IS DISTINCT FROM t.chunk_id);
        """
        conn.execute(text(update_sql))
        conn.commit()

        logger.info(f"[LOG] Assigned chunk_ids 1..{total_chunks} for {staging_table}")

        # 5️⃣ return list of chunk ids for fan-out
        chunk_ids = list(range(1, total_chunks + 1))
        return chunk_ids
