from azure.durable_functions import Blueprint
from sqlalchemy import text
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_getOrderBuckets(settings: dict):
    staging_table = qtbl(settings['staging_table'])
    order_col     = settings['fields']['order_info']['order_num']
    target_rows   = int(settings.get("target_rows_per_chunk", 1000))

    sql = f'''
      SELECT s."{order_col}"::text AS order_key, COUNT(*)::bigint AS n
      FROM {staging_table} s
      GROUP BY s."{order_col}"
      ORDER BY n DESC
    '''
    with db() as conn:
        conn.execute(text("SET search_path TO sales"))
        rows = conn.execute(text(sql)).mappings().all()

    buckets, cur, curn = [], [], 0
    for r in rows:
        n = int(r["n"])
        if cur and (curn + n) > target_rows:
            buckets.append(cur); cur, curn = [], 0
        cur.append(r["order_key"]); curn += n
    if cur:
        buckets.append(cur)

    return buckets
