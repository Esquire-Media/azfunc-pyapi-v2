
from azure.durable_functions import Blueprint
import logging
from sqlalchemy import create_engine, text
import os

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesNeighbors_findTenantOwnedLocations(ingress: dict) -> list[str]:
    """
    Given a tenant_id, finds the centroids and returns their lat lons in this form:

    [
        {'latitude': 33.7295, 'longitude': -117.8742},
        {'latitude': 34.0194, 'longitude': -118.4912},
        ...
    ]
    """
    tenant_id = ingress["tenant_id"] 
    engine = create_engine(os.environ["DATABIND_SQL_KEYSTONE"]) 

    sql = text("""
        WITH base AS (
          SELECT 
            mo."B" AS frame_id,
            tg.polygon
          FROM (
            SELECT 
              "A" AS "market",
              "B" AS "tenant_id"
            FROM keystone."_Market_tenants"
            WHERE "B" = :tenant_id
          ) mt
          JOIN keystone."Market" m ON mt.market = m.id
          JOIN keystone."_Market_owned" mo ON mt.market = mo."A"
          JOIN keystone."TargetingGeoFrame" tg ON tg.id = mo."B"
        ),
        features AS (
          SELECT 
            frame_id,
            jsonb_array_elements(polygon->'features') AS feature
          FROM base
        ),
        rings AS (
          SELECT
            frame_id,
            jsonb_array_elements(feature->'geometry'->'coordinates') AS ring
          FROM features
        ),
        points AS (
          SELECT 
            frame_id,
            jsonb_array_elements(ring) AS point_array
          FROM rings
        ),
        coords AS (
          SELECT
            frame_id,
            (point_array->>0)::double precision AS longitude,
            (point_array->>1)::double precision AS latitude
          FROM points
        ),
        centroids AS (
          SELECT 
            frame_id,
            AVG(latitude) AS latitude,
            AVG(longitude) AS longitude
          FROM coords
          GROUP BY frame_id
        )
        SELECT latitude, longitude FROM centroids
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)

    with engine.connect() as conn:
        result = conn.execute(sql, {"tenant_id": tenant_id})
        return [dict(row) for row in result.mappings().all()]