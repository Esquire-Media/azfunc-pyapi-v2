import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

# Zipcode boundary polygons live in the keystone (legato) Postgres database.
# The geometry column holds geography with no CRS assigned (SRID 0), so the
# query below relabels it to 4326 before doing any spatial work.
ZCTA_TABLE = "utils.zcta"

ZCTA_COLUMNS = ["Zipcode", "GeoJSON"]

_RADIUS_SQL = """
    SELECT lpad(id, 5, '0') AS "Zipcode",
           ST_AsGeoJSON(plot) AS "GeoJSON"
    FROM {table}
    WHERE ST_Intersects(
            ST_SetSRID(plot::geometry, 4326)::geography,
            ST_Buffer(
                ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)::geography,
                :radius
            )
          )
""".format(table=ZCTA_TABLE)


class ZipcodeEngine:
    def __init__(self, provider):
        self.provider = provider

    def load_from_radius(self, latitude: float, longitude: float, radius: float):
        """
        Load zipcode information for zipcodes within a given point and radius.

        Params:
        latitude    : latitude of the search area centerpoint
        longitude   : longitude of the search area centerpoint
        radius      : radius to search around the centerpoint, in metres

        Returns:
        results     : Pandas DataFrame with a Zipcode column and a GeoJSON
                      column holding the boundary of each matching zipcode.

        Notes:
        The spatial index lives in Postgres, so only the matching boundaries
        are ever held in the Function App. The radius is a spheroidal buffer
        in metres, matching the geography semantics the query previously used.
        """
        session: Session = self.provider.connect()

        try:
            rows = session.execute(
                text(_RADIUS_SQL),
                {
                    "latitude": latitude,
                    "longitude": longitude,
                    "radius": radius,
                },
            ).all()
        finally:
            session.close()

        return pd.DataFrame(rows, columns=ZCTA_COLUMNS)
