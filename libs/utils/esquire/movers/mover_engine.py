import pandas as pd
from datetime import date
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

# The movers dataset is read from the keystone (legato) Postgres database. The
# utils.attom view projects attom.taxassessor_normalized into the shape the
# audience builder expects and carries publication_date, which is the move
# date used for the date window.
MOVERS_VIEW = "utils.attom"

_COUNT_SQL = """
    SELECT "zipCode", count(address) AS "count"
    FROM {view}
    WHERE publication_date BETWEEN :start_date AND :end_date
      AND "zipCode" IN :zipcodes
    GROUP BY "zipCode"
""".format(view=MOVERS_VIEW)

_DETAIL_SQL = """
    SELECT address, city, state, "zipCode", "plus4Code", latitude, longitude
    FROM {view}
    WHERE publication_date BETWEEN :start_date AND :end_date
      AND "zipCode" IN :zipcodes
""".format(view=MOVERS_VIEW)

_DETAIL_COLUMNS = [
    "address",
    "city",
    "state",
    "zipCode",
    "plus4Code",
    "latitude",
    "longitude",
]


class MoverEngine:
    def __init__(self, provider):
        self.provider = provider

    def load_from_zipcodes(
        self, start_date: date, end_date: date, zipcodes: list, counts: bool = False
    ):
        """
        Given a list of zipcodes, return all mover addresses registered to those zipcodes.
        Uses string matching, and does not invoke any actual zipcode polygons for geography-based search.

        Params:
        start_date  : Pull movers on or after this date.
        end_date    : Pull movers on or before this date.
        zipcodes    : List of 5-digit zipcodes as strings or ints.
        counts      : If true, will only return mover counts instead of the address list. Results in significantly faster queries.

        Returns:
        results     : List of (zipcode, count) tuples when counts is True, otherwise a
                      Pandas DataFrame of mover addresses within the given zipcode list.

        Notes:
        The aggregation runs in Postgres, so the client only ever holds one row
        per zipcode regardless of how many movers match.
        """
        session: Session = self.provider.connect()

        params = {
            "start_date": start_date,
            "end_date": end_date,
            # psycopg2 cannot adapt numpy scalars, and the zipcode column is
            # text, so normalize to plain strings.
            "zipcodes": [str(zipcode) for zipcode in zipcodes],
        }

        try:
            if counts:
                statement = text(_COUNT_SQL).bindparams(
                    bindparam("zipcodes", expanding=True)
                )
                rows = session.execute(statement, params).all()
                return [(row[0], row[1]) for row in rows]

            statement = text(_DETAIL_SQL).bindparams(
                bindparam("zipcodes", expanding=True)
            )
            rows = session.execute(statement, params).all()
            return pd.DataFrame(rows, columns=_DETAIL_COLUMNS)
        finally:
            session.close()
