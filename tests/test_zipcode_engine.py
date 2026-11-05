"""
Tests for the Postgres-backed ZipcodeEngine.

The zipcode boundary polygons moved from a retired Synapse service to
``utils.zcta`` in the keystone (legato) Postgres database. These tests pin the
query shape and the parameter handling without needing a database.
"""

from libs.utils.esquire.zipcodes.zipcode_engine import ZipcodeEngine


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class FakeSession:
    def __init__(self, rows):
        self.rows = rows
        self.statement = None
        self.params = None
        self.closed = False

    def execute(self, statement, params):
        self.statement = statement
        self.params = params
        return FakeResult(self.rows)

    def close(self):
        self.closed = True


class FakeProvider:
    def __init__(self, rows):
        self.session = FakeSession(rows)

    def connect(self):
        return self.session


def test_returns_a_dataframe_with_the_expected_columns():
    provider = FakeProvider(
        [("60601", '{"type":"Polygon"}'), ("60602", '{"type":"Polygon"}')]
    )

    result = ZipcodeEngine(provider).load_from_radius(
        latitude=41.88, longitude=-87.63, radius=1609.0
    )

    assert list(result.columns) == ["Zipcode", "GeoJSON"]
    assert len(result) == 2
    assert list(result["Zipcode"]) == ["60601", "60602"]


def test_queries_the_zcta_table():
    provider = FakeProvider([])

    ZipcodeEngine(provider).load_from_radius(
        latitude=41.88, longitude=-87.63, radius=1609.0
    )

    sql = str(provider.session.statement)
    assert "utils.zcta" in sql
    assert "ST_Intersects" in sql
    assert "ST_AsGeoJSON" in sql


def test_normalizes_the_geometry_srid():
    """The plot column is geography with no CRS assigned, so it is relabelled."""
    provider = FakeProvider([])

    ZipcodeEngine(provider).load_from_radius(
        latitude=41.88, longitude=-87.63, radius=1609.0
    )

    sql = str(provider.session.statement)
    assert "ST_SetSRID(plot::geometry, 4326)::geography" in sql


def test_pads_the_zipcode_to_five_characters():
    """The movers view keys on a 5 character zipcode."""
    provider = FakeProvider([])

    ZipcodeEngine(provider).load_from_radius(
        latitude=41.88, longitude=-87.63, radius=1609.0
    )

    assert "lpad(id, 5, '0')" in str(provider.session.statement)


def test_buffers_the_point_in_metres():
    provider = FakeProvider([])

    ZipcodeEngine(provider).load_from_radius(
        latitude=41.88, longitude=-87.63, radius=1609.0
    )

    sql = str(provider.session.statement)
    assert "ST_MakePoint(:longitude, :latitude)" in sql
    assert "::geography" in sql


def test_passes_the_point_and_radius_through():
    provider = FakeProvider([])

    ZipcodeEngine(provider).load_from_radius(
        latitude=41.88, longitude=-87.63, radius=8046.0
    )

    params = provider.session.params
    assert params["latitude"] == 41.88
    assert params["longitude"] == -87.63
    assert params["radius"] == 8046.0


def test_closes_its_session():
    provider = FakeProvider([])

    ZipcodeEngine(provider).load_from_radius(
        latitude=41.88, longitude=-87.63, radius=1609.0
    )

    assert provider.session.closed is True
