"""
Tests for the Postgres-backed MoverEngine.

The movers dataset moved from Synapse ``dbo.movers`` to the keystone
``utils.attom`` view. These tests pin the query shape and the parameter
handling without needing a database.
"""

from datetime import date

from libs.utils.esquire.movers.mover_engine import MoverEngine


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


def test_load_from_zipcodes_returns_counts_per_zipcode():
    provider = FakeProvider([("60601", 12), ("60602", 3)])

    result = MoverEngine(provider).load_from_zipcodes(
        start_date=date(2026, 8, 1),
        end_date=date(2026, 10, 30),
        zipcodes=["60601", "60602"],
        counts=True,
    )

    assert result == [("60601", 12), ("60602", 3)]


def test_load_from_zipcodes_queries_the_attom_view():
    provider = FakeProvider([])

    MoverEngine(provider).load_from_zipcodes(
        start_date=date(2026, 8, 1),
        end_date=date(2026, 10, 30),
        zipcodes=["60601"],
        counts=True,
    )

    sql = str(provider.session.statement)
    assert "utils.attom" in sql
    assert "publication_date" in sql
    assert '"zipCode"' in sql
    assert "count(address)" in sql


def test_load_from_zipcodes_passes_the_date_window():
    provider = FakeProvider([])

    MoverEngine(provider).load_from_zipcodes(
        start_date=date(2026, 8, 1),
        end_date=date(2026, 10, 30),
        zipcodes=["60601"],
        counts=True,
    )

    params = provider.session.params
    assert params["start_date"] == date(2026, 8, 1)
    assert params["end_date"] == date(2026, 10, 30)


def test_load_from_zipcodes_normalizes_zipcodes_to_strings():
    """The zipcode column is text, and psycopg2 cannot adapt numpy scalars."""
    provider = FakeProvider([])

    MoverEngine(provider).load_from_zipcodes(
        start_date=date(2026, 8, 1),
        end_date=date(2026, 10, 30),
        zipcodes=[60601, 60602],
        counts=True,
    )

    assert provider.session.params["zipcodes"] == ["60601", "60602"]


def test_load_from_zipcodes_closes_its_session():
    provider = FakeProvider([])

    MoverEngine(provider).load_from_zipcodes(
        start_date=date(2026, 8, 1),
        end_date=date(2026, 10, 30),
        zipcodes=["60601"],
        counts=True,
    )

    assert provider.session.closed is True


def test_load_from_zipcodes_detail_returns_a_dataframe():
    provider = FakeProvider(
        [("123 Main St", "Chicago", "IL", "60601", "1234", 41.1, -87.6)]
    )

    result = MoverEngine(provider).load_from_zipcodes(
        start_date=date(2026, 8, 1),
        end_date=date(2026, 10, 30),
        zipcodes=["60601"],
        counts=False,
    )

    assert list(result.columns) == [
        "address",
        "city",
        "state",
        "zipCode",
        "plus4Code",
        "latitude",
        "longitude",
    ]
    assert len(result) == 1


def test_zipcode_list_is_expanded_into_an_in_clause():
    """An IN clause needs an expanding bindparam, not a single scalar."""
    provider = FakeProvider([])

    MoverEngine(provider).load_from_zipcodes(
        start_date=date(2026, 8, 1),
        end_date=date(2026, 10, 30),
        zipcodes=["60601", "60602"],
        counts=True,
    )

    assert provider.session.statement._bindparams["zipcodes"].expanding is True
