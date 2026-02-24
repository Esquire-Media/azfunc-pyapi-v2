from __future__ import annotations

from typing import Dict

from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_azurePostgres_getRecordCount(ingress: Dict) -> int:
    """
    Returns the total row count for the provided SQL query.

    ingress:
    {
        "bind": "BIND_HANDLE",
        "query": "SELECT * FROM table"
    }

    Idempotent: yes (pure read).
    """
    if not ingress or "bind" not in ingress or "query" not in ingress:
        raise ValueError("ingress must include 'bind' and 'query'.")

    # Establish a session/connection to execute the COUNT(*) over a derived table.
    # Using parameter binding only for the LIMIT/OFFSET is typical; here we interpolate query
    # into a derived table safely since it's treated as text and not parameterized by user input.
    # If the source of 'query' is end-user input, consider validating/whitelisting further.
    session: Session = from_bind(ingress["bind"]).connect()

    try:
        stmt = text(f"SELECT COUNT(*) AS total FROM ({ingress['query']}) AS total")
        result = session.execute(stmt).one_or_none()
        if result is None:
            return 0
        return int(result[0])
    finally:
        # Be explicit about cleanup if the returned object requires it.
        try:
            session.close()
        except Exception:
            pass
