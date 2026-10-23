
import os
from libs.data import from_bind, register_binding

if from_bind("sales_ingestor_keystone") is None:
    register_binding(
        "sales_ingestor_keystone",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_KEYSTONE"],
        schemas={
            "sales": None,
            "attribution": [
                "match_relationship",
                "match_geoframe_relationship",
            ],
        },
        pool_size=10,
        max_overflow=10,
    )
