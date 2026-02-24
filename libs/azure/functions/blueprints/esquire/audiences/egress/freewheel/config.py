from __future__ import annotations

import os

from libs.data import register_binding, from_bind

if not from_bind("keystone"):
    register_binding(
        "keystone",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_KEYSTONE"],
        schemas=["keystone"],
        pool_size=1000,
        max_overflow=100,
    )
