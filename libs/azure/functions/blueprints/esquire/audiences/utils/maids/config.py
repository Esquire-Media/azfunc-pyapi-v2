# File: libs/azure/functions/blueprints/esquire/audiences/maids/config.py

from libs.data import register_binding, from_bind
import os

maids_name = "maids.csv"
unvalidated_addresses_name = "addresses.csv"
validated_addresses_name = "validated_addresses.csv"
geoframes_name = "geoframes.json"

if not from_bind("audiences"):
    register_binding(
        "audiences",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_AUDIENCES"],
        schemas=["dbo"],
        pool_size=1000,
        max_overflow=100
    )

# if not from_bind("salesforce"):
#     register_binding(
#         "salesforce",
#         "Structured",
#         "sql",
#         url=os.environ["DATABIND_SQL_SALESFORCE"],
#         schemas=["dbo"],
#         pool_size=1000,
#         max_overflow=100
#     )