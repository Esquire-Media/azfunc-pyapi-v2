# File: /libs/azure/functions/blueprints/esquire/audiences/utils/activities/filterDemos.py

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from libs.azure.functions.blueprints.esquire.audiences.builder.activities.fetchAudience import _canonicalize_jsonlogic
from libs.utils.azure_storage import init_blob_client
import os
import csv
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import unquote

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiences_filterDemographics(ingress: dict) -> str:
    """
    Durable-safe streaming demographics filter.
    """

    import uuid
    import logging
    from azure.storage.blob import BlobClient

    source_url = ingress["source_url"]
    demo_filter = ingress["demographicFilter"]
    destination = ingress["destination"]

    # 1. Compile filter
    # use sql as in the rest of audience automation to ensure it's consistent
    # kind of an intermediate, narrower level
    where_sql = jsonlogic_to_sql(
        _canonicalize_jsonlogic(
            rewrite_demographic_fields(demo_filter)
            )
    )
    # then turn it into pythony dict-handling goodness
    predicate = compile_sql_where_predicate(where_sql)

    # 2. Build destination path
    blob_name = f"{destination['blob_prefix']}/{uuid.uuid4().hex}.csv"

    # 3. Open blob streams
    source_blob = BlobClient.from_blob_url(source_url)
    dest_blob = init_blob_client(
        conn_str=os.environ[destination["conn_str"]],
        container_name=destination["container_name"],
        blob_name=blob_name,
    )


    downloader = source_blob.download_blob()

    reader = csv.DictReader(
        iter_csv_lines_from_blob(downloader)
    )

    def output_generator():
        import io, csv

        buf = io.StringIO()
        writer = csv.writer(buf)

        writer.writerow(["deviceid"])
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)

        matched = 0
        for row in reader:
            if predicate(row):
                writer.writerow([row['hashed device id']])
                yield buf.getvalue()
                buf.seek(0); buf.truncate(0)

    dest_blob.upload_blob(
        data=output_generator(),
        overwrite=True,
    )

    sas_token = generate_blob_sas(
        account_name=dest_blob.account_name,
        container_name=dest_blob.container_name,
        blob_name=dest_blob.blob_name,
        account_key=dest_blob.credential.account_key,  # type: ignore[attr-defined]
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

    return f"{unquote(dest_blob.url)}?{sas_token}"

def compile_sql_where_predicate(where_sql: str):
    import re

    expr = where_sql

    # Logical operators
    expr = re.sub(r"\bAND\b", "and", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bOR\b", "or", expr, flags=re.IGNORECASE)

    expr = expr.replace("!=", "!=")
    expr = re.sub(r"(?<![<>=!])=(?!=)", "==", expr)

    # FIRST: replace quoted identifiers
    expr = re.sub(
        r'"([^"]+)"',
        lambda m: f'_val(row.get("{m.group(1)}"))',
        expr,
    )

    # THEN: convert IN (...) to Python in (...)
    expr = re.sub(
        r'_val\(row\.get\("([^"]+)"\)\)\s+IN\s+\(([^)]+)\)',
        lambda m: f'_val(row.get("{m.group(1)}")) in ({m.group(2)})',
        expr,
        flags=re.IGNORECASE,
    )

    print(expr)

    code = compile(expr, "<demographics-filter>", "eval")

    def _val(v):
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip()

            # Try numeric coercion
            try:
                return int(float(v))
            except ValueError:
                try:
                    return float(v)
                except ValueError:
                    return v
        return v

    def predicate(row: dict) -> bool:
        try:
            return bool(eval(code, {"row": row, "_val": _val}))
        except Exception:
            return False

    return predicate


import codecs
from typing import Iterator

def iter_csv_lines_from_blob(downloader, encoding: str = "utf-8") -> Iterator[str]:
    decoder = codecs.getincrementaldecoder(encoding)()
    buffer = ""

    for chunk in downloader.chunks():
        buffer += decoder.decode(chunk)
        while True:
            newline = buffer.find("\n")
            if newline < 0:
                break
            line = buffer[: newline + 1]
            buffer = buffer[newline + 1 :]
            yield line

    buffer += decoder.decode(b"", final=True)
    if buffer:
        yield buffer

def rewrite_demographic_fields(json_logic: dict) -> dict:
    """
    Rewrite incoming demographic JsonLogic into storage-aligned JsonLogic.
    Handles QueryBuilder-style `all` operator correctly.
    """
    import ast

    json_logic = ast.literal_eval(json_logic)

    BOOLEAN_SELECT_FIELDS = {
        "educationLevel",
        "estimatedAge",
        "householdIncome",
        "networth",
        "dwellingType",
        "gender",
    }

    BOOLEAN_MULTI_FIELDS = {
        "interest",
        "spectator",
        "donor",
        "reading",
        "buyer",
        "entertain",
        "presenceOf",
        "creditCard",
    }

    NUMERIC_RENAME = {
        "presenceOfChildren": "presence_of_children",
        "householdSize": "household_size",
    }

    CATEGORICAL_RENAME = {
        "creditCardCreditRating": "credit_card_credit_rating",
    }

    def process(node):

        if isinstance(node, dict):

            # --- HANDLE "all" FIRST ---
            if "all" in node:
                field_block, in_block = node["all"]

                if (
                    isinstance(field_block, dict)
                    and field_block.get("var") in BOOLEAN_MULTI_FIELDS
                    and isinstance(in_block, dict)
                    and "in" in in_block
                ):
                    values = in_block["in"][1]

                    conditions = [
                        {"==": [{"var": v}, 1]}
                        for v in values
                    ]

                    if len(conditions) == 1:
                        return process(conditions[0])

                    return {"and": [process(c) for c in conditions]}

                return {"all": [process(field_block), process(in_block)]}

            # --- HANDLE "in" ---
            if "in" in node:
                left, right = node["in"]

                if isinstance(left, dict) and "var" in left:
                    field = left["var"]

                    # Boolean select
                    if field in BOOLEAN_SELECT_FIELDS:
                        conditions = [
                            {"==": [{"var": v}, 1]}
                            for v in right
                        ]
                        if len(conditions) == 1:
                            return process(conditions[0])
                        return {"or": [process(c) for c in conditions]}

                    # Categorical rename
                    if field in CATEGORICAL_RENAME:
                        return {
                            "in": [
                                {"var": CATEGORICAL_RENAME[field]},
                                right
                            ]
                        }

                return {"in": [process(left), process(right)]}

            # --- VAR RENAME ---
            if "var" in node:
                var_name = node["var"]

                if var_name in NUMERIC_RENAME:
                    return {"var": NUMERIC_RENAME[var_name]}

                if var_name in CATEGORICAL_RENAME:
                    return {"var": CATEGORICAL_RENAME[var_name]}

                return node

            # Generic recursion
            return {k: process(v) for k, v in node.items()}

        if isinstance(node, list):
            return [process(n) for n in node]

        return node

    return process(json_logic)
