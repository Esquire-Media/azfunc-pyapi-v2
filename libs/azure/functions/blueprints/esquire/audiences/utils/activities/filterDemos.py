# File: /libs/azure/functions/blueprints/esquire/audiences/utils/activities/filterDemos.py

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.activities.fetchAudience import _canonicalize_jsonlogic
from libs.utils.azure_storage import init_blob_client
import os
import csv
import json
from io import StringIO
from typing import Iterator, Set
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import unquote

bp = Blueprint()


def evaluate_jsonlogic(logic: dict, row: dict) -> bool:
    """
    Directly evaluate JsonLogic against a row dictionary.

    Handles:
    - {"var": "fieldname"} -> returns row.get(fieldname)
    - {"in": [value, list]} -> returns value in list
    - {"==": [left, right]} -> returns left == right
    - {"!=": [left, right]} -> returns left != right
    - {"and": [conditions]} -> returns all(conditions)
    - {"or": [conditions]} -> returns any(conditions)
    - {"not": [condition]} -> returns not condition
    """
    if isinstance(logic, dict):
        # Handle "var" - fetch value from row
        if "var" in logic:
            field_name = logic["var"]
            return row.get(field_name)

        # Handle "in" - value in list
        if "in" in logic:
            left = evaluate_jsonlogic(logic["in"][0], row)
            right = evaluate_jsonlogic(logic["in"][1], row)
            if right is None:
                return False
            return left in right

        # Handle "==" - equality
        if "==" in logic:
            left = evaluate_jsonlogic(logic["=="][0], row)
            right = evaluate_jsonlogic(logic["=="][1], row)
            return left == right

        # Handle "!=" - inequality
        if "!=" in logic:
            left = evaluate_jsonlogic(logic["!="][0], row)
            right = evaluate_jsonlogic(logic["!="][1], row)
            return left != right

        # Handle "and" - all conditions must be true
        if "and" in logic:
            return all(evaluate_jsonlogic(cond, row) for cond in logic["and"])

        # Handle "or" - any condition must be true
        if "or" in logic:
            return any(evaluate_jsonlogic(cond, row) for cond in logic["or"])

        # Handle "not" - negation
        if "not" in logic:
            return not evaluate_jsonlogic(logic["not"][0], row)

        raise ValueError(f"Unknown JsonLogic operator: {logic}")

    if isinstance(logic, list):
        return [evaluate_jsonlogic(item, row) for item in logic]

    # Scalar values pass through
    return logic


def extract_columns_from_jsonlogic(logic: dict) -> Set[str]:
    """
    Extract column names referenced in a JsonLogic expression.
    """
    columns: Set[str] = set()

    def extract(node):
        if isinstance(node, dict):
            if "var" in node:
                columns.add(node["var"])
            else:
                for value in node.values():
                    extract(value)
        elif isinstance(node, list):
            for item in node:
                extract(item)

    extract(logic)
    return columns


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiences_filterDemographics(ingress: dict) -> str:
    """
    Durable-safe streaming demographics filter.
    """

    import uuid
    from azure.storage.blob import BlobClient

    source_url = ingress["source_url"]
    demo_filter = ingress["demographicFilter"]
    destination = ingress["destination"]

    # 1. Compile filter - use direct JsonLogic evaluation
    # Rewrite demographic fields for backward compatibility
    rewritten_filter = rewrite_demographic_fields(demo_filter)
    canonical_filter = _canonicalize_jsonlogic(rewritten_filter)

    # Extract required columns from JsonLogic
    required_columns = list(extract_columns_from_jsonlogic(canonical_filter))

    # Always include device id column
    required_columns.append("hashed device id")

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

    row_iter = build_indexed_reader(
        downloader=downloader,
        required_columns=required_columns,
    )

    def output_generator():
        buf = StringIO()
        writer = csv.writer(buf)

        writer.writerow(["deviceid"])
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)

        for row in row_iter:
            if evaluate_jsonlogic(canonical_filter, row):
                writer.writerow([row["hashed device id"]])
                yield buf.getvalue()
                buf.seek(0)
                buf.truncate(0)

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


def build_indexed_reader(
    downloader,
    required_columns: list,
    encoding: str = "utf-8",
) -> Iterator[dict]:

    import codecs

    decoder = codecs.getincrementaldecoder(encoding)()
    buffer = ""

    def line_iter():
        nonlocal buffer
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

    reader = csv.reader(line_iter())

    header = next(reader)

    # Map required column -> index
    index_map = {
        col: header.index(col)
        for col in required_columns
        if col in header
    }

    for row in reader:
        yield {
            col: row[idx] if idx < len(row) else None
            for col, idx in index_map.items()
        }


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

def rewrite_demographic_fields(json_logic: dict | str) -> dict:
    """
    Rewrite incoming demographic JsonLogic into storage-aligned JsonLogic.
    - Expands QueryBuilder-style select fields into one-hot boolean columns.
    - Renames direct-storage fields (e.g., homeOwner -> home_owner).
    Handles QueryBuilder-style `all` operator correctly.
    """
    import ast
    if (type(json_logic) == "str"):
        json_logic = json.loads(json_logic)
    json_logic = ast.literal_eval(json_logic)

    # Fields where the UI provides a *choice*, but storage is one-hot boolean columns.
    # Example: dwellingType == "dwelling_type single family"
    # becomes:    "dwelling_type single family" == 1
    BOOLEAN_SELECT_FIELDS = {
        "educationLevel",
        "estimatedAge",
        "householdIncome",
        "networth",
        "dwellingType",
        "gender",
        "creditCardCreditRating",
    }

    # Mapping from credit card credit rating letters to storage column names
    CREDIT_RATING_MAP = {
        "A": "credit score 800+",
        "B": "credit score 750-799",
        "C": "credit score 700-749",
        "D": "credit score 650-699",
        "E": "credit score 600-649",
        "F": "credit score 550-599",
        "G": "credit score 500-549",
        "H": "credit score <499",
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

    # Direct column renames (storage column exists as a normal field)
    DIRECT_RENAME = {
        "homeOwner": "home_owner",
        "hasCredit": "has_credit",
    }

    NUMERIC_RENAME = {
        "presenceOfChildren": "presence_of_children",
        "householdSize": "household_size",
    }

    CATEGORICAL_RENAME = {
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
                    conditions = [{"==": [{"var": v}, "1"]} for v in values]

                    if len(conditions) == 1:
                        return process(conditions[0])

                    return {"and": [process(c) for c in conditions]}

                return {"all": [process(field_block), process(in_block)]}

            # --- HANDLE "in" ---
            if "in" in node:
                left, right = node["in"]

                if isinstance(left, dict) and "var" in left:
                    field = left["var"]

                    # Boolean select: expand to one-hot boolean columns
                    if field in BOOLEAN_SELECT_FIELDS and isinstance(right, list):
                        # Special handling for creditCardCreditRating: map letter ratings to column names
                        if field == "creditCardCreditRating":
                            right = [CREDIT_RATING_MAP.get(v, v) for v in right]
                        conditions = [{"==": [{"var": v}, "1"]} for v in right]
                        if len(conditions) == 1:
                            return process(conditions[0])
                        return {"or": [process(c) for c in conditions]}

                    # Categorical rename (rare path)
                    if field in CATEGORICAL_RENAME:
                        return {"in": [{"var": CATEGORICAL_RENAME[field]}, right]}

                return {"in": [process(left), process(right)]}

            # --- HANDLE "==" (THIS is what was missing for dwellingType) ---
            if "==" in node:
                left, right = node["=="]

                # Pattern: {"==":[{"var":"dwellingType"},"dwelling_type single family"]}
                if isinstance(left, dict) and left.get("var") in BOOLEAN_SELECT_FIELDS and isinstance(right, str):
                    # Convert to one-hot boolean column check (use string "1" to match CSV format)
                    return {"==": [{"var": right}, "1"]}

                # Pattern: {"==":[{"var":"hasCredit"}, 1]} -> convert 1 to "1" for CSV compatibility
                # DIRECT_RENAME fields store boolean values as "0"/"1" strings in CSV
                if isinstance(left, dict) and left.get("var") in DIRECT_RENAME:
                    if isinstance(right, int):
                        right = str(right)

                return {"==": [process(left), process(right)]}

            # --- VAR RENAME ---
            if "var" in node:
                var_name = node["var"]

                if var_name in DIRECT_RENAME:
                    return {"var": DIRECT_RENAME[var_name]}

                if var_name in NUMERIC_RENAME:
                    return {"var": NUMERIC_RENAME[var_name]}

                if var_name in CATEGORICAL_RENAME:
                    return {"var": CATEGORICAL_RENAME[var_name]}

                return node

            return {k: process(v) for k, v in node.items()}

        if isinstance(node, list):
            return [process(n) for n in node]

        return node

    return process(json_logic)
