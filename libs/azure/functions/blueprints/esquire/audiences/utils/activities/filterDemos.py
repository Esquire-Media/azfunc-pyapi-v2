# File: /libs/azure/functions/blueprints/esquire/audiences/utils/activities/filterDemos.py

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.activities.fetchAudience import _canonicalize_jsonlogic
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from libs.utils.azure_storage import init_blob_client
import os
import csv
import json
from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas, DelimitedTextDialect
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import unquote

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiences_filterDemographics(ingress: dict) -> str:
    """
    Uses Azure Blob Query for server-side SQL pushdown filtering.
    Only matching rows are returned from Azure Storage - no local filtering.
    """
    import uuid
    from io import StringIO

    source_url = ingress["source_url"]
    demo_filter = ingress["demographicFilter"]
    destination = ingress["destination"]

    # 1. Transform filter: UI JsonLogic -> storage-aligned JsonLogic
    rewritten_filter = rewrite_demographic_fields(demo_filter)
    canonical_filter = _canonicalize_jsonlogic(rewritten_filter)

    # 2. Convert to SQL WHERE clause for server-side filtering
    where_clause = jsonlogic_to_sql(canonical_filter)

    # 3. Build destination path
    blob_name = f"{destination['blob_prefix']}/{uuid.uuid4().hex}.csv"

    # 4. Open blobs
    source_blob = BlobClient.from_blob_url(source_url)
    dest_blob = init_blob_client(
        conn_str=os.environ[destination["conn_str"]],
        container_name=destination["container_name"],
        blob_name=blob_name,
    )

    # 5. Execute query server-side - project ONLY "hashed device id" column
    # This minimizes data transfer since output only needs deviceids
    query_sql = f'SELECT "hashed device id" FROM BlobStorage WHERE {where_clause}'

    # Collect query results
    dialect = DelimitedTextDialect(delimiter=",", quotechar='"', lineterminator="\n", has_header="true")
    result_data = source_blob.query_blob(query_sql, blob_format=dialect).readall()

    # 6. Build output CSV with only "deviceid" header
    result_text = result_data.decode("utf-8")
    reader = csv.reader(StringIO(result_text))
    output_buffer = StringIO()
    writer = csv.writer(output_buffer)
    writer.writerow(["deviceid"])

    next(reader, None)  # skip header
    for row in reader:
        if row:
            writer.writerow([row[0]])  # hashed device id is first column

    # 7. Upload result
    dest_blob.upload_blob(
        data=output_buffer.getvalue().encode("utf-8"),
        overwrite=True,
    )

    # 8. Generate SAS token
    sas_token = generate_blob_sas(
        account_name=dest_blob.account_name,
        container_name=dest_blob.container_name,
        blob_name=dest_blob.blob_name,
        account_key=dest_blob.credential.account_key,  # type: ignore[attr-defined]
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

    return f"{unquote(dest_blob.url)}?{sas_token}"

def rewrite_demographic_fields(json_logic: dict | str) -> dict:
    """
    Rewrite incoming demographic JsonLogic into storage-aligned JsonLogic.
    - Expands QueryBuilder-style select fields into one-hot boolean columns.
    - Renames direct-storage fields (e.g., homeOwner -> home_owner).
    Handles QueryBuilder-style `all` operator correctly.
    """
    import ast
    if isinstance(json_logic, str):
        json_logic = json.loads(json_logic)
    if isinstance(json_logic, str):
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
