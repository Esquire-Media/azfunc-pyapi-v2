from __future__ import annotations

import base64
import csv
import logging
import os
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Any, Dict, List, Tuple

import pandas as pd
from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas

from libs.utils.azure_storage import get_cached_blob_client

# Import your DB loader + neighbor algorithm from the module you shared :contentReference[oaicite:5]{index=5}
from libs.utils.esquire.neighbors.logic_async import load_estated_data_db, find_neighbors_for_street

bp = Blueprint()
LOGGER = logging.getLogger(__name__)

# How long the returned SAS should be valid
_SAS_HOURS = int(os.getenv("NEIGHBORS_BATCH_SAS_HOURS", "48"))


def _norm_city(v: Any) -> str:
    return str(v or "").strip().upper()


def _norm_state(v: Any) -> str:
    return str(v or "").strip().upper()


def _norm_zip(v: Any) -> str:
    return str(v or "").strip()


def _partition_key(city: str, state: str, zip_code: str) -> Tuple[str, str, str]:
    return (_norm_city(city), _norm_state(state), _norm_zip(zip_code))


def _parse_conn_str(conn_str: str) -> Dict[str, str]:
    """
    Parse Azure Storage connection string into a dict.
    Needed to generate SAS with account key.
    """
    parts = {}
    for seg in conn_str.split(";"):
        if not seg.strip():
            continue
        if "=" not in seg:
            continue
        k, v = seg.split("=", 1)
        parts[k.strip()] = v.strip()
    return parts


def _make_read_sas_url(blob_client: BlobClient, conn_str_key: str) -> str:
    """
    Generate a read-only SAS URL matching the style your export_dataframe returns.
    """
    raw_conn_str = os.environ[conn_str_key]
    cs = _parse_conn_str(raw_conn_str)

    account_name = cs.get("AccountName")
    account_key = cs.get("AccountKey")
    if not account_name or not account_key:
        # Fall back to bare URL (may be fine if container is public / managed identity used elsewhere)
        return blob_client.url

    sas = generate_blob_sas(
        account_name=account_name,
        account_key=account_key,
        container_name=blob_client.container_name,
        blob_name=blob_client.blob_name,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=_SAS_HOURS),
    )
    return f"{blob_client.url}?{sas}"


def _csv_rows_for_neighbors(df: pd.DataFrame, include_header: bool) -> bytes:
    """
    Build CSV bytes for the neighbor output.
    We keep it consistent with prior output columns.
    """
    cols = ["address", "city", "state", "zipCode", "plus4Code"]
    out = df.reindex(columns=cols)
    sio = StringIO()
    out.to_csv(sio, index=False, header=include_header)
    return sio.getvalue().encode("utf-8")


@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesNeighbors_processPartitionBatch_blockblob(ingress: dict) -> str:
    """
    ingress = {
      "run_id": str,
      "batch_index": int,
      "partitions": [{"city":..,"state":..,"zip":..}, ...],
      "source_urls": [...],
      "destination": {"conn_str": "AzureWebJobsStorage", "container_name": "...", "blob_prefix": "..."},
      "process": {"housesPerSide": 20, "bothSides": True},
      "db_bind": "keystone"
    }

    Returns: SAS URL to a single CSV blob.
    Each partition corresponds to exactly one staged block.
    """

    run_id = str(ingress["run_id"])
    batch_index = int(ingress["batch_index"])
    partitions: List[dict] = ingress.get("partitions", []) or []
    source_urls: List[str] = ingress.get("source_urls", []) or []
    dest: dict = ingress["destination"]
    process: dict = ingress.get("process", {}) or {}
    bind: str = str(ingress.get("db_bind") or "keystone")

    n_per_side = int(process.get("housesPerSide", 20))
    # Keep legacy behavior consistent with your current pipeline wiring
    same_side_only = bool(process.get("bothSides", True))

    conn_str_key = dest.get("conn_str", "AzureWebJobsStorage")
    container = dest["container_name"]
    prefix = (dest.get("blob_prefix") or "").strip("/")

    # Deterministic blob name per (run_id, batch_index) => idempotent retries
    blob_name = f"{prefix}/neighbors/run_id={run_id}/batch-{batch_index:05d}.csv"

    blob_client = BlobClient.from_connection_string(
        os.environ[conn_str_key],
        container_name=container,
        blob_name=blob_name,
    )

    # Prepare the partition key set for this batch
    batch_keys: List[Tuple[str, str, str]] = []
    for p in partitions:
        batch_keys.append(_partition_key(p.get("city"), p.get("state"), p.get("zip")))

    key_set = set(batch_keys)

    # Build address rows per partition by scanning sources ONCE for this activity
    addresses_by_key: Dict[Tuple[str, str, str], List[dict]] = {k: [] for k in key_set}

    for url in source_urls:
        try:
            bc = get_cached_blob_client(url)
            csv_bytes = bc.download_blob().readall()
        except Exception as e:
            LOGGER.warning("Failed reading source blob", extra={"url": url, "err": repr(e)})
            continue

        try:
            reader = csv.DictReader(StringIO(csv_bytes.decode("utf-8")))
        except Exception as e:
            LOGGER.warning("Failed decoding source CSV", extra={"url": url, "err": repr(e)})
            continue

        for row in reader:
            k = _partition_key(row.get("city"), row.get("state"), row.get("zipCode") or row.get("zip"))
            if k in key_set:
                addresses_by_key[k].append(row)

    # Stage blocks
    block_ids: List[str] = []
    wrote_header = False

    # Make sure blob exists as block blob (stage_block is fine even if not yet created)
    for idx, part in enumerate(partitions):
        city = _norm_city(part.get("city"))
        state = _norm_state(part.get("state"))
        zip_code = _norm_zip(part.get("zip"))

        k = (city, state, zip_code)
        addr_rows = addresses_by_key.get(k, [])

        # Compute neighbors for this partition
        out_bytes: bytes = b""
        if addr_rows:
            addr_df = pd.DataFrame(addr_rows)

            # Normalize & align columns
            # Old pipeline sometimes used primary_number; align with street_number
            if "street_number" not in addr_df.columns and "primary_number" in addr_df.columns:
                addr_df = addr_df.rename(columns={"primary_number": "street_number"})

            # The find_neighbors code expects:
            # - street_name
            # - street_number
            if "street_name" in addr_df.columns:
                addr_df["street_name"] = addr_df["street_name"].astype(str).str.upper()

            # Ensure required fields exist
            if "street_number" in addr_df.columns and "street_name" in addr_df.columns:
                try:
                    estated_df = await load_estated_data_db(
                        city=city,
                        state=state,
                        zip_code=zip_code,
                        bind=bind,
                    )
                except Exception as e:
                    LOGGER.warning(
                        "DB estated load failed",
                        extra={"city": city, "state": state, "zip": zip_code, "err": repr(e)},
                    )
                    estated_df = pd.DataFrame()
                    raise e

                if not estated_df.empty:
                    group_results: List[pd.DataFrame] = []
                    est_street = estated_df["street_name"]

                    for street_name, street_addresses in addr_df.groupby("street_name"):
                        street_data = estated_df[est_street == str(street_name).upper()]
                        if street_data.empty:
                            continue

                        neighbors_df = find_neighbors_for_street(
                            street_data,
                            street_addresses,
                            n_per_side,
                            same_side_only,
                        )
                        if not neighbors_df.empty:
                            group_results.append(neighbors_df)

                    if group_results:
                        neighbors_out = pd.concat(group_results, ignore_index=True)
                        neighbors_out = neighbors_out.drop_duplicates()
                        out_bytes = _csv_rows_for_neighbors(neighbors_out, include_header=not wrote_header)
                        wrote_header = True

        # One block per partition (even if empty)
        block_id_raw = f"{run_id}:{batch_index:05d}:{idx:06d}"
        block_id = base64.b64encode(block_id_raw.encode("utf-8")).decode("utf-8")
        block_ids.append(block_id)

        await blob_client.stage_block(block_id=block_id, data=out_bytes)

    # Commit the full block list in the same order as partitions
    # (If all blocks are empty, commit creates an empty blob—fine.)
    await blob_client.commit_block_list(block_ids)

    sas_url = _make_read_sas_url(blob_client, conn_str_key)
    return sas_url