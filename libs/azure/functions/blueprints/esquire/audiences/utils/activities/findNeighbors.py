import base64
import os
import uuid
from datetime import datetime
from typing import Any, Iterable, Iterator, List, Mapping

import pandas as pd
from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobBlock,
    BlobClient,
    ContentSettings,
    BlobSasPermissions,
    generate_blob_sas,
)

from libs.utils.azure_storage import get_cached_blob_client
from libs.utils.esquire.neighbors.logic_async import (
    load_estated_data_db,
    find_neighbors_for_street,
)

bp = Blueprint()

def _new_block_id() -> str:
    return base64.b64encode(uuid.uuid4().bytes).decode("ascii")


def _stage_iterable_as_blocks(
    dest: BlobClient, data_parts: Iterable[bytes]
) -> list[BlobBlock]:

    blocks: list[BlobBlock] = []

    for part in data_parts:
        if not part:
            continue

        block_id = _new_block_id()
        dest.stage_block(block_id=block_id, data=part)
        blocks.append(BlobBlock(block_id=block_id))

    return blocks


def _partition_csv_bytes(
    city: str,
    state: str,
    zip_code: str,
    addresses: list[dict],
    n_per_side: int,
    same_side_only: bool,
    bind: str,
) -> bytes:

    if not addresses:
        return b""

    df = pd.DataFrame(addresses)

    if "primary_number" in df.columns and "street_number" not in df.columns:
        df = df.rename(columns={"primary_number": "street_number"})

    df["street_name"] = df["street_name"].astype(str).str.upper()

    estated_df = load_estated_data_db(
        city=city,
        state=state,
        zip_code=zip_code,
        bind=bind,
    )

    if estated_df.empty:
        return b""

    group_results: list[pd.DataFrame] = []
    est_street = estated_df["street_name"]

    for street_name, street_addresses in df.groupby("street_name"):
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

    if not group_results:
        return b""

    out_df = pd.concat(group_results, ignore_index=True).drop_duplicates()

    cols = ["address", "city", "state", "zipCode", "plus4Code"]
    out_df = out_df.reindex(columns=cols)

    return out_df.to_csv(index=False, header=False).encode("utf-8")


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesNeighbors_processBatch_blockblob(
    ingress: Mapping[str, Any]
) -> str:

    partitions = ingress["partitions"]
    source_urls = ingress.get("source_urls", [])
    dest = ingress["destination"]
    process = ingress.get("process", {})
    run_id = ingress["run_id"]
    batch_index = ingress["batch_index"]
    bind = ingress.get("db_bind", "keystone")

    n_per_side = int(process.get("housesPerSide", 20))
    same_side_only = not bool(process.get("bothSides", True))

    conn_str = os.getenv(dest["conn_str"], dest["conn_str"])
    container_name = dest["container_name"]
    blob_prefix = dest.get("blob_prefix", "").strip("/")

    blob_name = (
        f"{blob_prefix}/batch-{batch_index:05d}.csv"
    )

    dest_blob = BlobClient.from_connection_string(
        conn_str=conn_str,
        container_name=container_name,
        blob_name=blob_name,
    )

    # Build address map once per activity
    addresses_by_partition = {}

    for url in source_urls:
        bc = get_cached_blob_client(url)
        csv_bytes = bc.download_blob().readall()
        rows = pd.read_csv(pd.io.common.BytesIO(csv_bytes)).to_dict("records")

        for row in rows:
            key = (
                row.get("city", "").strip().upper(),
                row.get("state", "").strip().upper(),
                str(row.get("zipCode", "")).strip().zfill(5),
            )
            addresses_by_partition.setdefault(key, []).append(row)

    def _iter_partition_blocks() -> Iterator[bytes]:
        header_written = False

        for part in partitions:
            city = part["city"].strip().upper()
            state = part["state"].strip().upper()
            zip_code = part["zip"].strip()

            key = (city, state, zip_code)
            addresses = addresses_by_partition.get(key, [])

            data = _partition_csv_bytes(
                city,
                state,
                zip_code,
                addresses,
                n_per_side,
                same_side_only,
                bind,
            )

            if data and not header_written:
                header = b"address,city,state,zipCode,plus4Code\n"
                yield header
                header_written = True

            yield data

    block_list = _stage_iterable_as_blocks(dest_blob, _iter_partition_blocks())

    if not block_list:
        dest_blob.upload_blob(
            b"",
            overwrite=True,
            content_settings=ContentSettings(content_type="text/csv"),
        )
    else:
        dest_blob.commit_block_list(
            block_list,
            content_settings=ContentSettings(content_type="text/csv"),
        )

    return (
        dest_blob.url
        + "?"
        + generate_blob_sas(
            account_name=dest_blob.account_name,
            account_key=dest_blob.credential.account_key,
            container_name=dest_blob.container_name,
            blob_name=dest_blob.blob_name,
            permission=BlobSasPermissions(read=True, write=True),
            expiry=datetime.utcnow().replace(hour=23, minute=59),
        )
    )