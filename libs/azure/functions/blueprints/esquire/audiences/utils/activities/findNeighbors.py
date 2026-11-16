import base64
import logging
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobBlock,
    BlobClient,
    BlobSasPermissions,
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
)

from libs.utils.addrall import canonical_street_key
from libs.utils.azure_storage import get_cached_blob_client
from libs.utils.esquire.neighbors.logic_async import (
    find_neighbors_for_street,
)
from libs.utils.esquire.neighbors.logic_sql import (
    AttomPartitionReader,
)


bp = Blueprint()


_RESULT_COLUMNS = [
    "address",
    "city",
    "state",
    "zipCode",
    "plus4Code",
]

_BLOCK_SIZE_BYTES = int(
    os.getenv(
        "NEIGHBORS_RESULT_BLOCK_SIZE_BYTES",
        str(8 * 1024 * 1024),
    )
)

_SOURCE_READ_CHUNK_ROWS = int(
    os.getenv("NEIGHBORS_SOURCE_READ_CHUNK_ROWS", "2000")
)
_RESULT_WRITE_CHUNK_ROWS = int(
    os.getenv("NEIGHBORS_RESULT_WRITE_CHUNK_ROWS", "2000")
)

_ORDINAL_SUFFIX_RE = re.compile(
    r"^(\d+)(ST|ND|RD|TH)$"
)


def _normalize_street_name(
    value: str,
) -> str:
    if not value:
        return ""

    # Apply the same apostrophe/spacing comparison key on source and
    # ATTOM streets; original client fields remain in the source blob.
    normalized = canonical_street_key(value)
    match = _ORDINAL_SUFFIX_RE.match(normalized)

    return match.group(1) if match else normalized


def _partition_key(
    *,
    city: Any,
    state: Any,
    zip_code: Any,
) -> tuple[str, str, str]:
    return (
        str(city).strip().upper(),
        str(state).strip().upper(),
        str(zip_code).strip().zfill(5),
    )


def _partition_key_from_definition(
    partition: Mapping[str, Any],
) -> tuple[str, str, str]:
    return _partition_key(
        city=partition["city"],
        state=partition["state"],
        zip_code=partition["zip"],
    )


def _iter_source_rows(url: str):
    """Read source CSV in bounded chunks, spilling the download to disk.

    A neighbor batch previously held the entire source blob as bytes *and*
    the parsed DataFrame/row dictionaries simultaneously. Azure activities can
    process several partition batches concurrently, so avoid that replication.
    """
    blob = get_cached_blob_client(url)
    with tempfile.TemporaryFile(mode="w+b") as temp:
        blob.download_blob(max_concurrency=1).readinto(temp)
        temp.seek(0)
        for chunk in pd.read_csv(
            temp,
            dtype="string",
            keep_default_na=False,
            chunksize=_SOURCE_READ_CHUNK_ROWS,
        ):
            yield from chunk.to_dict("records")


def _load_addresses_by_partition(
    *,
    source_urls: list[str],
    partitions: list[Mapping[str, Any]],
) -> dict[
    tuple[str, str, str],
    list[dict[str, Any]],
]:
    target_keys = {
        _partition_key_from_definition(partition)
        for partition in partitions
    }

    addresses_by_partition: dict[
        tuple[str, str, str],
        list[dict[str, Any]],
    ] = {}

    for url in source_urls:
        for row in _iter_source_rows(url):
            key = _partition_key(
                city=row.get("city", ""),
                state=row.get("state", ""),
                zip_code=row.get("zipCode", ""),
            )

            if key not in target_keys:
                continue

            addresses_by_partition.setdefault(
                key,
                [],
            ).append(row)

    return addresses_by_partition


def _prepare_source_addresses(
    addresses: list[dict[str, Any]],
) -> pd.DataFrame:
    df = pd.DataFrame(addresses)

    if (
        "primary_number" in df.columns
        and "street_number" not in df.columns
    ):
        df = df.rename(
            columns={
                "primary_number": "street_number",
            }
        )

    # Preserve the existing source-data semantics.
    df["street_name"] = (
        df["street_name"]
        .astype(str)
        .str.upper()
    )

    df["street_name"] = (
        df["street_name"]
        .astype(str)
        .map(_normalize_street_name)
    )

    return df


def _prepare_attom_data(
    data: pd.DataFrame,
) -> pd.DataFrame:
    if data.empty:
        return data

    out = data.copy()

    out["street_name"] = (
        out["street_name"]
        .astype(str)
        .str.upper()
        .map(_normalize_street_name)
    )

    return out


def _partition_neighbors(
    *,
    partition: Mapping[str, Any],
    addresses: list[dict[str, Any]],
    reader: AttomPartitionReader,
    n_per_side: int,
    same_side_only: bool,
) -> pd.DataFrame:
    if not addresses:
        return pd.DataFrame(
            columns=_RESULT_COLUMNS
        )

    source = _prepare_source_addresses(
        addresses
    )

    city, state, zip_code = (
        _partition_key_from_definition(
            partition
        )
    )

    data = reader.load_partition(
        city=city,
        state=state,
        zip_code=zip_code,
    )

    if data.empty:
        return pd.DataFrame(
            columns=_RESULT_COLUMNS
        )

    data = _prepare_attom_data(data)

    group_results: list[
        pd.DataFrame
    ] = []

    # pandas groupby sorts street_name by default, matching the existing
    # partition-processing path.
    for street_name, street_addresses in source.groupby(
        "street_name"
    ):
        street_data = data[
            data["street_name"] == street_name
        ]

        if street_data.empty:
            continue

        neighbors = find_neighbors_for_street(
            street_data,
            street_addresses,
            n_per_side,
            same_side_only,
        )

        if not neighbors.empty:
            group_results.append(neighbors)

    if not group_results:
        return pd.DataFrame(
            columns=_RESULT_COLUMNS
        )

    result = pd.concat(
        group_results,
        ignore_index=True,
    ).drop_duplicates()

    return result[_RESULT_COLUMNS]


def _iter_batch_csv_payloads(
    *,
    partitions: list[Mapping[str, Any]],
    addresses_by_partition: dict,
    n_per_side: int,
    same_side_only: bool,
    bind: str,
):
    """Yield one partition at a time; never concatenate all neighbor results."""
    has_rows = False
    with AttomPartitionReader(bind=bind) as reader:
        for partition in partitions:
            key = _partition_key_from_definition(partition)
            result = _partition_neighbors(
                partition=partition,
                addresses=addresses_by_partition.get(key, []),
                reader=reader,
                n_per_side=n_per_side,
                same_side_only=same_side_only,
            )
            if result.empty:
                continue
            for start in range(0, len(result), _RESULT_WRITE_CHUNK_ROWS):
                yield result.iloc[start:start + _RESULT_WRITE_CHUNK_ROWS][
                    _RESULT_COLUMNS
                ].to_csv(index=False, header=not has_rows).encode("utf-8")
                has_rows = True
    if not has_rows:
        yield (",".join(_RESULT_COLUMNS) + "\n").encode("utf-8")


def _create_destination_blob(
    *,
    conn_str: str,
    container_name: str,
    blob_name: str,
) -> BlobClient:
    return BlobClient.from_connection_string(
        conn_str=conn_str,
        container_name=container_name,
        blob_name=blob_name,
    )


def _stage_payload_as_blocks(
    *,
    dest_blob: BlobClient,
    payload,
) -> None:
    content_settings = ContentSettings(
        content_type="text/csv"
    )

    block_list: list[BlobBlock] = []

    buffered = bytearray()
    block_index = 0

    def stage(block: bytes) -> None:
        nonlocal block_index
        block_id = base64.b64encode(
            f"{block_index:08d}".encode(
                "ascii"
            )
        ).decode("ascii")

        dest_blob.stage_block(
            block_id=block_id,
            data=block,
        )

        block_list.append(
            BlobBlock(
                block_id=block_id
            )
        )
        block_index += 1

    for part in (payload,) if isinstance(payload, bytes) else payload:
        buffered.extend(part)
        while len(buffered) >= _BLOCK_SIZE_BYTES:
            stage(bytes(buffered[:_BLOCK_SIZE_BYTES]))
            del buffered[:_BLOCK_SIZE_BYTES]
    if buffered:
        stage(bytes(buffered))

    dest_blob.commit_block_list(
        block_list,
        content_settings=content_settings,
    )


def persist_neighbors_blob_to_history(
    *,
    dest_blob: BlobClient,
    run_id: str,
    audience_id: str,
) -> None:
    container_name = os.getenv(
        "NEIGHBORS_HISTORICAL_CONTAINER_NAME"
    )

    if not container_name:
        return

    blob_service = None

    try:
        blob_service = BlobServiceClient(
            account_url=(
                f"https://"
                f"{dest_blob.account_name}"
                f".blob.core.windows.net"
            ),
            credential=dest_blob.credential,
        )

        container = (
            blob_service
            .get_container_client(
                container_name
            )
        )

        try:
            container.create_container()
        except ResourceExistsError:
            pass

        blob_filename = (
            dest_blob.blob_name
            .split("/")[-1]
        )

        dest_path = (
            f"neighbors-history/"
            f"{audience_id}/"
            f"{run_id}/"
            f"{blob_filename}"
        )

        historical_blob = (
            container.get_blob_client(
                dest_path
            )
        )

        historical_blob.start_copy_from_url(
            dest_blob.url
        )

    except Exception:
        logging.exception(
            "Neighbor historical copy failed"
        )

    finally:
        if blob_service is not None:
            try:
                blob_service.close()
            except Exception:
                pass


def _create_result_url(
    dest_blob: BlobClient,
) -> str:
    sas = generate_blob_sas(
        account_name=dest_blob.account_name,
        account_key=(
            dest_blob
            .credential
            .account_key
        ),
        container_name=dest_blob.container_name,
        blob_name=dest_blob.blob_name,
        permission=BlobSasPermissions(
            read=True,
            write=True,
        ),
        expiry=(
            datetime.now(timezone.utc)
            + timedelta(hours=24)
        ),
    )

    return f"{dest_blob.url}?{sas}"


@bp.activity_trigger(
    input_name="ingress"
)
def activity_esquireAudiencesNeighbors_processBatch_blockblob(
    ingress: Mapping[str, Any],
) -> str:
    partitions = ingress["partitions"]
    source_urls = ingress.get(
        "source_urls",
        [],
    )
    dest = ingress["destination"]
    process = ingress.get(
        "process",
        {},
    )

    run_id = ingress["run_id"]
    audience_id = ingress["audience"]["id"]
    batch_index = ingress["batch_index"]

    bind = ingress.get(
        "db_bind",
        "keystone",
    )

    n_per_side = int(
        process.get(
            "housesPerSide",
            20,
        )
    )

    same_side_only = not bool(
        process.get(
            "bothSides",
            True,
        )
    )

    conn_str = os.getenv(
        dest["conn_str"],
        dest["conn_str"],
    )

    container_name = (
        dest["container_name"]
    )

    blob_prefix = str(
        dest.get(
            "blob_prefix",
            "",
        )
    ).strip("/")

    filename = (
        f"batch-{batch_index:05d}.csv"
    )

    blob_name = (
        f"{blob_prefix}/{filename}"
        if blob_prefix
        else filename
    )

    addresses_by_partition = (
        _load_addresses_by_partition(
            source_urls=source_urls,
            partitions=partitions,
        )
    )

    dest_blob = _create_destination_blob(
        conn_str=conn_str,
        container_name=container_name,
        blob_name=blob_name,
    )

    try:
        _stage_payload_as_blocks(
            dest_blob=dest_blob,
            payload=_iter_batch_csv_payloads(
                partitions=partitions,
                addresses_by_partition=addresses_by_partition,
                n_per_side=n_per_side,
                same_side_only=same_side_only,
                bind=bind,
            ),
        )

        persist_neighbors_blob_to_history(
            dest_blob=dest_blob,
            run_id=run_id,
            audience_id=audience_id,
        )

        return _create_result_url(
            dest_blob
        )

    finally:
        dest_blob.close()
