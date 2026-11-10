import base64
import logging
import os
import re
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

_ORDINAL_SUFFIX_RE = re.compile(
    r"^(\d+)(ST|ND|RD|TH)$"
)


def _normalize_street_name(
    value: str,
) -> str:
    if not value:
        return ""

    normalized = str(value).strip().upper()
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


def _download_source_blob(
    url: str,
) -> bytes:
    blob = get_cached_blob_client(url)

    return (
        blob
        .download_blob(
            max_concurrency=1,
        )
        .readall()
    )


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
        csv_bytes = _download_source_blob(url)

        rows = pd.read_csv(
            pd.io.common.BytesIO(csv_bytes),
            dtype={
                "zipCode": "string",
                "plus4Code": "string",
            },
        ).to_dict("records")

        for row in rows:
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


def _build_batch_payload(
    *,
    partitions: list[Mapping[str, Any]],
    addresses_by_partition: dict[
        tuple[str, str, str],
        list[dict[str, Any]],
    ],
    n_per_side: int,
    same_side_only: bool,
    bind: str,
) -> bytes:
    result_frames: list[
        pd.DataFrame
    ] = []

    # One connection/session for the entire small Durable activity.
    with AttomPartitionReader(
        bind=bind
    ) as reader:
        for partition in partitions:
            key = _partition_key_from_definition(
                partition
            )

            result = _partition_neighbors(
                partition=partition,
                addresses=addresses_by_partition.get(
                    key,
                    [],
                ),
                reader=reader,
                n_per_side=n_per_side,
                same_side_only=same_side_only,
            )

            if not result.empty:
                result_frames.append(result)

    if not result_frames:
        return (
            ",".join(_RESULT_COLUMNS)
            + "\n"
        ).encode("utf-8")

    output = pd.concat(
        result_frames,
        ignore_index=True,
    )

    return output.to_csv(
        index=False,
    ).encode("utf-8")


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
    payload: bytes,
) -> None:
    content_settings = ContentSettings(
        content_type="text/csv"
    )

    block_list: list[BlobBlock] = []

    for block_index, start in enumerate(
        range(
            0,
            len(payload),
            _BLOCK_SIZE_BYTES,
        )
    ):
        block_id = base64.b64encode(
            f"{block_index:08d}".encode(
                "ascii"
            )
        ).decode("ascii")

        dest_blob.stage_block(
            block_id=block_id,
            data=payload[
                start:
                start + _BLOCK_SIZE_BYTES
            ],
        )

        block_list.append(
            BlobBlock(
                block_id=block_id
            )
        )

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

    payload = _build_batch_payload(
        partitions=partitions,
        addresses_by_partition=(
            addresses_by_partition
        ),
        n_per_side=n_per_side,
        same_side_only=(
            same_side_only
        ),
        bind=bind,
    )

    dest_blob = _create_destination_blob(
        conn_str=conn_str,
        container_name=container_name,
        blob_name=blob_name,
    )

    try:
        _stage_payload_as_blocks(
            dest_blob=dest_blob,
            payload=payload,
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
