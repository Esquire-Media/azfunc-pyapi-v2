import logging
import os
import re
from datetime import datetime
from typing import Any, Iterator, Mapping, TypeVar

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    BlobServiceClient,
    ContentSettings,
    ExponentialRetry,
    generate_blob_sas,
)

from libs.utils.azure_storage import get_cached_blob_client
from libs.utils.esquire.neighbors.logic_sql import NeighborSqlReader


bp = Blueprint()

T = TypeVar("T")


_SQL_PARTITIONS_PER_QUERY = int(
    os.getenv("NEIGHBORS_SQL_PARTITIONS_PER_QUERY", "10")
)
_BLOB_CONNECT_TIMEOUT_SECONDS = int(
    os.getenv("NEIGHBORS_BLOB_CONNECT_TIMEOUT_SECONDS", "10")
)
_BLOB_READ_TIMEOUT_SECONDS = int(
    os.getenv("NEIGHBORS_BLOB_READ_TIMEOUT_SECONDS", "30")
)
_BLOB_SERVER_TIMEOUT_SECONDS = int(
    os.getenv("NEIGHBORS_BLOB_SERVER_TIMEOUT_SECONDS", "30")
)


_ORDINAL_SUFFIX_RE = re.compile(
    r"^(\d+)(ST|ND|RD|TH)$"
)


def _chunked(
    items: list[T],
    size: int,
) -> Iterator[list[T]]:
    if size <= 0:
        raise ValueError("Chunk size must be > 0")

    for start in range(0, len(items), size):
        yield items[start:start + size]


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
    """
    Keep the existing cached-client/authentication behavior while
    applying bounded per-operation transport/server timeouts.
    """
    blob = get_cached_blob_client(url)

    downloader = blob.download_blob(
        max_concurrency=1,
        timeout=_BLOB_SERVER_TIMEOUT_SECONDS,
        connection_timeout=_BLOB_CONNECT_TIMEOUT_SECONDS,
        read_timeout=_BLOB_READ_TIMEOUT_SECONDS,
    )

    return downloader.readall()


def _load_addresses_by_partition(
    *,
    source_urls: list[str],
    partitions: list[Mapping[str, Any]],
) -> dict[
    tuple[str, str, str],
    list[dict[str, Any]],
]:
    """
    Parse each source blob once for this activity, but retain only rows
    belonging to this activity's partition batch.
    """
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


def _build_neighbor_requests(
    *,
    indexed_partitions: list[
        tuple[int, Mapping[str, Any]]
    ],
    addresses_by_partition: dict[
        tuple[str, str, str],
        list[dict[str, Any]],
    ],
) -> list[dict[str, Any]]:
    """
    Reproduce the source-address preparation in the current Python path.

    Compatibility details:
      - primary_number is renamed only when street_number does not exist
        as a DataFrame column at all;
      - street_name uses the existing normalization;
      - base_street_num uses pd.to_numeric(errors="coerce");
      - source_ord preserves the original source-row order.
    """
    requests: list[dict[str, Any]] = []

    for partition_ord, partition in indexed_partitions:
        key = _partition_key_from_definition(
            partition
        )
        city, state, zip_code = key

        addresses = addresses_by_partition.get(
            key,
            [],
        )

        if not addresses:
            continue

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

        # Preserve current failure behavior for malformed source schemas.
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

        df["source_ord"] = range(len(df))

        df = df.dropna(
            subset=["street_number"]
        ).copy()

        df["base_street_num"] = pd.to_numeric(
            df["street_number"],
            errors="coerce",
        )

        df = df.dropna(
            subset=["base_street_num"]
        )

        for row in df.itertuples(
            index=False
        ):
            base_street_num = row.base_street_num
            item = getattr(
                base_street_num,
                "item",
                None,
            )
            if item is not None:
                base_street_num = item()

            source_ord = row.source_ord
            item = getattr(
                source_ord,
                "item",
                None,
            )
            if item is not None:
                source_ord = item()

            requests.append(
                {
                    "partition_ord": int(
                        partition_ord
                    ),
                    "source_ord": int(
                        source_ord
                    ),
                    "city": city,
                    "state": state,
                    "zip_code": zip_code,
                    "street_name": row.street_name,
                    "base_street_num": (
                        base_street_num
                    ),
                }
            )

    return requests


def _build_batch_payload(
    *,
    partitions: list[
        Mapping[str, Any]
    ],
    addresses_by_partition: dict[
        tuple[str, str, str],
        list[dict[str, Any]],
    ],
    n_per_side: int,
    same_side_only: bool,
    bind: str,
) -> bytes:
    """
    Keep the Durable activity boundary large while keeping each SQL query
    small. partition_ord is assigned before SQL chunking so ordering is
    activity-wide and does not restart for every SQL query.
    """
    indexed_partitions = list(
        enumerate(partitions)
    )

    result_frames: list[
        pd.DataFrame
    ] = []

    with NeighborSqlReader(
        bind=bind
    ) as reader:
        for partition_chunk in _chunked(
            indexed_partitions,
            _SQL_PARTITIONS_PER_QUERY,
        ):
            requests = _build_neighbor_requests(
                indexed_partitions=partition_chunk,
                addresses_by_partition=(
                    addresses_by_partition
                ),
            )

            if not requests:
                continue

            result = reader.find_neighbors(
                requests=requests,
                n_per_side=n_per_side,
                same_side_only=same_side_only,
            )

            if not result.empty:
                result_frames.append(result)

    if not result_frames:
        # Preserve current empty-output behavior: zero-byte blob.
        return b""

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
        connection_timeout=(
            _BLOB_CONNECT_TIMEOUT_SECONDS
        ),
        read_timeout=(
            _BLOB_READ_TIMEOUT_SECONDS
        ),
        retry_policy=ExponentialRetry(
            initial_backoff=1,
            increment_base=2,
            retry_total=2,
        ),
    )


def _upload_result(
    *,
    dest_blob: BlobClient,
    payload: bytes,
) -> None:
    """
    One SDK-managed upload replaces per-partition stage_block calls plus
    commit_block_list.
    """
    dest_blob.upload_blob(
        payload,
        overwrite=True,
        max_concurrency=1,
        timeout=_BLOB_SERVER_TIMEOUT_SECONDS,
        connection_timeout=(
            _BLOB_CONNECT_TIMEOUT_SECONDS
        ),
        read_timeout=(
            _BLOB_READ_TIMEOUT_SECONDS
        ),
        content_settings=ContentSettings(
            content_type="text/csv"
        ),
    )


def persist_neighbors_blob_to_history(
    *,
    dest_blob: BlobClient,
    run_id: str,
    audience_id: str,
) -> None:
    """
    Historical persistence is strictly best-effort. No failure anywhere
    in this path may fail the audience-build activity.
    """
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
            connection_timeout=(
                _BLOB_CONNECT_TIMEOUT_SECONDS
            ),
            read_timeout=(
                _BLOB_READ_TIMEOUT_SECONDS
            ),
            retry_policy=ExponentialRetry(
                initial_backoff=1,
                increment_base=2,
                retry_total=1,
            ),
        )

        container = (
            blob_service
            .get_container_client(
                container_name
            )
        )

        try:
            container.create_container(
                timeout=(
                    _BLOB_SERVER_TIMEOUT_SECONDS
                ),
                connection_timeout=(
                    _BLOB_CONNECT_TIMEOUT_SECONDS
                ),
                read_timeout=(
                    _BLOB_READ_TIMEOUT_SECONDS
                ),
            )
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
            dest_blob.url,
            timeout=(
                _BLOB_SERVER_TIMEOUT_SECONDS
            ),
            connection_timeout=(
                _BLOB_CONNECT_TIMEOUT_SECONDS
            ),
            read_timeout=(
                _BLOB_READ_TIMEOUT_SECONDS
            ),
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
    return (
        dest_blob.url
        + "?"
        + generate_blob_sas(
            account_name=(
                dest_blob.account_name
            ),
            account_key=(
                dest_blob
                .credential
                .account_key
            ),
            container_name=(
                dest_blob.container_name
            ),
            blob_name=(
                dest_blob.blob_name
            ),
            permission=BlobSasPermissions(
                read=True,
                write=True,
            ),
            # Preserve current SAS-expiry behavior.
            expiry=(
                datetime.utcnow()
                .replace(
                    hour=23,
                    minute=59,
                )
            ),
        )
    )


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

    blob_name = (
        f"{blob_prefix}/"
        f"batch-{batch_index:05d}.csv"
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
        _upload_result(
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
