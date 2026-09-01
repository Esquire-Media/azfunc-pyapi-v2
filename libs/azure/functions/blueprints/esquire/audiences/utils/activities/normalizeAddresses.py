from __future__ import annotations

import base64
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

import pandas as pd
from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobBlock,
    BlobClient,
    BlobSasPermissions,
    ContentSettings,
    generate_blob_sas,
)

from libs.azure.functions.blueprints.esquire.audiences.builder.config import (
    MAPPING_DATASOURCE,
)
from libs.data import from_bind
from libs.utils.azure_storage import get_cached_blob_client


bp = Blueprint()


_NORMALIZATION_BATCH_SIZE = int(
    os.getenv(
        "ADDRESS_NORMALIZATION_BATCH_SIZE",
        "10000",
    )
)

_OUTPUT_BLOCK_SIZE_BYTES = int(
    os.getenv(
        "ADDRESS_NORMALIZATION_OUTPUT_BLOCK_SIZE_BYTES",
        str(8 * 1024 * 1024),
    )
)

_SQL_STATEMENT_TIMEOUT_MS = int(
    os.getenv(
        "ADDRESS_NORMALIZATION_SQL_STATEMENT_TIMEOUT_MS",
        "240000",
    )
)


_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "line1": (
        "address",
        "add1",
        "address1",
        "address_1",
        "address_line_1",
        "street_address",
    ),
    "line2": (
        "add2",
        "address2",
        "address_2",
        "address_line_2",
        "unit",
        "suite",
    ),
    "city": (
        "city",
        "location",
        "municipality",
    ),
    "state": (
        "state",
        "st",
        "state_abbrev",
        "stateabbrev",
    ),
    "zip": (
        "zipCode",
        "zip",
        "zipcode",
        "zip_code",
        "postal_code",
        "postalcode",
    ),
    "zip4": (
        "plus4Code",
        "zip4",
        "zip_4",
        "plus4",
    ),
    "street_name": (
        "street_name",
        "streetName",
        "streetname",
    ),
    "primary_number": (
        "primary_number",
        "street_number",
        "streetNumber",
        "address_number",
    ),
}


_TABLE_COLUMNS_SQL = """
SELECT column_name
FROM information_schema.columns
WHERE table_schema = %s
  AND table_name = %s
ORDER BY ordinal_position
"""


_NORMALIZE_BATCH_SQL = """
WITH input AS (
    SELECT *
    FROM jsonb_to_recordset(%s::jsonb)
        AS x(row_id bigint, full_address text)
)
SELECT
    input.row_id,
    normalized.address AS primary_number,
    normalized.streetname AS street_name,
    normalized.location AS city,
    normalized.stateabbrev AS state,
    normalized.zip AS zip_code,
    normalized.zip4 AS zip4
FROM input
LEFT JOIN LATERAL addresses.normalize(
    input.full_address
) AS normalized
    ON TRUE
ORDER BY input.row_id
"""


def _column_lookup(
    columns: Sequence[str],
) -> dict[str, str]:
    return {
        str(column).lower(): str(column)
        for column in columns
    }


def _find_column(
    columns: Sequence[str],
    candidates: Sequence[str],
) -> str | None:
    lookup = _column_lookup(columns)

    return next(
        (
            lookup[candidate.lower()]
            for candidate in candidates
            if candidate.lower() in lookup
        ),
        None,
    )


def _resolve_fields(
    columns: Sequence[str],
) -> dict[str, str | None]:
    return {
        role: _find_column(
            columns,
            candidates,
        )
        for role, candidates in _FIELD_CANDIDATES.items()
    }


def _is_neighbor_ready(
    columns: Sequence[str],
) -> bool:
    lookup = _column_lookup(columns)

    return (
        (
            "primary_number" in lookup
            or "street_number" in lookup
        )
        and "street_name" in lookup
        and "city" in lookup
        and "state" in lookup
        and "zipcode" in lookup
    )


def _source_metadata(
    data_source_id: str,
) -> dict[str, str]:
    config = MAPPING_DATASOURCE.get(
        data_source_id,
        {},
    )

    table = config.get(
        "table",
        {},
    )

    bind = str(
        config.get("bind") or ""
    ).strip()

    schema = str(
        table.get("schema") or ""
    ).strip()

    table_name = str(
        table.get("name") or ""
    ).strip()

    if not bind or not schema or not table_name:
        raise ValueError(
            "Unable to resolve source table for "
            f"dataSource {data_source_id!r}"
        )

    return {
        "bind": bind,
        "schema": schema,
        "table": table_name,
    }


def _load_table_columns(
    *,
    session,
    schema: str,
    table: str,
) -> list[str]:
    connection = session.connection()
    raw = connection.connection
    cursor = raw.cursor()

    try:
        cursor.execute(
            _TABLE_COLUMNS_SQL,
            (
                schema,
                table,
            ),
        )

        return [
            str(row[0])
            for row in cursor.fetchall()
        ]
    finally:
        cursor.close()
        session.rollback()


def _string_value(
    row: Mapping[str, Any],
    column: str | None,
) -> str:
    if not column:
        return ""

    value = row.get(column)

    if value is None or value is pd.NA:
        return ""

    value = str(value).strip()

    return (
        ""
        if value.lower() == "nan"
        else value
    )


def _build_address_string(
    row: Mapping[str, Any],
    fields: Mapping[str, str | None],
) -> str:
    zip_code = _string_value(
        row,
        fields.get("zip"),
    )

    zip4 = _string_value(
        row,
        fields.get("zip4"),
    )

    postal_code = (
        f"{zip_code}-{zip4}"
        if zip_code and zip4
        else zip_code
    )

    return " ".join(
        part
        for part in (
            _string_value(
                row,
                fields.get("line1"),
            ),
            _string_value(
                row,
                fields.get("line2"),
            ),
            _string_value(
                row,
                fields.get("city"),
            ),
            _string_value(
                row,
                fields.get("state"),
            ),
            postal_code,
        )
        if part
    )


def _validate_fields(
    *,
    fields: Mapping[str, str | None],
    blob_columns: Sequence[str],
    table_columns: Sequence[str],
    source_metadata: Mapping[str, str],
) -> None:
    if fields.get("line1"):
        return

    raise ValueError(
        "Unable to identify an address field in source blob. "
        f"blob_columns={sorted(blob_columns)!r}; "
        f"source="
        f"{source_metadata['schema']}."
        f"{source_metadata['table']}; "
        f"table_columns={sorted(table_columns)!r}"
    )


def _normalize_batch(
    *,
    session,
    addresses: Sequence[str],
) -> list[
    tuple[
        int,
        int | None,
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
    ]
]:
    payload = json.dumps(
        [
            {
                "row_id": row_id,
                "full_address": address,
            }
            for row_id, address in enumerate(
                addresses
            )
        ],
        separators=(",", ":"),
    )

    connection = session.connection()
    raw = connection.connection
    cursor = raw.cursor()

    try:
        cursor.execute(
            "SET LOCAL statement_timeout = "
            f"'{_SQL_STATEMENT_TIMEOUT_MS}ms'"
        )

        cursor.execute(
            _NORMALIZE_BATCH_SQL,
            (payload,),
        )

        return [
            (
                int(row_id),
                primary_number,
                street_name,
                city,
                state,
                zip_code,
                zip4,
            )
            for (
                row_id,
                primary_number,
                street_name,
                city,
                state,
                zip_code,
                zip4,
            ) in cursor.fetchall()
        ]
    finally:
        cursor.close()
        session.rollback()


def _canonicalize_geo_fields(
    frame: pd.DataFrame,
    fields: Mapping[str, str | None],
) -> pd.DataFrame:
    out = frame.copy()

    for target, source in {
        "city": fields.get("city"),
        "state": fields.get("state"),
        "zipCode": fields.get("zip"),
        "plus4Code": fields.get("zip4"),
    }.items():
        if target in out.columns:
            continue

        out[target] = (
            out[source]
            if source and source in out.columns
            else ""
        )

    return out


def _project_existing_street_fields(
    frame: pd.DataFrame,
    fields: Mapping[str, str | None],
) -> pd.DataFrame:
    out = _canonicalize_geo_fields(
        frame,
        fields,
    )

    street_name = fields.get(
        "street_name"
    )

    primary_number = fields.get(
        "primary_number"
    )

    if (
        "street_name" not in out.columns
        and street_name
    ):
        out["street_name"] = out[
            street_name
        ]

    if (
        "primary_number" not in out.columns
        and primary_number
    ):
        out["primary_number"] = out[
            primary_number
        ]

    return out


def _normalized_value(
    values: Sequence[tuple[Any, ...]],
    *,
    position: int,
    row_count: int,
) -> list[Any]:
    by_row_id = {
        row[0]: row[position]
        for row in values
    }

    return [
        by_row_id.get(row_id)
        for row_id in range(
            row_count
        )
    ]


def _apply_geo_fallback(
    *,
    out: pd.DataFrame,
    target: str,
    source: str | None,
    normalized_values: Sequence[Any],
) -> None:
    if target in out.columns:
        current = (
            out[target]
            .astype("string")
            .fillna("")
        )
    elif source and source in out.columns:
        current = (
            out[source]
            .astype("string")
            .fillna("")
        )
    else:
        current = pd.Series(
            [""] * len(out),
            index=out.index,
            dtype="string",
        )

    fallback = pd.Series(
        [
            ""
            if value is None
            else str(value)
            for value in normalized_values
        ],
        index=out.index,
        dtype="string",
    )

    out[target] = current.where(
        current.str.strip().ne(""),
        fallback,
    )


def _normalize_frame(
    *,
    frame: pd.DataFrame,
    fields: Mapping[str, str | None],
    session,
) -> pd.DataFrame:
    records = frame.to_dict(
        "records"
    )

    normalized = _normalize_batch(
        session=session,
        addresses=[
            _build_address_string(
                row,
                fields,
            )
            for row in records
        ],
    )

    row_count = len(frame)

    out = frame.copy()

    out["primary_number"] = pd.array(
        _normalized_value(
            normalized,
            position=1,
            row_count=row_count,
        ),
        dtype="Int64",
    )

    out["street_name"] = [
        value or ""
        for value in _normalized_value(
            normalized,
            position=2,
            row_count=row_count,
        )
    ]

    for target, source, position in (
        (
            "city",
            fields.get("city"),
            3,
        ),
        (
            "state",
            fields.get("state"),
            4,
        ),
        (
            "zipCode",
            fields.get("zip"),
            5,
        ),
        (
            "plus4Code",
            fields.get("zip4"),
            6,
        ),
    ):
        _apply_geo_fallback(
            out=out,
            target=target,
            source=source,
            normalized_values=_normalized_value(
                normalized,
                position=position,
                row_count=row_count,
            ),
        )

    return out


def _create_destination_blob(
    *,
    destination: Mapping[str, Any],
    source_index: int,
) -> BlobClient:
    conn_str_value = str(
        destination["conn_str"]
    )

    conn_str = os.getenv(
        conn_str_value,
        conn_str_value,
    )

    blob_prefix = str(
        destination.get(
            "blob_prefix",
            "",
        )
    ).strip("/")

    filename = (
        f"source-{source_index:05d}.csv"
    )

    blob_name = (
        f"{blob_prefix}/{filename}"
        if blob_prefix
        else filename
    )

    return BlobClient.from_connection_string(
        conn_str=conn_str,
        container_name=destination[
            "container_name"
        ],
        blob_name=blob_name,
    )


def _stage_bytes(
    *,
    dest_blob: BlobClient,
    payload: bytes,
    block_index: int,
    block_list: list[BlobBlock],
) -> int:
    for start in range(
        0,
        len(payload),
        _OUTPUT_BLOCK_SIZE_BYTES,
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
                start + _OUTPUT_BLOCK_SIZE_BYTES
            ],
        )

        block_list.append(
            BlobBlock(
                block_id=block_id
            )
        )

        block_index += 1

    return block_index


def _create_read_url(
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
        ),
        expiry=(
            datetime.now(timezone.utc)
            + timedelta(hours=24)
        ),
    )

    return f"{dest_blob.url}?{sas}"


def _download_to_tempfile(
    source_url: str,
):
    source_blob = get_cached_blob_client(
        source_url
    )

    temp = tempfile.NamedTemporaryFile(
        mode="w+b",
        suffix=".csv",
    )

    try:
        source_blob.download_blob(
            max_concurrency=1,
        ).readinto(temp)

        temp.flush()
        temp.seek(0)

        return temp
    except Exception:
        temp.close()
        raise


def _read_columns(
    temp,
) -> list[str]:
    temp.seek(0)

    columns = list(
        pd.read_csv(
            temp,
            nrows=0,
        ).columns
    )

    temp.seek(0)

    return [
        str(column)
        for column in columns
    ]


def _write_transformed_blob(
    *,
    temp,
    dest_blob: BlobClient,
    fields: Mapping[str, str | None],
    session,
    use_normalizer: bool,
) -> None:
    block_list: list[
        BlobBlock
    ] = []

    block_index = 0
    include_header = True

    temp.seek(0)

    for frame in pd.read_csv(
        temp,
        dtype="string",
        keep_default_na=False,
        chunksize=_NORMALIZATION_BATCH_SIZE,
    ):
        out = (
            _normalize_frame(
                frame=frame,
                fields=fields,
                session=session,
            )
            if use_normalizer
            else _project_existing_street_fields(
                frame,
                fields,
            )
        )

        payload = out.to_csv(
            index=False,
            header=include_header,
        ).encode("utf-8")

        block_index = _stage_bytes(
            dest_blob=dest_blob,
            payload=payload,
            block_index=block_index,
            block_list=block_list,
        )

        include_header = False

    if not block_list:
        payload = pd.DataFrame(
            columns=[
                "primary_number",
                "street_name",
                "city",
                "state",
                "zipCode",
                "plus4Code",
            ]
        ).to_csv(
            index=False
        ).encode("utf-8")

        _stage_bytes(
            dest_blob=dest_blob,
            payload=payload,
            block_index=0,
            block_list=block_list,
        )

    dest_blob.commit_block_list(
        block_list,
        content_settings=ContentSettings(
            content_type="text/csv"
        ),
    )


@bp.activity_trigger(
    input_name="ingress"
)
def activity_esquireAudiencesNeighbors_normalizeAddressBlob(
    ingress: Mapping[str, Any],
) -> dict[str, Any]:
    source_url = str(
        ingress["source_url"]
    )

    source_index = int(
        ingress["source_index"]
    )

    destination = ingress[
        "destination"
    ]

    data_source_id = str(
        ingress.get(
            "data_source",
            {},
        ).get("id") or ""
    ).strip()

    if not data_source_id:
        raise ValueError(
            "Normalization requires data_source.id"
        )

    temp = _download_to_tempfile(
        source_url
    )

    dest_blob: BlobClient | None = None
    session = None

    try:
        blob_columns = _read_columns(
            temp
        )

        if _is_neighbor_ready(
            blob_columns
        ):
            return {
                "url": source_url,
                "transformed": False,
            }

        metadata = _source_metadata(
            data_source_id
        )

        provider = from_bind(
            metadata["bind"]
        )

        session = provider.connect()

        table_columns = _load_table_columns(
            session=session,
            schema=metadata["schema"],
            table=metadata["table"],
        )

        fields = _resolve_fields(
            blob_columns
        )

        has_existing_street_parts = bool(
            fields.get("street_name")
            and fields.get(
                "primary_number"
            )
        )

        use_normalizer = not (
            has_existing_street_parts
        )

        if use_normalizer:
            _validate_fields(
                fields=fields,
                blob_columns=blob_columns,
                table_columns=table_columns,
                source_metadata=metadata,
            )

        dest_blob = _create_destination_blob(
            destination=destination,
            source_index=source_index,
        )

        _write_transformed_blob(
            temp=temp,
            dest_blob=dest_blob,
            fields=fields,
            session=session,
            use_normalizer=use_normalizer,
        )

        return {
            "url": _create_read_url(
                dest_blob
            ),
            "transformed": True,
        }

    finally:
        temp.close()

        if session is not None:
            try:
                session.close()
            except Exception:
                pass

        if dest_blob is not None:
            dest_blob.close()