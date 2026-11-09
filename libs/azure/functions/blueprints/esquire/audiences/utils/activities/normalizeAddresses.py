from __future__ import annotations

import base64
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

from libs.data import from_bind
from libs.utils.addrall import clean_addresses
from libs.utils.azure_storage import get_cached_blob_client


bp = Blueprint()


_NORMALIZATION_BATCH_SIZE = int(
    os.getenv(
        "ADDRESS_NORMALIZATION_BATCH_SIZE",
        "500",
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
        "60000",
    )
)


_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "line1": (
        "address",
        "add1",
        "address1",
        "address_1",
        "address_line_1",
        "delivery_line_1",
        "street",
        "street_address",
    ),
    "line2": (
        "add2",
        "address2",
        "address_2",
        "address_line_2",
        "delivery_line_2",
        "unit",
        "suite",
        "internal",  # PostGIS norm_addy secondary unit
    ),
    "city": (
        "city",
        "city_name",
        "location",
        "municipality",
    ),
    "state": (
        "state",
        "state_abbreviation",
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
        "full_zipcode",
    ),
    "zip4": (
        "plus4Code",
        "plus4_code",
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
        "streetnumber"
    ),
}


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
    fields = {
        role: _find_column(columns, candidates)
        for role, candidates in _FIELD_CANDIDATES.items()
    }

    # PostGIS norm_addy.address is the HOUSE NUMBER, not an address line.
    # Its output may also contain address_alphanumeric and streetName. Rebuild
    # the complete street before passing it to AddrAll; do not geocode "123"
    # as if it were a standalone address.
    names = {str(column).lower() for column in columns}
    is_norm_addy = (
        "streetname" in names
        and ("address" in names or "address_alphanumeric" in names)
        and bool(names.intersection({
            "stateabbrev", "location", "predirabbrev",
            "streettypeabbrev", "postdirabbrev", "address_alphanumeric", "zip",
        }))
    )
    if is_norm_addy:
        fields["line1"] = _find_column(
            columns,
            ("delivery_line_1", "address_line_1", "address1", "add1", "street_address"),
        )
        fields["primary_number"] = _find_column(
            columns, ("address_alphanumeric", "address", "primary_number"),
        )

    return fields


def _enriched_column(
    frame: pd.DataFrame,
    enriched: pd.DataFrame,
    *,
    target: str,
    enriched_name: str,
    existing_name: str | None,
) -> pd.Series:
    """Use standardization when present; never lose an unmatched input value."""
    canonical = enriched[enriched_name].astype("string").fillna("").str.strip()
    original_col = target if target in frame else existing_name
    if original_col and original_col in frame:
        original = frame[original_col].astype("string").fillna("")
    else:
        original = pd.Series("", index=frame.index, dtype="string")
    return canonical.where(canonical.ne(""), original)


def _reconstruct_street(
    frame: pd.DataFrame,
    fields: Mapping[str, str | None],
) -> pd.Series:
    """Allow already-separated neighbor input to receive the same enrichment."""
    parts = [fields["primary_number"]]
    parts.extend(
        _find_column(frame.columns, candidates)
        for candidates in (
            ("street_predirection", "predirabbrev", "predir"),
            ("street_name", "streetName", "streetname"),
            ("street_suffix", "streetTypeAbbrev", "streettypeabbrev"),
            ("street_postdirection", "postdirabbrev", "postdir"),
        )
    )
    columns = [p for p in parts if p]
    values = frame[columns].astype("string").fillna("")

    # A non-empty address_alphanumeric is the canonical house number. When
    # the parser supplied only a numeric address, fill the missing value from
    # that field for this row (not from a different address or an invented key).
    number_col = fields.get("primary_number")
    numeric_col = _find_column(frame.columns, ("address",))
    if (number_col and numeric_col and number_col != numeric_col
            and number_col.lower() == "address_alphanumeric"):
        number = values[number_col]
        values[number_col] = number.where(
            number.str.strip().ne(""),
            frame[numeric_col].astype("string").fillna(""),
        )

    return values.agg(
        lambda row: " ".join(part.strip() for part in row if part.strip()),
        axis=1,
    )


def _normalize_frame(
    *, frame: pd.DataFrame,
    fields: Mapping[str, str | None], session,
    use_geocode: bool = True,
    retry_missing_zip4: bool = True,
    geocode_sources: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Enrich every address, not only records missing canonical column names."""
    # read_csv(chunksize=...) retains global row indexes on later chunks;
    # bulk_validate returns zero-based indexes. Keep assignments positional.
    frame = frame.reset_index(drop=True)
    query_frame = frame
    address_column = fields.get("line1")
    if not address_column:
        query_frame = frame.copy()
        address_column = "__addrall_working_street"
        if address_column in query_frame.columns:
            raise ValueError(f"Reserved intermediate column already exists: {address_column}")
        query_frame[address_column] = _reconstruct_street(frame, fields)

    # The normal Smarty bridge would allocate dozens of diagnostic/USPS fields
    # only for this activity to discard them. Query the same AddrAll pipeline
    # with its bounded audience projection instead.
    enriched = clean_addresses(
        df=query_frame,
        ADDRESS=address_column,
        ADDRESS2=fields.get("line2"),
        CITY=fields.get("city"),
        STATE=fields.get("state"),
        ZIPCODE=fields.get("zip"),
        ZIP4=fields.get("zip4"),
        connection=session.connection().connection,
        batch_size=int(os.getenv("ADDRALL_BATCH_SIZE", "250")),
        use_geocode=use_geocode,
        retry_missing_zip4=retry_missing_zip4,
        geocode_sources=(
            geocode_sources if geocode_sources is not None
            else ("usps", "openaddresses")
        ),
        output_mode="audience",
    )
    # A previously supplied full last line is better than a newly assembled
    # *partial* locality. Do not replace full input information with less.
    if "last_line" in frame.columns:
        complete = (
            enriched[["city_name", "state_abbreviation", "zipcode"]]
            .astype("string").fillna("").apply(lambda col: col.str.strip().ne(""))
            .all(axis=1)
        )
        enriched.loc[~complete, "last_line"] = ""
    # The source is copied once. Preserve canonical input fields before
    # replacing them with improved address components.
    out = frame.copy()
    for target in (
        "primary_number", "street_name", "street_suffix", "city", "state",
        "street_predirection", "street_postdirection",
        "zipCode", "plus4Code", "latitude", "longitude",
        "delivery_line_1", "delivery_line_2", "last_line", "full_zipcode",
    ):
        original = f"addrall_original_{target}"
        if target in frame.columns and original not in frame.columns:
            out[original] = frame[target]

    for field in (
        "delivery_line_1", "delivery_line_2", "last_line", "full_zipcode",
        "street_suffix", "street_predirection", "street_postdirection",
        "addrall_match_status", "addrall_geocode_source",
        "addrall_geocode_match", "addrall_geocode_rating",
        "addrall_coordinate_source", "addrall_zip4_source",
        "addrall_normalize_status", "addrall_secondary_status",
    ):
        if field.startswith("addrall_"):
            out[field] = enriched[field]
        else:
            out[field] = _enriched_column(
                frame, enriched, target=field,
                enriched_name=field, existing_name=field,
            )

    for target, enriched_name, existing_name in (
        ("primary_number", "primary_number", fields.get("primary_number")),
        ("street_name", "street_name", fields.get("street_name")),
        ("city", "city_name", fields.get("city")),
        ("state", "state_abbreviation", fields.get("state")),
        ("zipCode", "zipcode", fields.get("zip")),
        ("plus4Code", "plus4_code", fields.get("zip4")),
        ("latitude", "latitude", "latitude"),
        ("longitude", "longitude", "longitude"),
    ):
        out[target] = _enriched_column(
            frame, enriched, target=target,
            enriched_name=enriched_name, existing_name=existing_name,
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
    *, temp, dest_blob: BlobClient,
    fields: Mapping[str, str | None], session,
    use_geocode: bool,
    retry_missing_zip4: bool,
    geocode_sources: Sequence[str] | None,
    source_columns: Sequence[str],
) -> None:
    block_list: list[BlobBlock] = []
    block_index = 0
    include_header = True
    # Aggregate small CSV chunks into full-size Azure blocks. Staging one block
    # per 500-row chunk would exhaust the 50,000-block limit on large files.
    pending = bytearray()
    temp.seek(0)

    for frame in pd.read_csv(
        temp, dtype="string", keep_default_na=False,
        chunksize=_NORMALIZATION_BATCH_SIZE,
    ):
        out = _normalize_frame(
            frame=frame, fields=fields, session=session,
            use_geocode=use_geocode,
            retry_missing_zip4=retry_missing_zip4,
            geocode_sources=geocode_sources,
        )
        payload = out.to_csv(index=False, header=include_header).encode("utf-8")
        pending.extend(payload)
        while len(pending) >= _OUTPUT_BLOCK_SIZE_BYTES:
            block_index = _stage_bytes(
                dest_blob=dest_blob,
                payload=bytes(pending[:_OUTPUT_BLOCK_SIZE_BYTES]),
                block_index=block_index, block_list=block_list,
            )
            del pending[:_OUTPUT_BLOCK_SIZE_BYTES]
        include_header = False
        del out, payload

    if include_header:
        # Preserve the input schema even when the CSV contains zero records.
        empty = _normalize_frame(
            frame=pd.DataFrame(columns=list(source_columns)),
            fields=fields, session=session,
            use_geocode=use_geocode,
            retry_missing_zip4=retry_missing_zip4,
            geocode_sources=geocode_sources,
        )
        pending.extend(empty.to_csv(index=False).encode("utf-8"))

    if pending:
        _stage_bytes(
            dest_blob=dest_blob, payload=bytes(pending),
            block_index=block_index, block_list=block_list,
        )

    dest_blob.commit_block_list(
        block_list, content_settings=ContentSettings(content_type="text/csv"),
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

    # AddrAll is installed on Keystone; data_source's table bind is not
    # necessarily the same database and is not used for address enrichment.
    temp = _download_to_tempfile(
        source_url
    )

    dest_blob: BlobClient | None = None
    session = None

    try:
        blob_columns = _read_columns(
            temp
        )

        fields = _resolve_fields(blob_columns)
        if not fields.get("line1") and not (
            fields.get("primary_number") and fields.get("street_name")
        ):
            raise ValueError(
                "Unable to identify a source street address or existing "
                f"street parts: {sorted(blob_columns)!r}"
            )

        session = from_bind("keystone").connect()
        with session.connection().connection.cursor() as cursor:
            cursor.execute(
                f"SET LOCAL statement_timeout = '{_SQL_STATEMENT_TIMEOUT_MS}ms'"
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
            use_geocode=bool(ingress.get("use_geocode", True)),
            retry_missing_zip4=bool(ingress.get("retry_missing_zip4", True)),
            geocode_sources=ingress.get("geocode_sources"),
            source_columns=blob_columns,
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