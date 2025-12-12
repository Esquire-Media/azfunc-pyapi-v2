from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, Mapping, Tuple, Union

import codecs
import csv
import itertools
import logging
import os
import uuid

import fsspec
from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient

bp = Blueprint()

_DEVICE_ID_COLS = ("deviceid", "device_id")
_AZURE_APPEND_BLOB_MAX_BLOCK_BYTES = 4 * 1024 * 1024


def _build_blob_client(source: Union[str, Dict[str, Any]]) -> BlobClient:
    """
    Build a BlobClient from either:
      - a blob URL string, or
      - a dict describing the binding configuration.
    """
    if isinstance(source, str):
        return BlobClient.from_blob_url(source)

    conn_str_env_name = source["conn_str"]
    return BlobClient.from_connection_string(
        conn_str=os.environ[conn_str_env_name],
        container_name=source["container_name"],
        blob_name=source["blob_name"],
    )


def _resolve_s3_path(destination: Dict[str, Any]) -> str:
    """
    Build the full s3:// URL from destination dict.
    """
    bucket: str = destination["bucket"]

    object_key_raw = destination.get("object_key")
    if object_key_raw:
        object_key = str(object_key_raw)
    else:
        account_id_raw = destination.get("account_id") or os.getenv(
            "FREEWHEEL_BUZZ_ACCOUNT_ID"
        )
        if not account_id_raw:
            raise KeyError(
                "S3 destination requires either 'object_key' or a configured "
                "account_id (destination['account_id'] or FREEWHEEL_BUZZ_ACCOUNT_ID)."
            )

        account_id = str(account_id_raw)
        object_key = f"user-list/dsp/{account_id}/segment-{uuid.uuid4().hex}.txt"

    object_key = object_key.lstrip("/")
    return f"s3://{bucket}/{object_key}"


def _parse_s3_url(s3_url: str) -> Tuple[str, str]:
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Expected s3:// URL, got: {s3_url!r}")
    rest = s3_url[len("s3://") :]
    parts = rest.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def _iter_text_lines_from_chunks(
    chunks: Iterable[bytes],
    *,
    encoding: str = "utf-8",
) -> Iterator[str]:
    decoder = codecs.getincrementaldecoder(encoding)()
    buf = ""

    for chunk in chunks:
        if not chunk:
            continue

        buf += decoder.decode(chunk)

        while True:
            nl = buf.find("\n")
            if nl < 0:
                break
            line = buf[: nl + 1]
            buf = buf[nl + 1 :]
            yield line

    tail = decoder.decode(b"", final=True)
    if tail:
        buf += tail

    if buf:
        yield buf if buf.endswith("\n") else (buf + "\n")


def _clean_token(s: str) -> str:
    return s.strip().lstrip("\ufeff").strip().strip('"').strip("'")


def _parse_csv_cells(line: str) -> list[str]:
    record = line.rstrip("\r\n")
    try:
        return next(csv.reader([record]))
    except Exception:
        return [record]


def _looks_like_header(line: str) -> bool:
    cells = _parse_csv_cells(line)
    lowered = {_clean_token(c).casefold() for c in cells if _clean_token(c)}
    return any(col in lowered for col in _DEVICE_ID_COLS)


def _extract_device_id_from_header_row(row: Dict[str, Any], device_key: str) -> str:
    raw = row.get(device_key) or ""
    if raw is None:
        return ""
    return str(raw).strip()


def _extract_device_id_from_line(line: str) -> str:
    stripped = _clean_token(line.rstrip("\r\n"))
    if not stripped:
        return ""

    if any(ch in stripped for ch in (",", "\t", '"', ";")):
        cells = _parse_csv_cells(stripped)
        if cells:
            return _clean_token(str(cells[0]))

    return stripped


def _coerce_max_append_block_bytes(raw: Any) -> int:
    default_val = _AZURE_APPEND_BLOB_MAX_BLOCK_BYTES
    if raw is None or raw == "":
        return default_val
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return default_val
    if val <= 0:
        return default_val
    return min(val, _AZURE_APPEND_BLOB_MAX_BLOCK_BYTES)


def _build_botocore_client(
    service: str,
    *,
    access_key: str,
    secret_key: str,
    session_token: str | None,
    region: str | None,
):
    """
    Use botocore directly (no boto3 dependency). botocore is typically present via s3fs.
    """
    try:
        import botocore.session  # type: ignore
        from botocore.config import Config  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "botocore is required for STS AssumeRole / PutObjectAcl but was not importable. "
            "Ensure your environment includes botocore (commonly installed with s3fs)."
        ) from exc

    sess = botocore.session.get_session()
    cfg = Config(retries={"max_attempts": 5, "mode": "standard"})
    return sess.create_client(
        service,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        config=cfg,
    )


def _assume_role(
    *,
    base_access_key: str,
    base_secret_key: str,
    base_session_token: str | None,
    role_arn: str,
    role_session_name: str,
    region: str | None,
    external_id: str | None = None,
) -> Dict[str, str]:
    sts = _build_botocore_client(
        "sts",
        access_key=base_access_key,
        secret_key=base_secret_key,
        session_token=base_session_token,
        region=region,
    )

    params: Dict[str, Any] = {
        "RoleArn": role_arn,
        "RoleSessionName": role_session_name,
    }
    if external_id:
        params["ExternalId"] = external_id

    resp = sts.assume_role(**params)
    creds = resp["Credentials"]
    return {
        "access_key": creds["AccessKeyId"],
        "secret_key": creds["SecretAccessKey"],
        "session_token": creds["SessionToken"],
    }


def _build_s3_filesystem_from_creds(
    *,
    access_key: str,
    secret_key: str,
    session_token: str | None,
    region: str | None,
    endpoint_url: str | None = None,
):
    client_kwargs: Dict[str, Any] = {}
    if region:
        client_kwargs["region_name"] = region
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    fs_kwargs: Dict[str, Any] = {
        "key": access_key,
        "secret": secret_key,
        "asynchronous": False,
    }
    if session_token:
        fs_kwargs["token"] = session_token
    if client_kwargs:
        fs_kwargs["client_kwargs"] = client_kwargs

    return fsspec.filesystem("s3", **fs_kwargs)


def _stream_blob_to_s3(
    *,
    segment_blob_client: BlobClient,
    fs: Any,
    s3_path: str,
) -> None:
    downloader = segment_blob_client.download_blob()
    with fs.open(s3_path, mode="wb") as s3_file:
        for chunk in downloader.chunks():
            if chunk:
                s3_file.write(chunk)


def _put_object_acl_bucket_owner_full_control(
    *,
    s3_url: str,
    access_key: str,
    secret_key: str,
    session_token: str | None,
    region: str | None,
):
    bucket, key = _parse_s3_url(s3_url)
    if not key:
        raise ValueError(f"Cannot set ACL on empty key for URL: {s3_url!r}")

    s3 = _build_botocore_client(
        "s3",
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
        region=region,
    )

    s3.put_object_acl(
        Bucket=bucket,
        Key=key,
        ACL="bucket-owner-full-control",
    )


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceFreewheel_initSegmentBlob(ingress: Dict[str, Any]) -> str:
    segment_blob = ingress["segment_blob"]
    blob_client = _build_blob_client(segment_blob)

    blob_client.create_append_blob()
    return blob_client.url


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceFreewheel_generateSegment(ingress: Dict[str, Any]) -> str:
    source = ingress["source"]
    segment_blob = ingress["segment_blob"]
    audience = ingress["audience"]

    segment_raw = audience.get("segment") if isinstance(audience, dict) else None
    if segment_raw is None or str(segment_raw).strip() == "":
        raise ValueError(
            "Missing required audience.segment for Freewheel Buyer Cloud export. "
            f"Got audience keys={list(audience.keys()) if isinstance(audience, dict) else type(audience)}"
        )
    segment_key = f"dsp-{str(segment_raw)}"

    source_blob_client = _build_blob_client(source)
    segment_blob_client = _build_blob_client(segment_blob)

    max_append_block_bytes = _coerce_max_append_block_bytes(
        ingress.get("max_append_block_bytes")
    )

    downloader = source_blob_client.download_blob()
    lines_iter = _iter_text_lines_from_chunks(downloader.chunks())

    first_line: str | None = None
    for line in lines_iter:
        if _clean_token(line):
            first_line = line
            break

    if first_line is None:
        logging.info("Source audience blob is empty; nothing to append.")
        return segment_blob_client.url

    header_mode = _looks_like_header(first_line)
    if header_mode:
        logging.info("Parsing audience source as CSV-with-header mode.")
        csv_iter = itertools.chain([first_line], lines_iter)
        reader = csv.DictReader(csv_iter)

        if not reader.fieldnames:
            raise ValueError("Source audience file has no readable header row.")

        field_map = {
            str(name).casefold(): str(name)
            for name in reader.fieldnames
            if name
        }
        device_key = field_map.get("deviceid") or field_map.get("device_id")
        if not device_key:
            raise ValueError(
                "Source audience file is missing required 'deviceid' or 'device_id' column "
                "needed for Buyer Cloud segment upload."
            )

        buf = bytearray()
        buf_bytes = 0

        def flush() -> None:
            nonlocal buf_bytes
            if not buf:
                return
            segment_blob_client.append_block(bytes(buf))
            buf.clear()
            buf_bytes = 0

        for row in reader:
            device_id = _extract_device_id_from_header_row(row, device_key)
            if not device_id:
                continue

            line_bytes = f"{device_id}|{segment_key}\n".encode("utf-8")
            if buf_bytes + len(line_bytes) > max_append_block_bytes and buf:
                flush()

            buf.extend(line_bytes)
            buf_bytes += len(line_bytes)

        flush()
        return segment_blob_client.url

    logging.info("Parsing audience source as headerless device-id list.")
    line_iter = itertools.chain([first_line], lines_iter)

    buf = bytearray()
    buf_bytes = 0

    def flush() -> None:
        nonlocal buf_bytes
        if not buf:
            return
        segment_blob_client.append_block(bytes(buf))
        buf.clear()
        buf_bytes = 0

    for line in line_iter:
        device_id = _extract_device_id_from_line(line)
        if not device_id:
            continue

        line_bytes = f"{device_id}|{segment_key}\n".encode("utf-8")
        if buf_bytes + len(line_bytes) > max_append_block_bytes and buf:
            flush()

        buf.extend(line_bytes)
        buf_bytes += len(line_bytes)

    flush()
    return segment_blob_client.url


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceFreewheel_uploadSegmentToS3(ingress: Dict[str, Any]) -> str:
    """
    FreeWheel Buyer Cloud requirements (per your instructions):
      1) Assume FreeWheel role: arn:aws:iam::164891057361:role/customer-s3-user-list-dsp-<dsp_account_id>
      2) Upload to s3://beeswax-data-<region>/user-list/dsp/<dsp_account_id>/
      3) Apply ACL bucket-owner-full-control via PutObjectAcl
    """
    segment_blob = ingress["segment_blob"]
    destination: Dict[str, Any] = ingress["destination"]
    delete_after_upload = bool(ingress.get("delete_after_upload", True))

    segment_blob_client = _build_blob_client(segment_blob)
    s3_path = _resolve_s3_path(destination)

    # Base creds: your IAM user/role credentials
    base_access_key = str(destination["access_key"])
    base_secret_key = str(destination["secret_key"])
    base_session_token = (
        str(destination["session_token"])
        if destination.get("session_token")
        else os.getenv("FREEWHEEL_SEGMENTS_AWS_SESSION_TOKEN")
    )

    role_arn = str(destination["role_arn"])
    role_session_name = str(
        destination.get("role_session_name") or f"esq-freewheel-segment-upload-{uuid.uuid4().hex[:12]}"
    )

    region = str(destination.get("region") or os.getenv("FREEWHEEL_SEGMENTS_S3_REGION") or "us-east-1")
    endpoint_url = destination.get("endpoint_url") or os.getenv("FREEWHEEL_SEGMENTS_S3_ENDPOINT_URL")

    external_id = destination.get("external_id") or os.getenv("FREEWHEEL_SEGMENTS_AWS_EXTERNAL_ID")

    # Assume FreeWheel role (required by their bucket policy)
    assumed = _assume_role(
        base_access_key=base_access_key,
        base_secret_key=base_secret_key,
        base_session_token=base_session_token,
        role_arn=role_arn,
        role_session_name=role_session_name,
        region=region,
        external_id=str(external_id) if external_id else None,
    )

    fs = _build_s3_filesystem_from_creds(
        access_key=assumed["access_key"],
        secret_key=assumed["secret_key"],
        session_token=assumed["session_token"],
        region=region,
        endpoint_url=str(endpoint_url) if endpoint_url else None,
    )

    # Upload (no ACL inline). Then apply ACL as separate PutObjectAcl, matching FW instructions.
    try:
        _stream_blob_to_s3(
            segment_blob_client=segment_blob_client,
            fs=fs,
            s3_path=s3_path,
        )
    except PermissionError as exc:
        raise PermissionError(
            "S3 upload failed with AccessDenied even after assuming the FreeWheel role. "
            "Common causes: (1) FreeWheel has not yet added your IAM ARN to the bucket policy, "
            "(2) wrong role ARN (dsp_account_id mismatch), (3) wrong bucket/region for continent, "
            "or (4) base creds cannot sts:AssumeRole for that role."
        ) from exc

    # Set ACL bucket-owner-full-control (required by FreeWheel step 7)
    try:
        _put_object_acl_bucket_owner_full_control(
            s3_url=s3_path,
            access_key=assumed["access_key"],
            secret_key=assumed["secret_key"],
            session_token=assumed["session_token"],
            region=region,
        )
    except PermissionError as exc:
        raise PermissionError(
            "Upload succeeded but PutObjectAcl failed with AccessDenied. "
            "The assumed role likely lacks s3:PutObjectAcl, or the bucket has ACLs disabled. "
            "FreeWheel instructions indicate ACL should be supported; verify bucket/object ownership settings and role permissions."
        ) from exc

    if delete_after_upload:
        try:
            segment_blob_client.delete_blob(delete_snapshots="include")
        except Exception:
            pass

    return s3_path
