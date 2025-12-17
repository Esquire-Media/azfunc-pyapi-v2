from __future__ import annotations

from typing import Any, Dict, List

import logging
import os

import requests
from azure.durable_functions import Blueprint

bp = Blueprint()


class BuzzError(Exception):
    """Raised when a Buzz API call fails."""


def _build_segment_upload_payload(ingress: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the JSON body for Buzz's segment_upload "alternative method", where
    files are already uploaded and we only provide segment_file_list.

    See: https://api-docs.freewheel.tv/advertiser/reference/segment_upload-post
    """
    segment_files: List[str] = ingress["segment_files"]
    if not segment_files:
        raise ValueError("segment_files must contain at least one path.")

    # Required pieces
    account_id_raw: Any = ingress.get("account_id") or os.environ[
        "FREEWHEEL_BUZZ_ACCOUNT_ID"
    ]
    account_id = int(account_id_raw)

    # If user_id_type is not explicitly overridden in the ingress, fall back
    # to FREEWHEEL_BUZZ_USER_ID_TYPE for backward compatibility.
    user_id_type = ingress.get("user_id_type") or os.environ[
        "FREEWHEEL_BUZZ_USER_ID_TYPE"
    ]

    # Sensible defaults that can be overridden via ingress or env
    file_format = (
        ingress.get("file_format")
        or os.getenv("FREEWHEEL_BUZZ_FILE_FORMAT", "DELIMITED")
    )
    segment_key_type = (
        ingress.get("segment_key_type")
        or os.getenv("FREEWHEEL_BUZZ_SEGMENT_KEY_TYPE", "DEFAULT")
    )
    operation_type = ingress.get("operation_type") or os.getenv(
        "FREEWHEEL_BUZZ_OPERATION_TYPE"
    )

    # Optional continent override (e.g. "EMEA", "APAC"); defaults to NAM at the API level.
    continent = ingress.get("continent") or os.getenv("FREEWHEEL_BUZZ_CONTINENT")

    payload: Dict[str, Any] = {
        # Alternative method uses this instead of file_name / size_in_bytes
        "segment_file_list": segment_files,
        "account_id": account_id,
        "file_format": file_format,
        "segment_key_type": segment_key_type,
        "user_id_type": user_id_type,
    }

    if operation_type:
        payload["operation_type"] = operation_type

    if continent:
        payload["continent"] = continent

    return payload


def _buzz_authenticate(session: requests.Session) -> None:
    """
    Authenticate against the Buzz v0.5 API and store the cookie on the session.

    Docs: https://api-docs.freewheel.tv/advertiser/docs/authentication
    """
    base_url = os.environ["FREEWHEEL_BUZZ_BASE_URL"].rstrip("/")
    email = os.environ["FREEWHEEL_BUZZ_EMAIL"]
    password = os.environ["FREEWHEEL_BUZZ_PASSWORD"]

    body = {
        "email": email,
        "password": password,
        # 0.5 auth supports long-lived sessions via keep_logged_in
        "keep_logged_in": True,
    }

    resp = session.post(f"{base_url}/rest/authenticate", json=body, timeout=10)
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        logging.exception("Buzz authentication HTTP error: %s", exc)
        raise BuzzError(f"Buzz authentication HTTP error: {exc}") from exc

    try:
        data = resp.json()
    except ValueError:
        raise BuzzError(
            f"Buzz authentication returned non-JSON body: {resp.text!r}"
        )

    if str(data.get("success")).lower() != "true":
        raise BuzzError(f"Buzz authentication failed: {data}")


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceFreewheel_segmentUpload(
    ingress: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Kick off Buzz v0.5 `segment_upload` processing for one or more pre-uploaded
    files using the "Alternative Method for Segment Upload" (segment_file_list).

    ingress:
        {
            "segment_files": ["s3://...", ...],
            # Optional overrides; will fall back to env vars if omitted:
            # "account_id": 1234,
            # "user_id_type": "AD_ID" | "IDFA" | "OTHER_MOBILE_ID" | ...,
            # "file_format": "DELIMITED",
            # "segment_key_type": "DEFAULT",
            # "operation_type": "ADD",
            # "continent": "EMEA",
        }
    """
    session = requests.Session()
    _buzz_authenticate(session)

    base_url = os.environ["FREEWHEEL_BUZZ_BASE_URL"].rstrip("/")
    payload = _build_segment_upload_payload(ingress)

    logging.info(
        "Calling Buzz segment_upload for account_id=%s with %d files (user_id_type=%s, continent=%s)",
        payload.get("account_id"),
        len(payload.get("segment_file_list") or []),
        payload.get("user_id_type"),
        payload.get("continent"),
    )

    resp = session.post(
        f"{base_url}/rest/segment_upload",
        json=payload,
        timeout=30,
    )

    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        logging.exception("Buzz segment_upload HTTP error: %s", exc)
        raise BuzzError(
            f"Buzz segment_upload HTTP error: {exc} - body={resp.text}"
        ) from exc

    # Buzz typically returns:
    #   {"success": true, "message": "...", "payload": {...}}
    try:
        return resp.json()
    except ValueError:
        # Fallback so we don't lose the response body on weird content-types
        return {"raw": resp.text}
