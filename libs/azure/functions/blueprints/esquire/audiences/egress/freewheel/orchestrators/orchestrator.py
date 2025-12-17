from __future__ import annotations

from typing import Any, Dict, List

import os

from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()

StatusPayload = Dict[str, Any]

_FREEWHEEL_ASSUME_ROLE_ACCOUNT_ID = "164891057361"
_FREEWHEEL_ASSUME_ROLE_NAME_PREFIX = "customer-s3-dsp-user-list-"


def _build_status(
    phase: str,
    audience_id: str | None = None,
    **extra: Any,
) -> StatusPayload:
    payload: StatusPayload = {"phase": phase}
    if audience_id is not None:
        payload["audience_id"] = audience_id
    if extra:
        payload.update(extra)
    return payload


def _set_status(
    context: DurableOrchestrationContext,
    phase: str,
    audience_id: str | None = None,
    **extra: Any,
) -> None:
    context.set_custom_status(_build_status(phase, audience_id, **extra))


def _build_source_blob_spec(
    storage: Dict[str, Any],
    blob_name: str,
) -> Dict[str, str]:
    return {
        "conn_str": storage["conn_str"],
        "container_name": storage["container_name"],
        "blob_name": blob_name,
    }


def _build_segment_blob_spec(
    audience_id: str,
    storage: Dict[str, Any],
    context: DurableOrchestrationContext,
) -> Dict[str, str]:
    unique_suffix = context.new_uuid()
    blob_name = f"tmp/freewheel/segment-{audience_id}-{unique_suffix}.txt"

    return {
        "conn_str": storage["conn_str"],
        "container_name": storage["container_name"],
        "blob_name": blob_name,
    }


def _resolve_buzz_account_id(buzz_cfg: Dict[str, Any]) -> int:
    account_id_raw: Any = buzz_cfg.get("account_id") or os.environ[
        "FREEWHEEL_BUZZ_ACCOUNT_ID"
    ]
    return int(account_id_raw)


def _resolve_segment_upload_user_id_types(buzz_cfg: Dict[str, Any]) -> List[str]:
    explicit = buzz_cfg.get("user_id_type")
    if explicit:
        return [str(explicit)]
    return ["AD_ID", "IDFA"]


def _ensure_audience_has_segment(
    audience_cfg: Dict[str, Any],
    fetched: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(audience_cfg)
    merged.update(fetched)

    seg = merged.get("segment")
    if seg is None or str(seg).strip() == "":
        raise ValueError(
            "Audience metadata is missing required 'segment' (Audience.freewheel). "
            f"merged_keys={list(merged.keys())}"
        )
    return merged


def _resolve_continent(buzz_cfg: Dict[str, Any]) -> str | None:
    # Buzz API uses "continent" field for EMEA uploads; keep it aligned.
    continent = buzz_cfg.get("continent") or os.getenv("FREEWHEEL_BUZZ_CONTINENT")
    if continent is None:
        return None
    c = str(continent).strip().upper()
    return c if c else None


def _resolve_beeswax_bucket_and_region(continent: str | None) -> Dict[str, str]:
    """
    Per FreeWheel instructions:
      - NAM:  beeswax-data-us-east-1 (region us-east-1)
      - EMEA: beeswax-data-eu-west-1 (region eu-west-1)
      - APAC: beeswax-data-ap-northeast-1 (region ap-northeast-1)
    """
    if continent == "EMEA":
        return {"bucket": "beeswax-data-eu-west-1", "region": "eu-west-1"}
    if continent == "APAC":
        return {"bucket": "beeswax-data-ap-northeast-1", "region": "ap-northeast-1"}
    return {"bucket": "beeswax-data-us-east-1", "region": "us-east-1"}


def _build_freewheel_role_arn(dsp_account_id: int) -> str:
    # arn:aws:iam::164891057361:role/customer-s3-user-list-dsp-<dsp_account_id>
    return (
        f"arn:aws:iam::{_FREEWHEEL_ASSUME_ROLE_ACCOUNT_ID}:role/"
        f"{_FREEWHEEL_ASSUME_ROLE_NAME_PREFIX}{dsp_account_id}"
    )


@bp.orchestration_trigger(context_name="context")
def freewheel_segment_orchestrator(
    context: DurableOrchestrationContext,
):
    ingress: Dict[str, Any] = context.get_input() or {}

    audience_cfg: Dict[str, Any] = ingress["audience"]
    audience_id = str(audience_cfg["id"])
    storage_cfg: Dict[str, Any] = ingress["destination"]

    _set_status(context, phase="starting", audience_id=audience_id)

    buzz_cfg: Dict[str, Any] = ingress.get("buzz") or {}

    # Ensure audience has segment (either passed in or fetched)
    try:
        fetched: Dict[str, Any] = yield context.call_activity(
            "activity_esquireAudienceFreewheel_fetchAudience",
            audience_id,
        )
        audience_cfg = _ensure_audience_has_segment(audience_cfg, fetched)
        ingress["audience"] = audience_cfg
    except Exception:
        return {}

    _set_status(
        context,
        phase="discovering_audience_blobs",
        audience_id=audience_id,
        container_name=storage_cfg.get("container_name"),
    )

    blob_names: List[str] = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPaths",
        {
            "conn_str": storage_cfg["conn_str"],
            "container_name": storage_cfg["container_name"],
            "audience_id": audience_id,
        },
    )

    if not blob_names:
        _set_status(context, phase="no_audience_blobs_found", audience_id=audience_id)
        return ingress

    blob_names = list(blob_names)
    total_blobs = len(blob_names)

    _set_status(
        context,
        phase="audience_blobs_discovered",
        audience_id=audience_id,
        blob_count=total_blobs,
    )

    account_id = _resolve_buzz_account_id(buzz_cfg)
    continent = _resolve_continent(buzz_cfg)
    beeswax = _resolve_beeswax_bucket_and_region(continent)

    # Base creds: your IAM user/role that is allowed to sts:AssumeRole into FreeWheel role.
    aws_destination: Dict[str, Any] = {
        "access_key": os.environ["FREEWHEEL_SEGMENTS_AWS_ACCESS_KEY"],
        "secret_key": os.environ["FREEWHEEL_SEGMENTS_AWS_SECRET_KEY"],
        # Optional if using STS source creds:
        # "session_token": os.environ.get("FREEWHEEL_SEGMENTS_AWS_SESSION_TOKEN"),
        "bucket": beeswax["bucket"],
        "region": beeswax["region"],
        "account_id": account_id,
        # Required by FreeWheel instructions:
        "role_arn": _build_freewheel_role_arn(account_id),
        # Optional: external id if FreeWheel requires it in trust policy
        # "external_id": os.environ.get("FREEWHEEL_SEGMENTS_AWS_EXTERNAL_ID"),
    }

    max_append_block_bytes = ingress.get("max_append_block_bytes")
    delete_after_upload = ingress.get("delete_after_upload", True)

    segment_blob_spec = _build_segment_blob_spec(
        audience_id=audience_id,
        storage=storage_cfg,
        context=context,
    )

    _set_status(
        context,
        phase="initializing_segment_blob",
        audience_id=audience_id,
        segment_blob_name=segment_blob_spec["blob_name"],
        blob_count=total_blobs,
    )

    yield context.call_activity(
        "activity_esquireAudienceFreewheel_initSegmentBlob",
        {"segment_blob": segment_blob_spec},
    )

    _set_status(
        context,
        phase="per_blob_pipeline_start",
        audience_id=audience_id,
        blob_count=total_blobs,
        delete_after_upload=delete_after_upload,
        has_max_append_block_bytes=max_append_block_bytes is not None,
        continent=continent,
        s3_bucket=beeswax["bucket"],
        s3_region=beeswax["region"],
        segment_blob_name=segment_blob_spec["blob_name"],
    )

    # Append all source blobs into the single staging append blob
    for blob_index, blob_name in enumerate(blob_names):
        source_blob_spec = _build_source_blob_spec(storage_cfg, blob_name)

        generate_input: Dict[str, Any] = {
            "audience": audience_cfg,
            "source": source_blob_spec,
            "segment_blob": segment_blob_spec,
        }
        if max_append_block_bytes is not None:
            generate_input["max_append_block_bytes"] = max_append_block_bytes

        _set_status(
            context,
            phase="generating_segment",
            audience_id=audience_id,
            blob_index=blob_index,
            blob_total=total_blobs,
            source_blob_name=source_blob_spec["blob_name"],
            segment_blob_name=segment_blob_spec["blob_name"],
        )

        yield context.call_activity(
            "activity_esquireAudienceFreewheel_generateSegment",
            generate_input,
        )

    # Upload the single staged segment file to S3 once
    _set_status(
        context,
        phase="uploading_segment_to_s3",
        audience_id=audience_id,
        segment_blob_name=segment_blob_spec["blob_name"],
    )

    s3_path: str = yield context.call_activity(
        "activity_esquireAudienceFreewheel_uploadSegmentToS3",
        {
            "segment_blob": segment_blob_spec,
            "destination": aws_destination,
            "delete_after_upload": delete_after_upload,
        },
    )

    s3_paths: List[str] = [s3_path]

    _set_status(
        context,
        phase="all_segments_uploaded_to_s3",
        audience_id=audience_id,
        segment_file_count=len(s3_paths),
        continent=continent,
        s3_path=s3_path,
    )

    base_segment_upload_input: Dict[str, Any] = {"segment_files": s3_paths}
    base_segment_upload_input.update(buzz_cfg)

    user_id_types: List[str] = _resolve_segment_upload_user_id_types(buzz_cfg)
    segment_upload_results: Dict[str, Any] = {}

    _set_status(
        context,
        phase="segment_upload_preparing",
        audience_id=audience_id,
        user_id_types=user_id_types,
        segment_file_count=len(s3_paths),
        continent=continent,
    )

    for user_id_type_index, user_id_type in enumerate(user_id_types):
        call_input = dict(base_segment_upload_input)
        call_input["user_id_type"] = user_id_type

        _set_status(
            context,
            phase="segment_upload_calling",
            audience_id=audience_id,
            user_id_type=user_id_type,
            user_id_type_index=user_id_type_index,
            user_id_type_total=len(user_id_types),
        )

        result: Dict[str, Any] = yield context.call_activity(
            "activity_esquireAudienceFreewheel_segmentUpload",
            call_input,
        )
        segment_upload_results[user_id_type] = result

        _set_status(
            context,
            phase="segment_upload_completed_for_user_id_type",
            audience_id=audience_id,
            user_id_type=user_id_type,
            user_id_type_index=user_id_type_index,
            user_id_type_total=len(user_id_types),
        )

    ingress["segment_upload"] = {
        "user_id_types": user_id_types,
        "results": segment_upload_results,
    }

    _set_status(
        context,
        phase="completed",
        audience_id=audience_id,
        user_id_types=user_id_types,
        segment_file_count=len(s3_paths),
        continent=continent,
    )

    return ingress
