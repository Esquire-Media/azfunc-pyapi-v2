from __future__ import annotations

from typing import Any, Dict, List

import os

from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()


def _build_source_blob_spec(
    storage: Dict[str, Any],
    blob_name: str,
) -> Dict[str, str]:
    """
    Build a standard Azure Blob spec dict for a source CSV blob.
    """
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
    """
    Build a deterministic (replay-safe) append-blob spec for the segment file.

    Uses context.new_uuid() to generate a stable identifier across replays,
    which is required for Durable Functions orchestrator code. 
    """
    # new_uuid() is replay-safe and returns a string. 
    unique_suffix = context.new_uuid()
    blob_name = f"tmp/freewheel/segment-{audience_id}-{unique_suffix}.txt"

    return {
        "conn_str": storage["conn_str"],
        "container_name": storage["container_name"],
        "blob_name": blob_name,
    }


def _resolve_buzz_account_id(buzz_cfg: Dict[str, Any]) -> int:
    """
    Resolve the Buyer Cloud account_id for both:
      - S3 path construction (user-list/dsp/<account_id>/...)
      - segment_upload payload.

    Priority:
      1) buzz_cfg["account_id"] if present
      2) FREEWHEEL_BUZZ_ACCOUNT_ID env var
    """
    account_id_raw: Any = buzz_cfg.get("account_id") or os.environ[
        "FREEWHEEL_BUZZ_ACCOUNT_ID"
    ]
    return int(account_id_raw)


def _resolve_segment_upload_user_id_types(buzz_cfg: Dict[str, Any]) -> List[str]:
    """
    Determine which user_id_type(s) to use for segment_upload.

    If buzz_cfg explicitly specifies a user_id_type, we honor that and only
    call segment_upload once (backwards compatible behavior).

    Otherwise, when the device IDs could be either AD_ID or IDFA, we call
    segment_upload twice, once for each user_id_type, reusing the same file(s).
    """
    explicit = buzz_cfg.get("user_id_type")
    if explicit:
        return [str(explicit)]

    # Unknown if the device IDs are IDFA or AD_ID -> treat as both.
    return ["AD_ID", "IDFA"]


@bp.orchestration_trigger(context_name="context")
def freewheel_segment_orchestrator(
    context: DurableOrchestrationContext,
):
    """
    Orchestrator for exporting an ESQ audience to Buyer Cloud via S3 segment upload.

    Expected ingress:
        {
            "audience": {
                "id": "<ESQ audience id>",
                "segment": "<segment key>",
                "expiration": <int> | None,
                ...
            },
            "destination": {
                # Azure storage location of the *audience* CSV blobs
                "conn_str": "<azure-blob-conn-str-name>",
                "container_name": "<azure-blob-container>",
            },
            # Optional pass-through overrides for Buzz:
            # "buzz": {
            #   "account_id": 1234,
            #   "user_id_type": "AD_ID" | "IDFA" | "OTHER_MOBILE_ID",
            #   "file_format": "DELIMITED",
            #   "segment_key_type": "DEFAULT",
            #   "operation_type": "ADD",
            #   "continent": "EMEA",
            # },
            # Optional knobs:
            # "max_append_block_bytes": 4194304,
            # "delete_after_upload": true,
        }

    Overall flow per audience:
      1. Discover newest audience CSV blobs for this audience.
      2. For each blob:
         a) Create an Azure Append Blob for the segment file (.txt).
         b) Convert CSV -> segment lines and append to that blob.
         c) Stream that append blob to S3 and delete it (optional).
      3. Call Buzz segment_upload with the resulting S3 paths.

    Device ID type handling:
      - If buzz.user_id_type is provided, call segment_upload once using that.
      - If buzz.user_id_type is NOT provided, call segment_upload twice:
          user_id_type="AD_ID" and user_id_type="IDFA",
        reusing the same segment_file_list so that mixed device IDs are
        treated as both.
    """
    ingress: Dict[str, Any] = context.get_input() or {}

    audience_cfg: Dict[str, Any] = ingress["audience"]
    audience_id = str(audience_cfg["id"])
    storage_cfg: Dict[str, Any] = ingress["destination"]

    # Buzz overrides (account_id, user_id_type, continent, etc.)
    buzz_cfg: Dict[str, Any] = ingress.get("buzz") or {}

    # 1) Find the latest audience blobs for this audience
    blob_names: List[str] = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPaths",
        {
            "conn_str": storage_cfg["conn_str"],
            "container_name": storage_cfg["container_name"],
            "audience_id": audience_id,
        },
    )

    if not blob_names:
        # Nothing to export -> nothing to send to Buzz
        return ingress

    # Normalize to a list to avoid surprises if the activity returns another sequence type.
    blob_names = list(blob_names)

    # Resolve Buyer Cloud account_id once for S3 path and segment_upload
    account_id = _resolve_buzz_account_id(buzz_cfg)

    # S3 destination config is shared for all segment files in this run.
    aws_destination: Dict[str, Any] = {
        "access_key": os.environ["FREEWHEEL_SEGMENTS_AWS_ACCESS_KEY"],
        "secret_key": os.environ["FREEWHEEL_SEGMENTS_AWS_SECRET_KEY"],
        "bucket": os.environ["FREEWHEEL_SEGMENTS_S3_BUCKET"],
        # Used by _resolve_s3_path for default key:
        #   user-list/dsp/<account_id>/segment-<uuid>.txt
        "account_id": account_id,
    }

    max_append_block_bytes = ingress.get("max_append_block_bytes")
    delete_after_upload = ingress.get("delete_after_upload", True)

    s3_paths: List[str] = []

    # 2) Per-blob pipeline: init append blob -> generate segment -> upload to S3
    for blob_name in blob_names:
        source_blob_spec = _build_source_blob_spec(storage_cfg, blob_name)
        segment_blob_spec = _build_segment_blob_spec(
            audience_id=audience_id,
            storage=storage_cfg,
            context=context,
        )

        # 2a) Initialize the append blob
        yield context.call_activity(
            "activity_esquireAudienceFreewheel_initSegmentBlob",
            {
                "segment_blob": segment_blob_spec,
            },
        )

        # 2b) Generate segment lines into that append blob
        generate_input: Dict[str, Any] = {
            "audience": audience_cfg,
            "source": source_blob_spec,
            "segment_blob": segment_blob_spec,
        }
        if max_append_block_bytes is not None:
            generate_input["max_append_block_bytes"] = max_append_block_bytes

        yield context.call_activity(
            "activity_esquireAudienceFreewheel_generateSegment",
            generate_input,
        )

        # 2c) Upload the append blob to S3 and (optionally) delete it
        s3_path: str = yield context.call_activity(
            "activity_esquireAudienceFreewheel_uploadSegmentToS3",
            {
                "segment_blob": segment_blob_spec,
                "destination": aws_destination,
                "delete_after_upload": delete_after_upload,
            },
        )
        s3_paths.append(s3_path)

    # 3) Notify Buzz that the files are ready for processing via the segment_upload endpoint.
    # Base payload shared across user_id_type variants.
    base_segment_upload_input: Dict[str, Any] = {"segment_files": s3_paths}
    base_segment_upload_input.update(buzz_cfg)

    user_id_types: List[str] = _resolve_segment_upload_user_id_types(buzz_cfg)
    segment_upload_results: Dict[str, Any] = {}

    for user_id_type in user_id_types:
        call_input = dict(base_segment_upload_input)
        call_input["user_id_type"] = user_id_type

        result: Dict[str, Any] = yield context.call_activity(
            "activity_esquireAudienceFreewheel_segmentUpload",
            call_input,
        )
        segment_upload_results[user_id_type] = result

    # Attach result for logging / debugging by upstream callers.
    # Structured by user_id_type to make the dual-upload behavior explicit.
    ingress["segment_upload"] = {
        "user_id_types": user_id_types,
        "results": segment_upload_results,
    }

    return ingress
