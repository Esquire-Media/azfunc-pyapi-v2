# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/eternal.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
from croniter import croniter
import datetime, orjson as json, pytz, hashlib
from typing import Any, Dict, List, Optional

bp = Blueprint()

HISTORY_MAX = 5  # keep the last N runs


def _ensure_utc(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(pytz.UTC)


def _safe_iso(dt: datetime.datetime) -> str:
    return _ensure_utc(dt).isoformat()


def _hash_query(q: Optional[str]) -> Optional[str]:
    if not q:
        return None
    try:
        return hashlib.sha256(q.encode("utf-8")).hexdigest()[:12]
    except Exception:
        return None


def _dedupe_and_trim_history(
    existing: List[Dict[str, Any]], new_entry: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Keep at most HISTORY_MAX entries, newest last. Deduplicate by (prefix|ran_at).
    """
    key = (new_entry.get("prefix") or "") + "|" + (new_entry.get("ran_at") or "")
    filtered: List[Dict[str, Any]] = []
    seen: set = set()
    for item in existing + [new_entry]:
        k = (item.get("prefix") or "") + "|" + (item.get("ran_at") or "")
        if k in seen:
            continue
        seen.add(k)
        filtered.append(item)
    try:
        filtered.sort(key=lambda x: x.get("ran_at") or "")
    except Exception:
        pass
    if len(filtered) > HISTORY_MAX:
        filtered = filtered[-HISTORY_MAX:]
    return filtered


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquire_audience(context: DurableOrchestrationContext):
    """
    Eternal orchestrator that:
      - Determines if it's time to (re)build.
      - Builds + uploads when due (or forced).
      - Publishes a rich custom_status including previous run summaries.
      - Sleeps until the next tick or an external 'restart' event, then continue_as_new.
    """
    ingress = context.get_input() or {}
    history: List[Dict[str, Any]] = list(ingress.get("history") or [])

    # Fetch audience definition from DB
    ingress["audience"] = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudience",
        {"id": context.instance_id},
    )

    # Determine the last run time from storage layout (prefix ends in ISO timestamp)
    audience_blob_prefix = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPrefix",
        {
            "conn_str": ingress["destination"]["conn_str"],
            "container_name": ingress["destination"]["container_name"],
            "audience_id": ingress["audience"]["id"],
        },
    )
    last_run_time = (
        datetime.datetime.fromisoformat(audience_blob_prefix.split("/")[-1])
        if audience_blob_prefix
        else datetime.datetime(year=1970, month=1, day=1)
    )
    last_run_time = _ensure_utc(last_run_time)

    # Compute schedule windows
    next_run = (
        croniter(ingress["audience"]["rebuildSchedule"], last_run_time)
        .get_next(datetime.datetime)
        .replace(tzinfo=pytz.UTC)
    )
    now = _ensure_utc(context.current_utc_datetime)
    next_from_now = (
        croniter(ingress["audience"]["rebuildSchedule"], now)
        .get_next(datetime.datetime)
        .replace(tzinfo=pytz.UTC)
    )

    # Build a best-effort summary of the *latest completed* run
    latest_summary: Optional[Dict[str, Any]] = None
    if audience_blob_prefix:
        # ran_at from the last path segment
        try:
            ran_at = datetime.datetime.fromisoformat(audience_blob_prefix.split("/")[-1])
        except Exception:
            ran_at = datetime.datetime(1970, 1, 1)
        ran_at_iso = _safe_iso(ran_at)

        # Count result files in newest folder
        paths = yield context.call_activity(
            "activity_esquireAudiencesUtils_newestAudienceBlobPaths",
            {
                "conn_str": ingress["destination"]["conn_str"],
                "container_name": ingress["destination"]["container_name"],
                "audience_id": ingress["audience"]["id"],
            },
        )
        file_count = len(paths) if isinstance(paths, list) else None

        # Count distinct MAIDs in that folder (Synapse OPENROWSET)
        device_count = None
        try:
            count_resp = yield context.call_activity(
                "activity_synapse_query",
                {
                    "bind": "audiences",
                    "query": """
                        SELECT COUNT(DISTINCT deviceid) AS [count]
                        FROM OPENROWSET(
                            BULK '{}/{}/*',
                            DATA_SOURCE = '{}',
                            FORMAT = 'CSV',
                            PARSER_VERSION = '2.0',
                            HEADER_ROW = TRUE
                        ) AS [data]
                        WHERE LEN(deviceid) = 36
                    """.format(
                        ingress["destination"]["container_name"],
                        audience_blob_prefix,
                        ingress["destination"]["data_source"],
                    ),
                },
            )
            if isinstance(count_resp, list) and count_resp:
                device_count = count_resp[0].get("count")
        except Exception:
            device_count = None

        latest_summary = {
            "ran_at": ran_at_iso,
            "prefix": audience_blob_prefix,
            "file_count": file_count,
            "device_count": device_count,
        }

    if latest_summary:
        history = _dedupe_and_trim_history(history, latest_summary)

    # Pre-run status
    context.set_custom_status(
        {
            "state": "Idle",
            "next_run": _safe_iso(next_run),
            "next_from_now": _safe_iso(next_from_now),
            "previous_runs": history,
            "audience_id": ingress["audience"]["id"],
            "schedule": ingress["audience"]["rebuildSchedule"],
            "enabled": bool(ingress["audience"]["status"]),
        }
    )

    # Build + upload if due (or forced)
    if (now >= next_run and ingress["audience"]["status"]) or ingress.get("forceRebuild"):
        try:
            context.set_custom_status(
                {
                    "state": "Building audience...",
                    "previous_runs": history,
                    "audience_id": ingress["audience"]["id"],
                }
            )

            build = yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiences_builder",
                ingress,
            )

            advertiser = (build.get("audience", {}) or {}).get("advertiser", {}) or {}
            targets = {"meta": advertiser.get("meta"), "xandr": advertiser.get("xandr")}

            context.set_custom_status(
                {
                    "state": "Uploading audience...",
                    "previous_runs": history,
                    "targets": targets,
                    "audience_id": ingress["audience"]["id"],
                }
            )

            yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiences_uploader",
                build,
            )

            # Summarize the newly produced run (from storage)
            post_prefix = yield context.call_activity(
                "activity_esquireAudiencesUtils_newestAudienceBlobPrefix",
                {
                    "conn_str": build["destination"]["conn_str"],
                    "container_name": build["destination"]["container_name"],
                    "audience_id": build["audience"]["id"],
                },
            )

            # Try to count files quickly
            post_paths = yield context.call_activity(
                "activity_esquireAudiencesUtils_newestAudienceBlobPaths",
                {
                    "conn_str": build["destination"]["conn_str"],
                    "container_name": build["destination"]["container_name"],
                    "audience_id": build["audience"]["id"],
                },
            )
            post_file_count = len(post_paths) if isinstance(post_paths, list) else None

            post_summary = {
                "ran_at": _safe_iso(now),
                "prefix": post_prefix,
                "file_count": {
                    "actual": post_file_count,
                    "expected": len(build.get("results") or []) if isinstance(build.get("results"), list) else None,
                },
                "device_count": (build.get("audience", {}) or {}).get("count"),
                "targets": targets,
                "query_hash": _hash_query(build.get("query")),
            }
            history = _dedupe_and_trim_history(history, post_summary)

            # Completed status (and when we'll run next from now)
            context.set_custom_status(
                {
                    "state": "Completed",
                    "completed_at": _safe_iso(now),
                    "next_run": _safe_iso(
                        croniter(ingress["audience"]["rebuildSchedule"], now).get_next(datetime.datetime)
                    ),
                    "previous_runs": history,
                    "audience_id": build["audience"]["id"],
                }
            )

        except Exception as e:
            history = _dedupe_and_trim_history(
                history,
                {
                    "ran_at": _safe_iso(now),
                    "error": str(e),
                    "prefix": None,
                    "file_count": None,
                    "device_count": None,
                },
            )
            context.set_custom_status(
                {
                    "state": "Error",
                    "error": str(e),
                    "previous_runs": history,
                    "audience_id": ingress.get("audience", {}).get("id"),
                }
            )

            # Existing notification behavior
            yield context.call_activity(
                "activity_microsoftGraph_sendEmail",
                {
                    "from_id": "74891a5a-d0e9-43a4-a7c1-a9c04f6483c8",
                    "to_addresses": ["isaac@esquireadvertising.com"],
                    "subject": "esquire-auto-audience Failure",
                    "message": str(e),
                    "content_type": "html",
                },
            )
            raise e

    # Publish the next daily tick and sleep
    next_tick = croniter("0 0 * * *", now).get_next(datetime.datetime)
    context.set_custom_status(
        {
            "state": "Sleeping",
            "next_run": _safe_iso(next_tick),
            "previous_runs": history,
            "audience_id": ingress["audience"]["id"],
        }
    )

    timer_task = context.create_timer(next_tick)
    external_event_task = context.wait_for_external_event("restart")
    winner = yield context.task_any([timer_task, external_event_task])
    if winner == external_event_task:
        timer_task.cancel()
        settings = json.loads(winner.result)
        if "history" not in settings:
            settings["history"] = history
    else:
        settings: dict = context.get_input() or {}
        settings.pop("forceRebuild", None)
        settings["history"] = history

    context.continue_as_new(settings)
    return ""
