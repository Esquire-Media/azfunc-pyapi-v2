# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/eternal.py

import os
from azure.durable_functions import Blueprint, DurableOrchestrationContext
from croniter import croniter
import datetime, orjson as json, pytz, hashlib, re, html
from typing import Any, Dict, List, Optional, Tuple

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


# ---------------------------
# Error formatting utilities
# ---------------------------


class _ParsedOrchError:
    def __init__(self, name: str, message: Optional[str], stack: List[str]):
        self.name = name
        self.message = (message or "").strip() or None
        self.stack = stack


def _parse_durable_error_chain(err_text: str) -> List[_ParsedOrchError]:
    """
    Best-effort parser for Durable orchestrator exception strings like:
      Orchestrator function 'A' failed: Orchestrator function 'B' failed: None
      Message: None, StackTrace: at ...
      Message: Orchestrator function 'B' failed: None Message: None, StackTrace: at ...
      , StackTrace: at ...

    Strategy:
    - Split on "Orchestrator function '<name>' failed:" boundaries to reveal the nested chain.
    - For each segment, pull optional 'Message:' and 'StackTrace:'.
    - Normalize + de-duplicate stack frames while preserving order.
    """
    if not err_text:
        return []

    parts = re.split(r"(?=Orchestrator function ')", err_text)
    results: List[_ParsedOrchError] = []

    for part in parts:
        m_name = re.search(r"Orchestrator function '([^']+)'", part)
        if not m_name:
            # If no orchestrator header, try to salvage as the leaf error
            # Treat it as an anonymous step with whatever message/stack we can find.
            name = "orchestrator"
        else:
            name = m_name.group(1)

        msg = None
        stack_text = ""

        # Grab the last explicit Message: ... before a StackTrace:, if present
        m_msg = re.search(
            r"Message:\s*(.*?)(?:,\s*StackTrace:|$)", part, flags=re.DOTALL
        )
        if m_msg:
            msg = m_msg.group(1)

        # Prefer the last StackTrace: section in this piece (closest to the leaf)
        stack_candidates = re.findall(r"StackTrace:\s*(.*)", part, flags=re.DOTALL)
        if stack_candidates:
            stack_text = stack_candidates[-1]

        # Split stack into lines, strip noise, de-dup preserving order
        raw_lines = [ln.strip() for ln in stack_text.splitlines() if ln.strip()]
        seen = set()
        stack_lines: List[str] = []
        for ln in raw_lines:
            if ln in seen:
                continue
            seen.add(ln)
            stack_lines.append(ln)

        results.append(_ParsedOrchError(name=name, message=msg, stack=stack_lines))

    # If we didn't find any orchestrator markers at all, return a single leaf with raw text
    if not results:
        return [_ParsedOrchError(name="orchestrator", message=err_text, stack=[])]

    return results


def _ellipsize_lines(lines: List[str], max_lines: int = 25) -> List[str]:
    if len(lines) <= max_lines:
        return lines
    head = lines[: max_lines - 1]
    return head + ["… ({} more lines)".format(len(lines) - (max_lines - 1))]


def _format_error_html(
    *,
    instance_id: str,
    audience_id: Optional[str],
    exception: Exception,
    now_utc: datetime.datetime,
) -> Tuple[str, str]:
    """
    Returns (subject_suffix, html_body)
    """
    err_text = str(exception) or repr(exception)
    err_type = type(exception).__name__
    chain = _parse_durable_error_chain(err_text)

    # Build a breadcrumb of failing orchestrators (outer → inner)
    breadcrumb = " → ".join([c.name for c in chain if c.name])

    # Build sections per orchestrator with message + stack
    sections_html: List[str] = []
    for c in chain:
        msg_html = (
            f"<div><code>{html.escape(c.message)}</code></div>"
            if c.message and c.message.lower() != "none"
            else "<div><em>No message</em></div>"
        )
        stack_lines = _ellipsize_lines(c.stack, max_lines=25)
        stack_html = (
            "<pre style='margin:8px 0;padding:8px;border:1px solid #eee;"
            "border-radius:6px;overflow:auto;font-size:12px;'>"
            + html.escape("\n".join(stack_lines))
            + "</pre>"
            if stack_lines
            else "<div><em>No stack trace</em></div>"
        )
        sections_html.append(
            f"""
            <details style="margin:10px 0;" open>
              <summary style="cursor: pointer; font-weight:600;">
                Orchestrator: <code>{html.escape(c.name)}</code>
              </summary>
              {msg_html}
              {stack_html}
            </details>
            """
        )

    raw_block = (
        "<details style='margin-top:14px;'>"
        "<summary style='cursor:pointer;font-weight:600;'>Raw exception</summary>"
        "<pre style='margin:8px 0;padding:8px;border:1px solid #eee;border-radius:6px;overflow:auto;font-size:12px;'>"
        + html.escape(err_text)
        + "</pre></details>"
    )

    header_rows = [
        ("Status", "<strong style='color:#b00020;'>Error</strong>"),
        ("When (UTC)", html.escape(_safe_iso(now_utc))),
        ("Instance ID", f"<code>{html.escape(instance_id)}</code>"),
        (
            "Audience ID",
            (
                f"<code>{html.escape(str(audience_id))}</code>"
                if audience_id
                else "<em>unknown</em>"
            ),
        ),
        ("Exception Type", f"<code>{html.escape(err_type)}</code>"),
        ("Orchestrator Chain", f"<code>{html.escape(breadcrumb or 'n/a')}</code>"),
    ]

    info_table = (
        "<table cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-size:14px;'>"
        + "".join(
            f"<tr>"
            f"<td style='border:1px solid #eee;background:#fafafa;font-weight:600;'>{k}</td>"
            f"<td style='border:1px solid #eee;'>{v}</td>"
            f"</tr>"
            for k, v in header_rows
        )
        + "</table>"
    )

    body = f"""
    <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,'Helvetica Neue',Arial,'Noto Sans',sans-serif; line-height:1.45; color:#111;">
      <h2 style="margin:0 0 8px;">esquire-auto-audience failure</h2>
      <p style="margin:0 0 12px;">A Durable Functions orchestrator run failed.</p>
      {info_table}
      <h3 style="margin:16px 0 6px;">Details</h3>
      {''.join(sections_html)}
      {raw_block}
    </div>
    """.strip()

    subject_suffix = f"instance {instance_id}"
    if audience_id:
        subject_suffix += f" | audience {audience_id}"
    return subject_suffix, body


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

    # NEW: Exit gracefully if the audience is disabled
    if not bool((ingress.get("audience") or {}).get("status", False)):
        context.set_custom_status(
            {
                "state": "Disabled",
                "previous_runs": history,
                "audience_id": (ingress.get("audience") or {}).get("id"),
                "enabled": False,
            }
        )
        return ""

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
            ran_at = datetime.datetime.fromisoformat(
                audience_blob_prefix.split("/")[-1]
            )
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
    if (now >= next_run and ingress["audience"]["status"]) or ingress.get(
        "forceRebuild"
    ):
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
                    "expected": (
                        len(build.get("results") or [])
                        if isinstance(build.get("results"), list)
                        else None
                    ),
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
                        croniter(ingress["audience"]["rebuildSchedule"], now).get_next(
                            datetime.datetime
                        )
                    ),
                    "previous_runs": history,
                    "audience_id": build["audience"]["id"],
                }
            )

        except Exception as e:
            # Record into history
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
            to = [t for t in os.getenv("NOTIFICATION_RECIPIENTS", "").split(";") if t]
            if len(to):
                # -------- Enhanced email contents --------
                audience_id = (ingress.get("audience") or {}).get("id")
                subject_suffix, html_body = _format_error_html(
                    instance_id=context.instance_id,
                    audience_id=audience_id,
                    exception=e,
                    now_utc=now,
                )
                subject = f"esquire-auto-audience Failure — {subject_suffix}"

                # Existing notification behavior (HTML, with instance id and formatted error)
                yield context.call_activity(
                    "activity_microsoftGraph_sendEmail",
                    {
                        "from_id": "74891a5a-d0e9-43a4-a7c1-a9c04f6483c8",
                        "to_addresses": to,
                        "subject": subject,
                        "message": html_body,
                        "content_type": "html",
                    },
                )
            # Re-raise to preserve Durable semantics / retries / diagnostics
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
