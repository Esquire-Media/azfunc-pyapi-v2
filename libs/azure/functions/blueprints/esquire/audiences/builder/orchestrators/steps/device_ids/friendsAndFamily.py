from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from urllib.parse import urlparse
import hashlib

bp = Blueprint()

MAX_CONCURRENT_TASKS = 20


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2friendsandfamily_deviceids(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    retry = RetryOptions(first_retry_interval_in_milliseconds=2000, max_number_of_attempts=3)

    results = []

    for source_url in ingress["source_urls"]:
        # stable per-source key => deterministic blob paths
        source_key = _stable_source_key(source_url)

        # 1) split source CSV into working/-1 chunks
        chunk_urls = yield context.call_activity(
            "activity_split_addresses_latlon_csv",
            {
                "source_url": source_url,
                "destination": {
                    **ingress["working"],
                    "blob_prefix": f"{ingress['working']['blob_prefix']}/-1/{source_key}",
                },
                "rows_per_chunk": ingress["process"].get("chunkRows", 500),
                "required_fields": ["latitude", "longitude"],
            },
        )
        if not chunk_urls:
            continue

        # 2) autopoly each chunk -> FeatureCollection (bounded per chunk)
        poly_tasks = [
            context.call_activity(
                "activity_faf_autopoly_from_chunk",
                {
                    "chunk_url": cu,
                    "fallback_buffer_m": ingress["process"].get("fallbackBufferM", 20),
                    "osm": ingress["process"].get("osm", {}),
                },
            )
            for cu in chunk_urls
        ]

        for poly_batch in chunked(poly_tasks, MAX_CONCURRENT_TASKS):
            fcs = yield context.task_all(poly_batch)

            onspot_tasks = [
                context.call_sub_orchestrator_with_retry(
                    "onspot_orchestrator",
                    retry,
                    {
                        "endpoint": "/save/geoframe/all/devices",
                        "request": fc,
                        "conn_str": ingress["working"]["conn_str"],
                        "container_name": ingress["working"]["container_name"],
                        "blob_prefix": f"{ingress['working']['blob_prefix']}/0/{source_key}",
                    },
                )
                for fc in fcs
                if fc and fc.get("type") == "FeatureCollection" and fc.get("features")
            ]

            if onspot_tasks:
                yield context.task_all(onspot_tasks)


        # 3) merge all onspot output blobs under working/0/{source_key}
        merged_url = yield context.call_activity(
            "activity_onSpot_mergeDevices",
            {
                "source": {
                    "conn_str": ingress["working"]["conn_str"],
                    "container_name": ingress["working"]["container_name"],
                    "blob_prefix": f"{ingress['working']['blob_prefix']}/0/{source_key}",
                },
                "destination": {
                    "conn_str": ingress["working"]["conn_str"],
                    "container_name": ingress["working"]["container_name"],
                    "blob_name": f"{ingress['working']['blob_prefix']}/0/{source_key}/merged.csv",
                },
                "header": ingress["process"].get("onspotHeader", ["deviceid"]),
            },
        )

        # 4) filter -> destination/1/{source_key}.csv (deviceid-only)
        filtered_url = yield context.call_activity(
            "activity_faf_filter_devices_blob",
            {
                "source_url": merged_url,
                "destination": {
                    **ingress["destination"],
                    "blob_prefix": f"{ingress['destination']['blob_prefix']}/1",
                },
                "output_name": f"{source_key}.csv",
                "thresholds": ingress["process"].get("thresholds", {}),
            },
        )

        if filtered_url:
            results.append(filtered_url)

    return results


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _stable_source_key(source_url: str) -> str:
    # stable across retries/replays
    h = hashlib.sha1(source_url.encode("utf-8")).hexdigest()
    return h[:16]
