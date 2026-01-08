# File: libs/azure/functions/blueprints/esquire/audiences/steps/friendsAndFamily.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import hashlib
from datetime import timedelta

bp = Blueprint()

MAX_CONCURRENT_TASKS = 20


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2friendsandfamily_deviceids(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    retry = RetryOptions(first_retry_interval_in_milliseconds=2000, max_number_of_attempts=3)

    # only knobs we support
    thresholds = {
        "min_count": int(ingress.get("process", {}).get("min_count", 2)),
        "top_n": ingress.get("process", {}).get("top_n", None),
    }

    # get 30 day window for checking device count
    end_dt = context.current_utc_datetime - timedelta(days=6)
    start_dt = end_dt - timedelta(days=30)
    date_end = end_dt.strftime("%Y-%m-%d")
    date_start = start_dt.strftime("%Y-%m-%d")

    results = []

    for source_url in ingress["source_urls"]:
        source_key = _stable_source_key(source_url)

        # -1: chunk addresses (writes blobs + returns SAS urls)
        chunk_urls = yield context.call_activity(
            "activity_faf_chunk_sales_latlon_csv",
            {
                "source_url": source_url,
                "destination": {
                    **ingress["working"],
                    "blob_prefix": f"{ingress['working']['blob_prefix']}/-1/{source_key}",
                },
                "rows_per_chunk": int(ingress.get("process", {}).get("chunkRows", 500)),
                "required_fields": ["latitude", "longitude"],
            },
        )
        if not chunk_urls:
            continue

        # -1: autopoly chunk -> writes FeatureCollection json + returns SAS url
        poly_tasks = [
            context.call_activity(
                "activity_faf_autopoly_chunk_to_fc_url",
                {
                    "chunk_url": cu,
                    "working": ingress["working"],
                    "output_prefix": f"{ingress['working']['blob_prefix']}/-1/{source_key}/polys",
                    "fallback_buffer_m": int(ingress.get("process", {}).get("fallbackBufferM", 20)),
                    "osm": ingress.get("process", {}).get("osm", {}),
                    "date_end": date_end,
                    "date_start":date_start
                },
            )
            for cu in chunk_urls
        ]

        # 0: submit OnSpot per FC URL (URL-only request via sources[])
        for poly_batch in chunked(poly_tasks, MAX_CONCURRENT_TASKS):
            fc_urls = yield context.task_all(poly_batch)

            # load features per fc url (fan-out)
            load_tasks = [
                context.call_activity(
                    "activity_load_features_from_url", 
                    {
                        "source_url": fc_url
                        })
                for fc_url in fc_urls
                if fc_url
            ]
            payloads = yield context.task_all(load_tasks)

            # submit to onspot (fan-out) immediately; don't store payloads longer than this batch
            onspot_tasks = [
                context.call_sub_orchestrator_with_retry(
                    "onspot_orchestrator",
                    retry,
                    {
                        "endpoint": "/save/geoframe/all/countgroupedbydevice",
                        "request": {
                            "type": "FeatureCollection",
                            "features": payload.get("features")
                        },
                        "conn_str": ingress["working"]["conn_str"],
                        "container_name": ingress["working"]["container_name"],
                        "blob_prefix": f"{ingress['working']['blob_prefix']}/0/{source_key}",
                    },
                )
                for payload in payloads
                if payload and payload.get("features")
            ]

            if onspot_tasks:
                count_results = yield context.task_all(onspot_tasks)

                # Collect URLs of the demographic results
            count_urls = []
            for result in count_results:
                job_location_map = {
                    job["id"]: job["location"].replace("az://", "https://")
                    for job in result["jobs"]
                }
                for callback in result["callbacks"]:
                    if callback["success"]:
                        if callback["id"] in job_location_map:
                            count_urls.append(job_location_map[callback["id"]])

        # 1: filter -> destination/1/{source_key}.csv
        filtered_urls = yield context.task_all([
                context.call_activity(
                    "activity_faf_filter_countgroupedbydevice_to_deviceids",
                    {
                        "source_url": count_url,
                        "destination": {
                            **ingress["destination"],
                            "blob_prefix": f"{ingress['destination']['blob_prefix']}/1",
                        },
                        "output_name": f"{source_key}.csv",
                        "thresholds": thresholds,
                    },
                )
         for count_url in count_urls])


        if filtered_urls:
            results = results + filtered_urls

    return results


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _stable_source_key(source_url: str) -> str:
    return hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:16]
