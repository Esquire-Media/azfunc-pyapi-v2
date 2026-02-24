# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/addresses/friends_and_family.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import hashlib

bp = Blueprint()

MAX_CONCURRENT = 20


def _stable_source_key(source_url: str) -> str:
    return hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:16]


def _chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _stable_sorted_urls(urls):
    return sorted([u for u in urls if u])


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2friendsandfamily_deviceids(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    retry = RetryOptions(first_retry_interval_in_milliseconds=2000, max_number_of_attempts=3)

    thresholds = {
        "min_count": int(ingress.get("process", {}).get("min_count", 2)),
        "top_n": ingress.get("process", {}).get("top_n", None),
    }

    results = []

    for source_url in ingress["source_urls"]:
        source_key = _stable_source_key(source_url)

        # -1) chunk sales into chunk blobs
        chunk_urls = yield context.call_activity(
            "activity_faf_chunk_sales_latlon_csv",
            {
                "source_url": source_url,
                "destination": {
                    **ingress["working"],
                    "blob_prefix": f"{ingress['working']['blob_prefix']}/-1/{source_key}",
                },
                "rows_per_chunk": int(ingress.get("process", {}).get("chunkRows", 200)),
                "required_fields": ["latitude", "longitude"],
            },
        )
        if not chunk_urls:
            continue

        # -1) autopoly each chunk -> polygon CSV urls
        poly_tasks = [
            context.call_activity(
                "activity_faf_autopoly_chunk_to_polygon_csv",
                {
                    "chunk_url": cu,
                    "working": ingress["working"],
                    "output_prefix": f"{ingress['working']['blob_prefix']}/-1/{source_key}/polygons",
                    "fallback_buffer_m": int(ingress.get("process", {}).get("fallbackBufferM", 20)),
                    "osm": ingress.get("process", {}).get("osm", {}),
                },
            )
            for cu in chunk_urls
        ]

        polygon_csv_urls = []
        for batch in _chunked(poly_tasks, MAX_CONCURRENT):
            polygon_csv_urls.extend([u for u in (yield context.task_all(batch)) if u])

        if not polygon_csv_urls:
            continue

        # 0) format polygons -> json feature list urls (deterministic)
        stable_urls = _stable_sorted_urls(polygon_csv_urls)
        format_poly_tasks = [
            context.call_activity(
                "activity_esquireAudienceBuilder_formatPolygons",
                {"source": url, "destination": ingress["working"]},
            )
            for url in stable_urls
        ]
        polygon_feature_urls = yield context.task_all(format_poly_tasks)

        polygon_feature_urls = [u for u in polygon_feature_urls if u]
        if not polygon_feature_urls:
            continue

        # 0) OnSpot counts (sub-orchestrator) -> count blob urls (https)
        count_urls = yield context.call_sub_orchestrator_with_retry(
            "orchestrator_esquireAudiencesSteps_polygon2deviceidcounts",
            retry,
            {
                "working": ingress["working"],
                # counts should land under working/0/{source_key}
                "destination": {
                    **ingress["working"],
                    "blob_prefix": f"{ingress['working']['blob_prefix']}/0/{source_key}",
                },
                "source_urls": polygon_feature_urls,
                "custom_coding": ingress.get("custom_coding", {}),
            },
        )

        count_urls = [u for u in (count_urls or []) if isinstance(u, str)]
        if not count_urls:
            continue

        # 1) filter each count blob per location/request
        filter_tasks = [
            context.call_activity(
                "activity_faf_filter_count_blob_to_deviceids",
                {
                    "source_url": url,
                    "thresholds": thresholds,
                    "container_name": ingress["destination"]["container_name"],
                    "blob_prefix": f"{ingress['destination']['blob_prefix']}",
                    "conn_str": "AzureWebJobsStorage",
                },
            )
            for i, url in enumerate(count_urls)
        ]

        filtered_urls = []
        for batch in _chunked(filter_tasks, MAX_CONCURRENT):
            filtered_urls.extend([u for u in (yield context.task_all(batch)) if u])

        results.extend(filtered_urls)

    return results
