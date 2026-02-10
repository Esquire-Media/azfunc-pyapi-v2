from azure.durable_functions import Blueprint, DurableOrchestrationContext
import os
from typing import List

bp = Blueprint()

_DEFAULT_DEMOS_BATCH_SIZE = 25


def _chunk(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        size = _DEFAULT_DEMOS_BATCH_SIZE
    return [items[i : i + size] for i in range(0, len(items), size)]


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_deviceidsDemoFiltered(
    context: DurableOrchestrationContext,
):
    """
    Batched orchestration:
      deviceids -> demographics -> filtered deviceids

    Returns
    -------
    List[str]
        URLs of filtered deviceid CSV blobs
    """

    ingress = context.get_input()

    source_urls: List[str] = ingress["source_urls"]
    if not source_urls:
        return []

    batch_size = int(
        os.getenv("DEMOS_BATCH_SIZE", str(_DEFAULT_DEMOS_BATCH_SIZE))
    )

    all_filtered_urls: List[str] = []

    for batch_index, batch in enumerate(_chunk(source_urls, batch_size)):
        # ---- Phase 1: deviceids -> demographics (fan-out) ----
        demographics_batches = yield context.task_all(
            [
                context.call_sub_orchestrator(
                    "orchestrator_esquireAudiencesSteps_deviceids2Demographics",
                    {
                        **ingress,
                        # sub-orchestrator expects a list
                        "source_urls": [source_url],
                    },
                )
                for source_url in batch
            ]
        )

        # Flatten demographics URLs
        demo_urls: List[str] = [
            url
            for result in demographics_batches
            for url in result
        ]

        if not demo_urls:
            continue

        # ---- Phase 2: demographics -> filtered deviceids (fan-out) ----
        filtered_urls = yield context.task_all(
            [
                context.call_activity(
                    "activity_esquireAudiences_filterDemographics",
                    {
                        **ingress,
                        "source_url": demo_url,
                    },
                )
                for demo_url in demo_urls
            ]
        )

        all_filtered_urls.extend(filtered_urls)

    return all_filtered_urls
