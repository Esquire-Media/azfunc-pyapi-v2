from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def suborchestrator_salesIngestor_eav(context: DurableOrchestrationContext):
    settings = context.get_input()
    retry = RetryOptions(15000, 3)

    # 1) Prelude: batch entity, metadata, attributes, header map (once)
    yield context.call_activity_with_retry(
        "activity_salesIngestor_eavPrelude",
        retry,
        settings
    )

    # 2) Build balanced order buckets (do not split an order)
    chunk_ids  = yield context.call_activity_with_retry(
        "activity_salesIngestor_assignChunks",
        retry,
        {
            **settings,
            "target_rows_per_chunk": settings.get("target_rows_per_chunk", 1000)
        }
    )

    # 3) Fan-out/fan-in chunk processors, capped parallelism
    for chunk_id in chunk_ids:
        yield context.call_activity_with_retry(
            "activity_salesIngestor_eavTransformChunk",
            retry,
            {**settings, "chunk_id": chunk_id}
        )


    return "EAV fanout/fanin complete"