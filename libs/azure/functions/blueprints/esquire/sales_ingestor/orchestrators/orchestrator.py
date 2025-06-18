from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.database_helpers import insert_upload_record
from sqlalchemy import create_engine
import os
import logging
from http import HTTPStatus
from azure.functions import HttpResponse

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_ingestData(context: DurableOrchestrationContext):
    settings = context.get_input()
    retry = RetryOptions(15000, 3)

    # do the initial upload to show we're working on this
    engine = create_engine(os.environ['DATABIND_SQL_KEYSTONE_DEV'])
    insert_upload_record(
        engine=engine,
        upload_id=settings['metadata']['upload_id'],
        tenant_id='',
        upload_timestamp=settings['metadata']['upload_timestamp'],
        status='Pending',
        metadata={key:val for key, val in settings['metadata'].items() if key not in ['tenant_id','upload_timestamp', 'upload_id', '']},
        schema='sales'
    )
    res = {}
    # get the info from the blob
    try:
        settings = yield context.call_activity_with_retry("activity_readBlob", retry, settings)
    except Exception as e:
        res = {'status':HTTPStatus.INTERNAL_SERVER_ERROR, 'message':'Failure to find blob.', 'error':e}

    # get the validated address information
    if not res:
        try:
            settings = yield context.call_activity_with_retry("activity_validateAddresses", retry, settings)
        except Exception as e:
            res = {'status':HTTPStatus.INTERNAL_SERVER_ERROR, 'message':'Failure to validate addresses.', 'error':e}

    if not res:
        # the primary transformations
        try:
            settings = yield context.call_activity_with_retry("activity_transformData", retry, settings)
        except Exception as e:
            res = {'status':HTTPStatus.INTERNAL_SERVER_ERROR, 'message':'Failure to transform data.', 'error':e}

    if not res:
        try:
            yield context.call_activity_with_retry("activity_writeDatabase", retry, settings)
        except Exception as e:
            return {'status':HTTPStatus.INTERNAL_SERVER_ERROR, 'message':'Failure to write to database.', 'error':e}

    if res:
        # if any errors are caught, post an error card to teams tagging Ryan and the calling user
        yield context.call_activity(
            "activity_microsoftGraph_postErrorCard",
            {
                "function_name": "esquire-sales-ingestion",
                "instance_id": context.instance_id,
                "owners": ["matt@esquireadvertising.com", settings["metadata"]["uploader"]],
                "error": f"{type(res['error']).__name__} : {res['error']}"[:1000],
                "webhook": os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            },
        )
        logging.warning("Error card sent")
        raise e
        return HttpResponse(status=500, body=f"{res['message']}{res['error']}")

    else:
        return {"status": "success"}
