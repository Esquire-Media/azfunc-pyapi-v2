from libs.azure.functions import Blueprint
from azure.durable_functions import (
    DurableOrchestrationClient,
    DurableOrchestrationContext,
)
from fuzzywuzzy import fuzz
from azure.storage.blob import BlobClient
from libs.azure.functions.http import HttpRequest, HttpResponse
import os
import pandas as pd
from libs.utils.smarty import bulk_validate

bp: Blueprint = Blueprint()

# Define an HTTP-triggered function that starts a new orchestration
@bp.route(route="test/esquire/rooftop_polys")
@bp.durable_client_input(client_name="client")
async def rooftopPoly_test(req: HttpRequest, client: DurableOrchestrationClient):
    blob_url = "https://esqdevdurablefunctions.blob.core.windows.net/general/a0H6e00000bNazEEAS_test.csv?sv=2021-10-04&st=2023-11-15T21%3A41%3A33Z&se=2024-11-16T21%3A41%3A00Z&sr=b&sp=r&sig=ZMq%2Fn7IfAp9Z%2FOzOBvf3nHacmqv%2BSqOYUOJvU1w3Eqw%3D"
    for chunk in pd.read_csv(blob_url,chunksize=20, encoding_errors="ignore"):
        if isinstance((mapped_chunk := detect_column_names(chunk)), pd.DataFrame):
            if isinstance(
                (
                    valid_chunk := bulk_validate(
                        df=mapped_chunk,
                        address_col="street"
                        if "street" in mapped_chunk.columns
                        else None,
                        city_col="city" if "city" in mapped_chunk.columns else None,
                        state_col="state" if "state" in mapped_chunk.columns else None,
                        zip_col="zipcode"
                        if "zipcode" in mapped_chunk.columns
                        else None,
                    )
                    .dropna(subset=["delivery_line_1"])
                    .apply(
                        lambda row: f"{row['delivery_line_1']}, {row['city_name']} {row['state_abbreviation']}, {row['zipcode']}",
                        axis=1,
                    )
                    .str.upper()
                ),
                pd.Series,
            ):
                df = valid_chunk
        break
    
    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_rooftopPolys",
        client_input= df.to_list()
    )

    # Return a response that includes the status query URLs
    return client.create_check_status_response(req, instance_id)


def detect_column_names(df):
    """
    Attempts to automatically detect the address component columns in a sales file
    Returns a slice of the sales data with detected columns for [address, city, state, zip]
    """
    # dictionary of common column headers for address components
    mapping = {
        "street": ["address", "street", "delivery_line_1", "line1", "add"],
        "city": ["city", "city_name"],
        "state": ["state", "st", "state_abbreviation"],
        "zip": ["zip", "zipcode", "postal", "postalcodeid"],
    }

    # find best fit for each address field
    for dropdown, defaults in mapping.items():
        column_scores = [
            max([fuzz.ratio(column.upper(), default.upper()) for default in defaults])
            for column in df.columns
        ]
        best_fit_idx = column_scores.index(max(column_scores))
        best_fit = df.columns[best_fit_idx]
        df = df.rename(columns={best_fit: dropdown})

    return df[["street", "city", "state", "zip"]]