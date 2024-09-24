from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, AuthLevel
from fuzzywuzzy import fuzz
from libs.utils.smarty import bulk_validate
import pandas as pd, io

bp: Blueprint = Blueprint()


# Define an HTTP-triggered function that starts a new orchestration
@bp.route(route="esquire/rooftop_polys", auth_level=AuthLevel.FUNCTION)
@bp.durable_client_input(client_name="client")
async def starter_rooftopPoly(req: HttpRequest, client: DurableOrchestrationClient):
    addresses = []
    for chunk in pd.read_csv(io.BytesIO(req.get_body()), chunksize=1000, encoding_errors="ignore"):
        if isinstance((mapped_chunk := detect_column_names(chunk)), pd.DataFrame):
            if isinstance(
                (
                    valid_chunk := bulk_validate(
                        df=mapped_chunk,
                        address_col=(
                            "street" if "street" in mapped_chunk.columns else None
                        ),
                        city_col="city" if "city" in mapped_chunk.columns else None,
                        state_col="state" if "state" in mapped_chunk.columns else None,
                        zip_col=(
                            "zipcode" if "zipcode" in mapped_chunk.columns else None
                        ),
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
                addresses += valid_chunk.to_list()
        break

    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_rooftopPolys",
        client_input=addresses,
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
