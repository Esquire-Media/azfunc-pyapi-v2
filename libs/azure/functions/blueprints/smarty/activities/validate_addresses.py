"""Legacy activity name; address enrichment is performed by Keystone AddrAll."""

import pandas as pd
from azure.durable_functions import Blueprint

from libs.utils.azure_storage import load_dataframe, export_dataframe
from libs.utils.smarty import bulk_validate, detect_column_names


bp: Blueprint = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_smarty_validateAddresses(ingress: dict):
    """Geocode, correct, complete and normalize addresses while keeping every row.

    Accepts `source`, optional `column_mapping`, `destination`, and
    `columns_to_return` as before. Optional settings: `use_geocode`,
    `retry_missing_zip4`, `batch_size`, `geocode_sources`, `keep_original`.
    No USPS/Smarty DPV assertion is made.
    """
    data = load_dataframe(ingress["source"])
    mapping = detect_column_names(
        data.columns, override_mappings=ingress.get("column_mapping", {}),
    )
    result = bulk_validate(
        df=data,
        address_col=mapping["street"],
        addr2_col=mapping["addr2"],
        city_col=mapping["city"],
        state_col=mapping["state"],
        zip_col=mapping["zip"],
        zip4_col=mapping["zip4"],
        batch_size=ingress.get("batch_size"),
        geocode_sources=ingress.get("geocode_sources"),
        use_geocode=bool(ingress.get("use_geocode", True)),
        retry_missing_zip4=bool(ingress.get("retry_missing_zip4", True)),
        drop_address=not bool(ingress.get("keep_original", True)),
    )
    if ingress.get("columns_to_return"):
        result = result[ingress["columns_to_return"]]

    if ingress.get("destination"):
        # CSV/Parquet exporters accept pandas nulls; do not allocate a second
        # object-typed copy of the entire data set just for a blob write.
        return export_dataframe(df=result, destination=ingress["destination"])
    # Inline Durable payloads need JSON-safe nulls. Large jobs should supply
    # a destination instead of returning their complete data as activity JSON.
    result = result.astype(object).where(pd.notna(result), None)
    return result.to_dict(orient="records")
