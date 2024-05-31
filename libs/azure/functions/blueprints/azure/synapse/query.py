# File: libs/azure/functions/blueprints/synapse/query.py

from libs.azure.functions import Blueprint
from libs.data import from_bind
import pandas as pd, logging

# Try to import orjson for faster JSON serialization/deserialization.
# Fall back to the built-in json module if orjson is not available.
try:
    import orjson as json
except ImportError:
    import json

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def activity_synapse_query(ingress: dict):
    """
    This function queries a database and returns a dict object with the assurance that it is JSON (de)serializable.

    Params:
    ingress (dict): A dictionary containing the following keys:
        - bind (str): The connection string or identifier for the Synapse database.
        - query (str): The SQL query to be executed (e.g., "SELECT * FROM table").

    Returns:
    dict: The result of the query in JSON-serializable format, or an error message if an exception occurs.
    """

    # Use a context manager to ensure the database connection is properly closed after use.
    with from_bind(ingress["bind"]).connect() as connection:
        # Execute the SQL query and read the result into a pandas DataFrame.
        df = pd.read_sql(ingress["query"], connection.connection())
        logging.warning(df)
        # Convert the DataFrame to JSON format with orient="records" to ensure it is JSON-serializable.
        json_result = df.to_json(orient="records")
        # Deserialize the JSON string to a Python dictionary and return it.
        return json.loads(json_result)
