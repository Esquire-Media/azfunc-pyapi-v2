import os
import numpy as np
import pandas as pd
from azure.storage.blob import ContainerClient
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor


def get_all_neighbors(
    address_df: pd.DataFrame, N: int, same_side_only: bool = True, limit: int = -1
):
    """
    Main function to get address neighbors by batching by the CSZ for quicker reading.

    Parameters
    ----------
    address_df : pandas.DataFrame
        Formatted query street data from smarty. Must include headers street_number, street_name, city, state, zip_code.
    N : int
        Number of neighbors to fetch in each direction (if possible).
    same_side_only : bool, optional
        Flag to indicate if we're querying both or one side of the street, by default True.
    limit : int, optional
        Optional maximum number of records to return, by default -1.

    Returns
    -------
    pandas.DataFrame
        All neighbors of the input addresses.
    """
    results_list = []

    # Group data by city, state, and zip_code to batch operations
    for (city, state, zip_code), part_df in address_df.groupby(
        ["city", "state", "zip_code"]
    ):
        city = city.replace(" ", "_")

        try:
            # Load data concurrently
            estated_data = load_estated_data_partitioned_blob(
                f"estated_partition_testing/state={state}/zip_code={zip_code}/city={city}/"
            )
        except Exception as e:
            continue

        for street_name, street_addresses in part_df.groupby("street_name"):
            street_data = estated_data[
                estated_data["street_name"] == str(street_name).upper()
            ]
            if street_data.empty:
                continue

            neighbors = find_neighbors_for_street(
                street_data, street_addresses, N, same_side_only
            )
            if not neighbors.empty:
                results_list.append(neighbors)

    results = (
        pd.concat(results_list, ignore_index=True) if results_list else pd.DataFrame()
    )

    if not results.empty:
        results = results.reset_index(drop=True)
        return results.head(limit) if limit > 0 else results
    else:
        return pd.DataFrame()


def find_neighbors_for_street(
    data: pd.DataFrame, addresses: pd.DataFrame, N: int, same_side_only: bool
) -> pd.DataFrame:
    """
    Vectorized way of finding neighbors for all addresses on a given street.

    Parameters
    ----------
    data : pandas.DataFrame
        The relevant estated data on this street.
    addresses : pandas.DataFrame
        The input addresses on this street.
    N : int
        Number of neighbors to fetch in each direction (if possible).
    same_side_only : bool
        Flag to indicate if we're querying both or one side of the street.

    Returns
    -------
    pandas.DataFrame
        All neighbors of the input addresses.
    """
    data = (
        data.sort_values(by="street_number")
        .dropna(subset=["street_number"])
        .reset_index(drop=True)
    )
    increment = 2 if same_side_only else 1

    addresses = addresses.dropna(subset=["street_number"]).copy()
    addresses["base_street_num"] = pd.to_numeric(
        addresses["street_number"], errors="coerce"
    )
    addresses = addresses.dropna(subset=["base_street_num"])

    if data.empty or addresses.empty:
        return pd.DataFrame()

    start_indices = data["street_number"].searchsorted(
        addresses["base_street_num"] - N * increment, side="left"
    )
    end_indices = data["street_number"].searchsorted(
        addresses["base_street_num"] + N * increment, side="right"
    )

    if len(start_indices) == 0 or len(end_indices) == 0:
        return pd.DataFrame()

    try:
        base_ids = np.repeat(addresses.index, end_indices - start_indices)
        neighbor_indices = np.concatenate(
            [np.arange(start, end) for start, end in zip(start_indices, end_indices)]
        )
    except ValueError:
        return pd.DataFrame()

    result = pd.DataFrame(
        {"base_address_id": base_ids, "neighbor_index": neighbor_indices}
    )
    neighbors = result.merge(data, left_on="neighbor_index", right_index=True)

    if same_side_only:
        base_evenness = addresses["base_street_num"] % 2
        evenness_map = dict(zip(addresses.index, base_evenness))
        neighbors["base_evenness"] = neighbors["base_address_id"].map(evenness_map)
        neighbors = neighbors[
            neighbors["street_number"] % 2 == neighbors["base_evenness"]
        ]

    return neighbors.drop(
        columns=["base_evenness", "base_address_id", "neighbor_index"]
    ).reset_index(drop=True)


def load_parquet_from_blob(blob_dir_path):
    """
    Load parquet files from an Azure Blob Storage directory.

    Parameters
    ----------
    blob_dir_path : str
        The directory path in the Azure Blob Storage container.

    Returns
    -------
    pandas.DataFrame
        The concatenated DataFrame of all parquet files in the given directory.
    """
    container_client = ContainerClient.from_connection_string(
        os.environ["DATALAKE_CONN_STR"], container_name="general"
    )
    blobs = list(container_client.list_blobs(name_starts_with=blob_dir_path))

    # Use ThreadPoolExecutor to parallelize downloading and reading blobs
    def load_blob(blob):
        if blob.size == 0:
            return pd.DataFrame()

        blob_client = container_client.get_blob_client(blob)
        blob_stream = blob_client.download_blob()
        return pd.read_parquet(
            BytesIO(blob_stream.readall()),
            columns=["street_number", "street_name", "city", "state", "zip_code"],
        )

    with ThreadPoolExecutor() as executor:
        dataframes = list(executor.map(load_blob, blobs))

    non_empty_dataframes = [df for df in dataframes if not df.empty]
    if non_empty_dataframes:
        return (
            pd.concat(non_empty_dataframes, ignore_index=True)
            .drop_duplicates()
            .dropna(subset=["street_number"])
        )
    else:
        return pd.DataFrame()


def load_estated_data_partitioned_blob(table_path):
    """
    Read in estated data for a given path, ensuring that the street number is an integer.

    Parameters
    ----------
    table_path : str
        The path to the partitioned blob storage directory.

    Returns
    -------
    pandas.DataFrame
        The formatted estated data.
    """
    blob_df = load_parquet_from_blob(table_path)
    blob_df = blob_df.dropna(subset=["street_number", "street_name"], how="any")
    blob_df["street_number"] = pd.to_numeric(
        blob_df["street_number"], errors="coerce"
    ).astype("Int64")
    blob_df["street_name"] = blob_df["street_name"].astype("str")
    return blob_df
