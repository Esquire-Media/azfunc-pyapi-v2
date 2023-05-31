from libs.utils.decorators import staticproperty
from typing import Any, Callable, List


class TableKeyValueProvider:
    """
    Table-based key-value storage provider.

    This class provides methods to save, load, and delete key-value pairs
    using table-based storage mechanisms.
    """

    @staticproperty
    def SUPPORTED_SCHEMES(self) -> List[str]:
        """
        List of supported schemes.

        Returns
        -------
        List[str]
            A list of supported schemes.
        """

        return ["azure_table"]

    @staticproperty
    def RESOURCE_TYPE_DELIMITER(self) -> str:
        """
        Delimiter used to separate resource types in the key.

        Returns
        -------
        str
            The resource type delimiter.
        """

        return "."

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize an instance of TableKeyValueProvider.

        Parameters
        ----------
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.
        """

        if len(args):
            self.scheme = args[0]
        if "scheme" in kwargs:
            self.scheme = kwargs.pop("scheme")
        self.config = {**kwargs}

    def __getitem__(self, handle: str) -> Any:
        """
        Retrieve an item from the storage using a handle.

        Parameters
        ----------
        handle : str
            The handle associated with the item.

        Returns
        -------
        Any
            The retrieved item.
        """

        match self.scheme:
            case "azure_table":
                handle = handle.split(self.RESOURCE_TYPE_DELIMITER)
                conn = self.connect(
                    table_name=handle[0],
                    partition_key=handle[1] if len(handle) > 1 else None,
                    key_split_char=self.RESOURCE_TYPE_DELIMITER,
                )
                return conn

    def connect(self, table_name: str, **kwargs) -> Any:
        """
        Connect to a table.

        Parameters
        ----------
        table_name : str
            The name of the table.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        Any
            The connection to the specified table.
        """

        match self.scheme:
            case "azure_table":
                from .azure_table import Client as AzureTableClient

                return AzureTableClient.from_service_client(
                    service_client=self.config["client"],
                    table_name=table_name,
                    **kwargs,
                )

    def save(self, key: str, value: Any, encoder: Callable = None, **kwargs) -> None:
        """
        Save a key-value pair in the storage.

        Parameters
        ----------
        key : str
            The key associated with the value.
        value : Any
            The value to be saved.
        encoder : Callable, optional
            The encoder function to use for encoding the value, by default None.
        **kwargs : dict
            Additional keyword arguments.
        """

        if encoder:
            value = encoder(value, **kwargs)
        match self.scheme:
            case "azure_table":
                table_name, partition_key, row_key = self.parse_key(key)
                conn = self.connect(table_name)
                if not hasattr(value, "keys"):
                    value = {"value": value}
                conn.upsert_entity(
                    {"PartitionKey": partition_key, "RowKey": row_key, **value}
                )

    def load(self, key: str, decoder: Callable = None, **kwargs) -> Any:
        """
        Load a value from the storage using a key.

        Parameters
        ----------
        key : str
            The key associated with the value.
        decoder : Callable, optional
            The decoder function to use for decoding the value, by default None.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        Any
            The loaded value.
        """

        match self.scheme:
            case "azure_table":
                table_name, partition_key, row_key = self.parse_key(key)
                conn = self.connect(table_name)
                value = conn.get_entity(partition_key=partition_key, row_key=row_key)
        return decoder(value) if decoder else value

    def drop(self, key: str, **kwargs) -> None:
        """
        Delete a key-value pair from the storage.

        Parameters
        ----------
        key : str
            The key associated with the value to be deleted.
        **kwargs : dict
            Additional keyword arguments.
        """

        match self.scheme:
            case "azure_table":
                table_name, partition_key, row_key = self.parse_key(key)
                conn = self.connect(table_name)
                conn.delete_entity(partition_key=partition_key, row_key=row_key)

    def parse_key(self, key: str):
        """
        Parse a key into table name, partition key, and row key.

        Parameters
        ----------
        key : str
            The key to parse.

        Returns
        -------
        Tuple[str, str, str]
            The table name, partition key, and row key.
        """

        match self.scheme:
            case "azure_table":
                key = key.split(self.RESOURCE_TYPE_DELIMITER)
                table_name = key[0]
                partition_key = key[1]
                row_key = ""
                if len(key) > 2:
                    row_key = key[2]
                return table_name, partition_key, row_key
