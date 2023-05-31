from azure.core.pipeline import Pipeline
from azure.data.tables import TableClient, TableServiceClient
from azure.data.tables._base_client import TransportWrapper
from typing import Any


class Client(TableClient):
    """
    Custom table client class.

    This class extends the `TableClient` class from the `azure.data.tables` package
    and provides additional functionality.
    """

    __partition_key__: str

    def __init__(self, endpoint: str, table_name: str, **kwargs: Any) -> None:
        """
        Initialize an instance of Client.

        Parameters
        ----------
        endpoint : str
            The endpoint URL.
        table_name : str
            The name of the table.
        **kwargs : Any
            Additional keyword arguments.
        """

        self.__partition_key__ = kwargs.pop("partition_key", None)
        super().__init__(endpoint, table_name, **kwargs)

    @classmethod
    def from_service_client(
        cls, service_client: TableServiceClient, table_name: str, **kwargs
    ):
        """
        Create a client instance from a service client.

        Parameters
        ----------
        service_client : TableServiceClient
            The service client instance.
        table_name : str
            The name of the table.
        **kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        Client
            The created client instance.
        """

        pipeline = Pipeline(  # type: ignore
            transport=TransportWrapper(
                service_client._client._client._pipeline._transport
            ),  # pylint: disable = protected-access
            policies=service_client._policies,
        )
        return cls(
            service_client.url,
            table_name=table_name,
            credential=service_client.credential,
            api_version=service_client.api_version,
            pipeline=pipeline,
            location_mode=service_client._location_mode,
            _hosts=service_client._hosts,
            **kwargs
        )

    def __call__(self, row_key: str, partition_key: str = None, **kwargs):
        """
        Get an entity from the table.

        Parameters
        ----------
        row_key : str
            The row key.
        partition_key : str, optional
            The partition key, by default None.
        **kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        Any
            The retrieved entity.
        """

        if not partition_key and hasattr(self, "__partition_key__"):
            partition_key = self.__partition_key__
        return self.get_entity(partition_key=partition_key, row_key=row_key, **kwargs)
