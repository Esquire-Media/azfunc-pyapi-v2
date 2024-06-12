"""
    Azure Table storage implementation
"""

from azure.data.tables import TableServiceClient
from azure.core.credentials import (
    AzureKeyCredential,
    AzureNamedKeyCredential,
    AzureSasCredential,
)
from datetime import datetime, timedelta, timezone
from py_abac.storage.memory import MemoryStorage
from py_abac.exceptions import PolicyExistsError
from py_abac.policy import Policy
from typing import Union, Generator
import orjson as json, logging

LOG = logging.getLogger(__name__)


class AzureTableStorage(MemoryStorage):
    """
    Stores and retrieves policies from Azure Tables

    :param table_name: name of the table used for saving data to
    :param conn_str: can be found in your storage account in the Azure Portal under the "Access Keys" section
    :param endpoint: can be found in your storage account in the Azure Portal under the "Access Keys" section
    :param credential: may be provided in a number of different forms, depending on the type of authorization you wish to use
    :param cache_enable: set to true to enable limited caching
    :param cache_expiry: timedelta used to expire existing cache. when using a serverless environment it may be best to keep this low if you expect many changes
    """

    def __init__(
        self,
        table_name: str,
        conn_str: Union[str, None] = None,
        endpoint: Union[str, None] = None,
        credential: Union[
            AzureKeyCredential, AzureNamedKeyCredential, AzureSasCredential, None
        ] = None,
        partition_key: str = "policy",
        cache_enabled: bool = True,
        cache_expiry: timedelta = timedelta(minutes=5),
    ):
        # Create table service client
        _table_service = None
        if conn_str:
            _table_service = TableServiceClient.from_connection_string(
                conn_str=conn_str
            )
        elif endpoint and credential:
            _table_service = TableServiceClient(
                endpoint=endpoint, credential=credential
            )
        if _table_service:
            self._table_client = _table_service.create_table_if_not_exists(table_name)

        self.partition_key = partition_key
        self.cache_enabled = cache_enabled
        self.cache_expiry: timedelta = cache_expiry
        self.last_cached = datetime(1970, 1, 1, tzinfo=timezone.utc)

    def cache(self, override: bool = False):
        if (
            override
            or datetime.utcnow().timestamp()
            > (self.last_cached + self.cache_expiry).timestamp()
        ):
            super().__init__()
            for data in self._table_client.query_entities(
                query_filter=f"PartitionKey eq '{self.partition_key}'"
            ):
                data["uid"] = str(data["RowKey"])
                del data["PartitionKey"]
                del data["RowKey"]
                data["rules"] = json.loads(data["rules"])
                data["targets"] = json.loads(data["targets"])
                super().add(Policy.from_json(data))
            self.last_cached = datetime.utcnow()

    def get(self, uid: str) -> Union[Policy, None]:
        """
        Get specific policy
        """
        if self.cache_enabled:
            self.cache()
            super().get(uid)
        else:
            try:
                data = self._table_client.get_entity(self.partition_key, uid)
                data["uid"] = str(data["RowKey"])
                del data["PartitionKey"]
                del data["RowKey"]
                data["rules"] = json.loads(data["rules"])
                data["targets"] = json.loads(data["targets"])
                return Policy.from_json(data)
            except:
                return None

    def get_all(self, limit: int, offset: int) -> Generator[Policy, None, None]:
        """
        Retrieve all the policies within a window.

        .. note:

            Selecting s limit/offset is not supported by Azure Tables, so
            rows data is yielded based on its index value.
        """
        if self.cache_enabled:
            self.cache()
            for policy in super().get_all(limit, offset):
                yield policy
        else:
            self._check_limit_and_offset(limit, offset)
            index = 0
            for data in self._table_client.query_entities(
                query_filter=f"PartitionKey eq '{self.partition_key}'"
            ):
                if index >= offset and index <= offset + limit:
                    data["uid"] = str(data["RowKey"])
                    del data["PartitionKey"]
                    del data["RowKey"]
                    data["rules"] = json.loads(data["rules"])
                    data["targets"] = json.loads(data["targets"])
                    yield Policy.from_json(data)
                index += 1

    def get_for_target(
        self, subject_id: str, resource_id: str, action_id: str
    ) -> Generator[Policy, None, None]:
        """
        Get all policies for given target IDs.

        .. note:

            Currently all policies are returned for evaluation.
        """
        if self.cache_enabled:
            self.cache()
            for policy in super().get_for_target(subject_id, resource_id, action_id):
                yield policy
        else:
            for data in self._table_client.query_entities(
                query_filter=f"PartitionKey eq '{self.partition_key}'"
            ):
                data["uid"] = str(data["RowKey"])
                del data["PartitionKey"]
                del data["RowKey"]
                data["rules"] = json.loads(data["rules"])
                data["targets"] = json.loads(data["targets"])
                yield Policy.from_json(data)

    def add(self, policy: Policy):
        """
        Store a policy
        """
        try:
            data: dict = policy.to_json()
            self._table_client.create_entity(
                {
                    "PartitionKey": self.partition_key,
                    "RowKey": data["uid"],
                    "description": data["description"],
                    "rules": json.dumps(data["rules"]).decode(),
                    "targets": json.dumps(data["targets"]).decode(),
                    "effect": data["effect"],
                    "priority": data["priority"],
                }
            )
        except:
            LOG.error(
                "Error trying to create already existing policy with UID=%s.",
                policy.uid,
            )
            raise PolicyExistsError(policy.uid)

        if self.cache_enabled:
            self.cache(True)

        LOG.info("Added Policy: %s", policy)

    def update(self, policy: Policy):
        """
        Update a policy
        """
        try:
            data: dict = policy.to_json()
            self._table_client.update_entity(
                {
                    "PartitionKey": self.partition_key,
                    "RowKey": data["uid"],
                    "description": data["description"],
                    "rules": json.dumps(data["rules"]).decode(),
                    "targets": json.dumps(data["targets"]).decode(),
                    "effect": data["effect"],
                    "priority": data["priority"],
                }
            )
        except:
            raise ValueError("Policy with UID='{}' does not exist.".format(policy.uid))

        if self.cache_enabled:
            self.cache(True)

        LOG.info("Updated Policy with UID=%s. New value is: %s", policy.uid, policy)

    def delete(self, uid: str):
        """
        Delete a policy
        """
        try:
            self._table_client.delete_entity(self.partition_key, uid)
        except:
            raise ValueError("Policy with UID='{}' does not exist.".format(uid))

        if self.cache_enabled:
            self.cache(True)

        LOG.info("Deleted Policy with UID=%s.", uid)
