# File: libs/data/structured/sqlalchemy/__init__.py

from libs.utils.decorators import staticproperty
from .interface import QueryFrame
from .marshmallow import extend_models as extend_models_marshmallow, schema
from .utils import extend_models as extend_models_base, name_for_collection_relationship
from sqlalchemy import (
    create_engine,
    Column,
    Engine,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
)
from sqlalchemy.ext.automap import automap_base, AutomapBase
from sqlalchemy.orm import Session
from typing import Any, Callable, List
import uuid

MODEL_EXTENSION_STEPS: List[Callable] = [
    extend_models_base,
    extend_models_marshmallow,
]


class SQLAlchemyStructuredProvider:
    """
    Structured data storage provider using SQLAlchemy.

    This provider allows storing, loading, and dropping structured data using SQLAlchemy as the underlying database engine.

    Examples
    --------
    >>> from libs.data import register_binding, from_binding
    >>> register_binding(
    >>>     "an_sql_server",
    >>>     "Structured",
    >>>     "sql",
    >>>     url="mssql+pymssql://<username>:<password>@<host>:<port>/<database>?charset=utf8",
    >>>     schemas=["other", "dbo"],
    >>> )
    >>> provider = from_bind("an_sql_server")
    >>> qf = provider["other.table1"]
    >>> qf = qf[[qf["column1"], qf["column2"], qf["column3"]]]
    >>> # Perform actions with the QueryFrame.
    """

    @staticproperty
    def SUPPORTED_SCHEMES(self) -> list:
        """
        List of supported schemes.

        Returns
        -------
        list
            A list of supported schemes.

        Examples
        --------
        >>> supported_schemes = SQLAlchemyStructuredProvider.SUPPORTED_SCHEMES

        Notes
        -----
        This property returns a list of supported schemes for the SQLAlchemyStructuredProvider.
        """

        return ["sql"]

    @staticproperty
    def RESOURCE_TYPE_DELIMITER(self) -> str:
        """
        Resource type delimiter.

        Returns
        -------
        str
            The resource type delimiter.

        Examples
        --------
        >>> delimiter = SQLAlchemyStructuredProvider.RESOURCE_TYPE_DELIMITER

        Notes
        -----
        This property returns the delimiter used to separate resource types in keys.
        """

        return "."

    @staticproperty
    def DEFAULT_SCHEMA(self) -> str:
        """
        Default schema.

        Returns
        -------
        str
            The default schema.

        Examples
        --------
        >>> default_schema = SQLAlchemyStructuredProvider.DEFAULT_SCHEMA

        Notes
        -----
        This property returns the default schema used when no schema is specified.
        """

        return "default"

    def get_schema(self, type_: str = "marshmallow") -> dict:
        """
        Get the schema for a given structure type.

        Parameters
        ----------
        type_ : str, optional
            The type of the structure, by default "marshmallow".

        Returns
        -------
        dict
            A dictionary representing the structure's schema.

        Examples
        --------
        >>> schema = provider.get_schema(type_='marshmallow')

        Notes
        -----
        This method retrieves the schema for a given structure type, such as "marshmallow".
        """

        match type_:
            case "marshmallow":
                return schema.marshmallow_schema_to_dict(self)

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize the SQLAlchemyStructuredProvider.

        Parameters
        ----------
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.

        Examples
        --------
        >>> provider = SQLAlchemyStructuredProvider(engine='sqlite:///:memory:')

        Notes
        -----
        This method initializes the SQLAlchemyStructuredProvider by setting up the underlying database engine and metadata.
        It reflects the database tables, prepares the base automap, and extends the models.
        It also sets up the session and provides access to the models for performing CRUD operations on the structured data.
        """

        kw = kwargs.keys()

        # Remove the 'scheme' key from kwargs
        kwargs.pop("scheme")

        # Generate a unique ID for the provider
        self.id: str = uuid.uuid4().hex

        # Determine the schemas to reflect (if provided)
        self.schemas: List[str] = kwargs.pop(
            "schemas", [s] if (s := kwargs.pop("schema", None)) else None
        )

        # Set up the database engine
        self.engine: Engine = (
            kwargs.pop("engine")
            if "engine" in kw
            else create_engine(kwargs.pop("url"), **kwargs)
            if "url" in kw
            else None
        )
        if not self.engine:
            raise Exception("No engine configuration values specified.")

        # Create the metadata object
        self.metadata = MetaData()

        # Reflect the database tables
        if self.schemas:
            # Reflect tables for specific schemas
            for s in self.schemas:
                self.metadata.reflect(bind=self.engine, schema=s, views=True)
                for table in self.metadata.tables.values():
                    if table.schema == s:
                        if not table.primary_key:
                            # Set primary key if not defined
                            for col in table.c:
                                if (
                                    (c := col.name.lower()) == "id"
                                    or c == "uuid"
                                    or c == "guid"
                                ):
                                    col.primary_key = True
                                    table.append_constraint(PrimaryKeyConstraint(col))
                                elif c[-3:] == "_id":
                                    col.primary_key = True
                                    table.append_constraint(PrimaryKeyConstraint(col))
                        if not table.primary_key:
                            # Add a fake primary key column if no primary key defined
                            table.append_column(
                                Column("fake_pk_id", Integer, primary_key=True)
                            )
                            table.append_constraint(PrimaryKeyConstraint("fake_pk_id"))
        else:
            # Reflect tables for all schemas
            self.metadata.reflect(bind=self.engine, views=True)
            for table in self.metadata.tables.values():
                if not table.primary_key:
                    # Set primary key if not defined
                    for col in table.c:
                        if (
                            (c := col.name.lower()) == "id"
                            or c == "uuid"
                            or c == "guid"
                        ):
                            col.primary_key = True
                            table.append_constraint(PrimaryKeyConstraint(col))
                        elif c[-3:] == "_id":
                            col.primary_key = True
                            table.append_constraint(PrimaryKeyConstraint(col))
                if not table.primary_key:
                    # Add a fake primary key column if no primary key defined
                    table.append_column(Column("fake_pk_id", Integer, primary_key=True))
                    table.append_constraint(PrimaryKeyConstraint("fake_pk_id"))

        # Create the base automap
        self.base: AutomapBase = automap_base(metadata=self.metadata)

        # Set up the session
        self.session: Callable = lambda: Session(self.engine)

        # Prepare the base automap
        self.base.prepare(
            modulename_for_table=self.modulename_for_table,
            name_for_collection_relationship=name_for_collection_relationship,
        )

        # Retrieve the models for the provider's ID
        self.models = self.base.by_module.get(self.id)

        # Extend the models with additional functionality
        for func in MODEL_EXTENSION_STEPS:
            func(
                models=[
                    self.models[schema][model]
                    for schema in self.models.keys()
                    for model in self.models[schema].keys()
                ],
                session=self.session,
            )

    def __getitem__(self, handle):
        """
        Get a QueryFrame for the specified handle.

        Parameters
        ----------
        handle : str
            The handle to retrieve the QueryFrame.

        Returns
        -------
        QueryFrame
            A QueryFrame object representing the structured data.

        Examples
        --------
        >>> query_frame = provider['schema.table.primary_key']
        >>> # Perform actions with the QueryFrame.

        Notes
        -----
        This method retrieves a QueryFrame object for the specified handle, which represents structured data.
        """

        selected = self.models
        selectors = handle.split(self.RESOURCE_TYPE_DELIMITER)
        if self.schemas:
            if selectors[0] not in self.schemas:
                selectors = [self.schemas[0]] + selectors
        else:
            selectors = [self.DEFAULT_SCHEMA] + selectors
        for selector in selectors:
            selected = selected[selector]
        return QueryFrame(selected, self.session)

    def connect(self) -> Session:
        """
        Connect to the structured data storage.

        Returns
        -------
        Session
            A SQLAlchemy Session object.

        Examples
        --------
        >>> session = provider.connect()
        >>> # Perform actions with the SQLAlchemy session.

        Notes
        -----
        This method establishes a connection to the structured data storage and returns a SQLAlchemy Session object.
        """

        return self.session()

    def save(
        self,
        key: str,
        value: Any,
        schema_name: str = None,
        table_name: str = None,
        model: Any = None,
    ) -> None:
        """
        Save a value to the specified key.

        Parameters
        ----------
        key : str
            The key to save the value.
        value : Any
            The value to save.
        schema_name : str, optional
            The schema name, by default None.
        table_name : str, optional
            The table name, by default None.
        model : Any, optional
            The model to use, by default None.

        Examples
        --------
        >>> provider.save('schema.table.primary_key', value)

        Notes
        -----
        This method saves a value to the specified key in the structured data storage.
        """

        session = self.session()
        if not model:
            if not table_name and not schema_name:
                schema_name, table_name, primary_key = self.parse_key(key)
            model = self.models[schema_name or self.DEFAULT_SCHEMA][table_name]
        record = session.query(model).get(primary_key)
        if record:
            for k, v in value.items():
                setattr(record, k, v)
        else:
            session.add(model(**value))
        session.commit()

    def load(
        self,
        key: str,
        schema_name: str = None,
        table_name: str = None,
        model: Any = None,
    ) -> Any:
        """
        Load a value from the specified key.

        Parameters
        ----------
        key : str
            The key to load the value.
        schema_name : str, optional
            The schema name, by default None.
        table_name : str, optional
            The table name, by default None.
        model : Any, optional
            The model to use, by default None.

        Returns
        -------
        Any
            The loaded value.

        Examples
        --------
        >>> value = provider.load('schema.table.primary_key')

        Notes
        -----
        This method loads a value from the specified key in the structured data storage.
        """

        session = self.session()
        if not model:
            if not table_name and not schema_name:
                schema_name, table_name, primary_key = self.parse_key(key)
            model = self.models[schema_name or self.DEFAULT_SCHEMA][table_name]
        return session.query(model).get(primary_key)

    def filter(
        self, filters: List[str], decoder: Callable = None, **kwargs
    ) -> List[Any]:
        """
        Filter the structured data.

        Parameters
        ----------
        filters : List[str]
            The list of filters.
        decoder : Callable, optional
            The decoder function to decode the values, by default None.
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        List[Any]
            The filtered data.

        Examples
        --------
        >>> filtered_data = provider.filter(['schema.table.column == "value"'])

        Notes
        -----
        ***TODO***
        This method filters the structured data based on the provided filters and returns the filtered data.
        """

        pass

    def drop(
        self,
        key: str,
        schema_name: str = None,
        table_name: str = None,
        model: Any = None,
    ) -> None:
        """
        Drop the value associated with the specified key.

        Parameters
        ----------
        key : str
            The key to drop the value.
        schema_name : str, optional
            The schema name, by default None.
        table_name : str, optional
            The table name, by default None.
        model : Any, optional
            The model to use, by default None.

        Examples
        --------
        >>> provider.drop('schema.table.primary_key')

        Notes
        -----
        This method deletes the record with the specified key from the structured data storage.
        """

        session = self.session()
        if not model:
            if not table_name and not schema_name:
                schema_name, table_name, primary_key = self.parse_key(key)
            model = self.models[schema_name or self.DEFAULT_SCHEMA][table_name]
        session.delete(session.query(model).get(primary_key))
        session.commit()

    def parse_key(self, key: str):
        """
        Parse the key into schema, table, and primary key.

        Parameters
        ----------
        key : str
            The key to parse.

        Returns
        -------
        Tuple[str, str, str]
            The parsed schema name, table name, and primary key.

        Examples
        --------
        >>> schema, table, primary_key = provider.parse_key('schema.table.primary_key')

        Notes
        -----
        This method parses the specified key into schema name, table name, and primary key components.
        """

        key = key.split(self.RESOURCE_TYPE_DELIMITER)
        schema_name = key[0]
        table_name = key[1]
        primary_key = ""
        if len(key) > 2:
            primary_key = key[2]
        return schema_name, table_name, primary_key

    def modulename_for_table(self, cls, tablename, table) -> str:
        """
        Get the module name for a table.

        Parameters
        ----------
        cls : Any
            The class.
        tablename : str
            The table name.
        table : Any
            The table.

        Returns
        -------
        str
            The module name for the table.

        Examples
        --------
        >>> module_name = provider.modulename_for_table(cls, tablename, table)

        Notes
        -----
        This method retrieves the module name for a table in the structured data storage.
        """

        return (
            f"{self.id}.{table.schema}"
            if getattr(table, "schema", None)
            else f"{self.id}.{self.DEFAULT_SCHEMA}"
        )
