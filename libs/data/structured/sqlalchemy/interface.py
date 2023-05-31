from marshmallow import Schema
from sqlalchemy import Column
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import InstrumentedAttribute, Mapper, Query, Session
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList
from typing import Any
import logging


class QueryFrame:
    """
    Represents a query frame for querying SQLAlchemy models.

    Parameters
    ----------
    model : Any
        The SQLAlchemy model class.
    session : Session
        The SQLAlchemy Session object.

    Attributes
    ----------
    __model : Any
        The SQLAlchemy model class.
    __mapper : Mapper
        The SQLAlchemy Mapper object.
    __session : Session
        The SQLAlchemy Session object.
    __select : List[InstrumentedAttribute]
        The list of selected fields.
    __ops : List[Tuple[str, BinaryExpression or BooleanClauseList]]
        The list of query operations.
    __sort : List[Column]
        The list of sorting columns.
    __limit : int
        The limit for the number of results.
    __offset : int
        The offset for the query results.

    Methods
    -------
    __getitem__(self, key: Any = None)
        Retrieve items from the query frame.
    __build__(self) -> Query
        Build the SQLAlchemy query object.
    __call__(self, key: str = None)
        Execute the query and retrieve the results.
    __len__(self) -> int
        Get the count of query results.
    __repr__(self) -> str
        Get a string representation of the query.
    __getitem_field(self, key)
        Retrieve an attribute as an InstrumentedAttribute.
    __slice(self, key)
        Handle slicing operations on the query frame.
    to_pandas(self)
        Convert the query results to a pandas DataFrame.
    """

    def __init__(self, model: Any, session: Session) -> None:
        """
        Initialize a QueryFrame instance.

        Parameters
        ----------
        model : Any
            The SQLAlchemy model class.
        session : Session
            The SQLAlchemy Session object.
        """

        self.__model = model
        self.__mapper: Mapper = inspect(model)
        self.__session = session
        self.__select = []
        self.__ops = []
        self.__sort = ()
        self.__limit = 0
        self.__offset = 0
    
    @property
    def schema(self) -> Schema:
        pass

    @property
    def __primary_key__(self) -> Column:
        """
        Get the primary key column of the model.

        Returns
        -------
        Column
            The primary key column.
        """

        for column in self.__mapper.columns.values():
            if column.primary_key:
                return column

    def __getitem__(self, key: Any = None):
        """
        Retrieve items from the query frame.

        Parameters
        ----------
        key : Any, optional
            The key to retrieve items. If None, return the QueryFrame object.

        Returns
        -------
        QueryFrame or Any
            The retrieved item or the QueryFrame object.
        """

        match key:
            case list():
                for item in key:
                    self[item]
            case tuple():
                return [self[item] for item in key]
            case InstrumentedAttribute():
                self.__select.append(key)
            case BinaryExpression() | BooleanClauseList():
                self.__ops.append(("where", key))
            case str():
                return self.__getitem_field(key)
            case slice():
                self.__slice(key)
        return self

    def __build__(self) -> Query:
        """
        Build the SQLAlchemy query object.

        Returns
        -------
        Query
            The SQLAlchemy query object.
        """

        query: Query = (
            self.__session().query(*(self.__select or [self.__model])).select_from(self.__model)
        )
        for op in self.__ops:
            query = getattr(query, op[0])(op[1])
        if len(self.__sort):
            query = query.order_by(*self.__sort)
            if self.__limit:
                query = query.limit(self.__limit)
            if self.__offset:
                query = query.offset(self.__offset)
        return query

    def __call__(self, key: str = None):
        """
        Execute the query and retrieve the results.

        Parameters
        ----------
        key : str, optional
            The key to retrieve a specific item. If None, retrieve all items.

        Returns
        -------
        Any or List[Any]
            The retrieved item or the list of retrieved items.
        """

        if key:
            return self.__session().query(self.__model).get(key)
        return self.__build__().all()

    def __len__(self) -> int:
        """
        Get the count of query results.

        Returns
        -------
        int
            The count of query results.
        """

        return self.__build__().count()

    def __repr__(self) -> str:
        """
        Get a string representation of the query.

        Returns
        -------
        str
            The string representation of the query.
        """

        query = self.__build__()
        query_string = str(
            query.statement.compile(
                bind=self.__session().bind,
                compile_kwargs={"literal_binds": True},
            )
        )
        try:
            from sql_formatter.core import format_sql

            return format_sql(query_string)
        except:
            return query_string

    def __getitem_field(self, key):
        """
        Retrieve an attribute as an InstrumentedAttribute.

        Parameters
        ----------
        key : Any
            The attribute key.

        Returns
        -------
        InstrumentedAttribute or None
            The retrieved attribute as an InstrumentedAttribute or None if not found.
        """

        if isinstance(field := getattr(self.__model, key, None), InstrumentedAttribute):
            return field

    def __slice(self, key):
        """
        Handle slicing operations on the query frame.

        Parameters
        ----------
        key : slice
            The slice object.

        Returns
        -------
        None
        """

        if not len(self.__sort):
            self.__sort = [key for key in self.__mapper.primary_key]
            if len(self.__select):
                match self.__select[0]:
                    case InstrumentedAttribute():
                        self.__sort = [self.__select[0]]
        start = 0
        stop = 0
        count = 0
        if (
            (isinstance(key.start, int) and key.start < 0)
            or (isinstance(key.stop, int) and key.stop < 0)
            or key.stop == None
        ):
            self.__limit = 0
            self.__offset = 0
            count = self.__build__().count()
            if key.start < 0:
                start = key.start + count
            else:
                start = key.start
            if key.stop == None or key.stop <= 0:
                stop = (key.stop or 0) + count
            else:
                stop = key.stop
        else:
            start = key.start or 0
            stop = key.stop or 0
        self.__limit = stop - start
        self.__offset = start
        
    def sort_values(self, *args):
        self.__sort = args

    def to_pandas(self):
        """
        Convert the query results to a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            The pandas DataFrame representing the query results.

        Raises
        ------
        Exception
            If pandas is not installed.
        """

        try:
            import pandas as pd
        except:
            raise Exception("Pandas is not installed.")
        return pd.read_sql(str(self), self.__session().connection())
