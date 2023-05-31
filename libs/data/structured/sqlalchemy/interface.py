from marshmallow import Schema
from sqlalchemy import Column
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import InstrumentedAttribute, Mapper, Query, Session
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList
from typing import Any
import logging


class QueryFrame:
    def __init__(self, model: Any, session: Session) -> None:
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
        for column in self.__mapper.columns.values():
            if column.primary_key:
                return column

    def __getitem__(self, key: Any = None):
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
        if key:
            return self.__session().query(self.__model).get(key)
        return self.__build__().all()

    def __len__(self) -> int:
        return self.__build__().count()

    def __repr__(self) -> str:
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
        if isinstance(field := getattr(self.__model, key, None), InstrumentedAttribute):
            return field

    def __slice(self, key):
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
        try:
            import pandas as pd
        except:
            raise Exception("Pandas is not installed.")
        return pd.read_sql(str(self), self.__session().connection())
