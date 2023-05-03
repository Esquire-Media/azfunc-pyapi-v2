from .interface import QueryFrame
from .marshmallow import extend_models as extend_models_marshmallow, schema
from .utils import extend_models as extend_models_base, name_for_collection_relationship
from libs.utils.decorators import staticproperty

from sqlalchemy import create_engine, Engine
from sqlalchemy.ext.automap import automap_base, AutomapBase
from sqlalchemy.orm import Session
from typing import Any, Callable, List
import uuid

MODEL_EXTENSION_STEPS: List[Callable] = [
    extend_models_base,
    extend_models_marshmallow,
]


class SQLAlchemyStructuredProvider:
    @staticproperty
    def SUPPORTED_SCHEMES(self) -> list:
        return ["sql"]

    @staticproperty
    def RESOURCE_TYPE_DELIMITER(self) -> str:
        return "."

    @staticproperty
    def DEFAULT_SCHEMA(self) -> str:
        return "default"

    def get_schema(self, type_: str = "marshmallow") -> dict:
        match type_:
            case "marshmallow":
                return schema.marshmallow_schema_to_dict(self)

    def __init__(self, *args, **kwargs) -> None:
        kw = kwargs.keys()
        kwargs.pop("scheme")
        self.id: str = uuid.uuid4().hex
        self.schemas: List[str] = (
            kwargs.pop("schemas")
            if "schemas" in kw
            else [kwargs.pop("schema")]
            if "schema" in kw
            else []
        )
        self.engine: Engine = (
            kwargs.pop("engine")
            if "engine" in kw
            else create_engine(kwargs.pop("url"), **kwargs)
            if "url" in kw
            else None
        )
        if not self.engine:
            raise Exception("No engine configuration values specified.")

        self.base: AutomapBase = automap_base()
        self.session: Callable = lambda: Session(self.engine)

        for schema in self.schemas:
            self.base.prepare(
                autoload_with=self.engine,
                schema=schema,
                modulename_for_table=self.modulename_for_table,
                name_for_collection_relationship=name_for_collection_relationship,
            )
        self.models = self.base.by_module.get(self.id)

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
        selected = self.models
        for selector in handle.split(self.RESOURCE_TYPE_DELIMITER):
            selected = selected[selector]
        return QueryFrame(selected, self.session)

    def connect(self) -> Session:
        return self.session()

    def save(
        self,
        key: str,
        value: Any,
        schema_name: str = None,
        table_name: str = None,
        model: Any = None,
    ) -> None:
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
        session = self.session()
        if not model:
            if not table_name and not schema_name:
                schema_name, table_name, primary_key = self.parse_key(key)
            model = self.models[schema_name or self.DEFAULT_SCHEMA][table_name]
        return session.query(model).get(primary_key)

    def filter(
        self, filters: List[str], decoder: Callable = None, **kwargs
    ) -> List[Any]:
        pass

    def drop(
        self,
        key: str,
        schema_name: str = None,
        table_name: str = None,
        model: Any = None,
    ) -> None:
        session = self.session()
        if not model:
            if not table_name and not schema_name:
                schema_name, table_name, primary_key = self.parse_key(key)
            model = self.models[schema_name or self.DEFAULT_SCHEMA][table_name]
        session.delete(session.query(model).get(primary_key))
        session.commit()

    def parse_key(self, key: str):
        key = key.split(self.RESOURCE_TYPE_DELIMITER)
        schema_name = key[0]
        table_name = key[1]
        primary_key = ""
        if len(key) > 2:
            primary_key = key[2]
        return schema_name, table_name, primary_key

    def modulename_for_table(self, cls, tablename, table) -> str:
        return (
            f"{self.id}.{table.schema}"
            if getattr(table, "schema", None)
            else f"{self.id}.{self.DEFAULT_SCHEMA}"
        )
