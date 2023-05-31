from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, SQLAlchemySchema
from marshmallow_sqlalchemy.convert import ModelConverter
from marshmallow_sqlalchemy.fields import Nested
from sqlalchemy.orm import Session, Relationship, RelationshipProperty
from typing import Any, Callable, List
try:
    import simplejson as json
except ImportError:
    import json


DEFAULT_CONVERTER = ModelConverter()
EXTENSION_STEPS: List[Callable] = [
    lambda model, session: setattr(
        model,
        "__marshmallow__",
        type(
            f"{model.__module__}.{model.__qualname__}",
            (SQLAlchemyAutoSchema,),
            {
                "Meta": type(
                    "Meta",
                    (object,),
                    {
                        "model": model,
                        "sqla_session": session,
                        "render_module": json
                    },
                )
            },
        ),
    ),
    lambda model, session: setattr(
        model,
        "__marshmallow__",
        type(
            f"{model.__module__}.{model.__qualname__}",
            (SQLAlchemySchema,),
            {
                **DEFAULT_CONVERTER.fields_for_model(model),
                **{
                    field_name: Nested(v.mapper.class_.__marshmallow__, depth=0)
                    if isinstance(v, Relationship)
                    else Nested(v.mapper.class_.__marshmallow__, depth=0, many=True)
                    for field_name, v in model.__mapper__.attrs.items()
                    if isinstance(v, RelationshipProperty)
                },
                "Meta": type(
                    "Meta",
                    (object,),
                    {
                        "model": model,
                        "sqla_session": session,
                        "render_module": json
                    },
                ),
            },
        ),
    ),
    lambda model, _: setattr(
        model, "__repr__", lambda self: self.__marshmallow__().dumps(self)
    ) if not hasattr(model, "__repr__") else None,
]


def extend_models(models: List[Any], session: Session):
    """
    Extend Marshmallow schemas for SQLAlchemy models.

    Parameters
    ----------
    models : List[Any]
        List of SQLAlchemy models to extend the schemas for.
    session : Session
        SQLAlchemy session object.

    """
    for func in EXTENSION_STEPS:
        for model in models:
            func(model, session)
