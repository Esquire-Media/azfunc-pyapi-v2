from sqlalchemy.orm import Session, Query
from sqlalchemy.schema import ForeignKeyConstraint
from typing import Any, Callable, List, Union, Type

EXTENSION_STEPS: List[Callable] = [
    lambda model, session: setattr(model, "__class_getitem__", model_get(session)),
    lambda model, _: setattr(
        model, "__getitem__", lambda self, key: getattr(self, key)
    ),
    lambda model, _: setattr(model, "__setitem__", model_set_column),
]


def extend_models(models: List[Any], session: Session) -> None:
    """
    Extend the models with additional functionality.

    Parameters
    ----------
    models : List[Any]
        The list of models to extend.
    session : Session
        The SQLAlchemy Session object.

    Returns
    -------
    None
    """

    for func in EXTENSION_STEPS:
        for model in models:
            func(model, session)


def model_set_column(self: object, key: str, value: Any) -> None:
    """
    Set a column value on the model.

    Parameters
    ----------
    self : object
        The model object.
    key : str
        The column name.
    value : Any
        The value to set.

    Returns
    -------
    None
    """

    setattr(self, key, value)
    self._sa_instance_state.session.commit()


from .interface import QueryFrame


def model_get(session: Session) -> Callable:
    """
    Get the __class_getitem__ method for the model.

    Parameters
    ----------
    session : Session
        The SQLAlchemy Session object.

    Returns
    -------
    Callable
        The __class_getitem__ method.
    """

    @classmethod
    def __class_getitem__(cls, key) -> Union[Query, Any]:
        frame = QueryFrame(cls, session)
        return frame[key]

    return __class_getitem__


import logging


def name_for_collection_relationship(
    base: Type[Any],
    local_cls: Type[Any],
    referred_cls: Type[Any],
    constraint: ForeignKeyConstraint,
):
    """
    Get the name for the collection relationship.

    Parameters
    ----------
    base : Type[Any]
        The base class.
    local_cls : Type[Any]
        The local class.
    referred_cls : Type[Any]
        The referred class.
    constraint : ForeignKeyConstraint
        The foreign key constraint.

    Returns
    -------
    str
        The name for the collection relationship.
    """

    if constraint.comment:
        return f"{constraint.comment.lower()}"
    if constraint.name:
        return f"{constraint.name.lower()}"
