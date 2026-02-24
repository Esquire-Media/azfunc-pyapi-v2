from sqlalchemy.orm import Session, Query, RelationshipProperty
from sqlalchemy.schema import ForeignKeyConstraint
from typing import Any, Callable, List, Optional, Union, Type
import re

EXTENSION_STEPS: List[Callable] = [
    lambda model, session: setattr(model, "__class_getitem__", model_get(session)),
    lambda model, _: setattr(model, "__getitem__", lambda self, key: getattr(self, key)),
    lambda model, _: setattr(model, "__setitem__", model_set_column),
]


def extend_models(models: List[Any], session: Session) -> None:
    """
    Extend the models with additional convenience behavior (indexing & item assignment).
    """
    for func in EXTENSION_STEPS:
        for model in models:
            func(model, session)


def model_set_column(self: object, key: str, value: Any) -> None:
    """
    Set a column value on the model and commit immediately.
    """
    setattr(self, key, value)
    self._sa_instance_state.session.commit()


from .interface import QueryFrame  # noqa: E402


def model_get(session: Session) -> Callable:
    """
    Provide __class_getitem__ to return a QueryFrame when indexing the model class.

    Example:  ModelClass[ModelClass.some_column == 1][:50]
    """

    @classmethod
    def __class_getitem__(cls, key) -> Union[Query, Any]:
        frame = QueryFrame(cls, session)
        return frame[key]

    return __class_getitem__


# ---------- Relationship naming helpers ----------

def _sanitize_identifier(value: str) -> str:
    """
    Convert an arbitrary string into a conservative Python identifier:
      - replace non-word chars with underscores
      - prefix underscore if the identifier would start with a digit
    """
    value = re.sub(r"\W+", "_", value or "")
    if not value:
        return "_"
    if re.match(r"^\d", value):
        value = f"_{value}"
    return value


def _camel_from_table(relname: str) -> str:
    """
    Convert a snake_case table name to a CamelCase class-like string.
    """
    if not relname:
        return ""
    return re.sub(r"\s+", "", re.sub(r"(?:^|_)(\w)", lambda m: m.group(1).upper(), relname))


def _fk_child_cols(constraint: ForeignKeyConstraint) -> List[str]:
    """
    Return the list of child (local) column names for the FK, in ordinal order.
    """
    try:
        # ForeignKeyConstraint.elements is an ordered collection (by ordinal)
        return [elem.parent.key for elem in (constraint.elements or [])]
    except Exception:
        return []


def _fk_suffix_by_child_cols(constraint: ForeignKeyConstraint) -> str:
    """
    Suffix string based on the child (local) FK column names to disambiguate multiple FKs.
    """
    cols = _fk_child_cols(constraint)
    return "_".join(cols) if cols else ""


def _explicit_label_from_comment(constraint: ForeignKeyConstraint) -> str | None:
    """
    If the FK has a COMMENT (not the name), use it verbatim (sanitized). This allows
    humans to force specific attribute names without relying on DB-generated names.
    """
    try:
        comment = getattr(constraint, "comment", None)
        if comment and str(comment).strip():
            return _sanitize_identifier(str(comment))
    except Exception:
        pass
    return None


def _ensure_unique_name(target_cls: Type[Any], base_name: str) -> str:
    """
    Ensure an attribute name is unique on `target_cls`.
    We also maintain a per-class reservation set so collisions are avoided
    even while SQLAlchemy is mid-configuration (before attrs appear on the class).
    """
    # Existing attributes and previously reserved names
    existing = set(dir(target_cls)) | set(getattr(target_cls, "__dict__", {}).keys())
    reserved: set[str] = set(getattr(target_cls, "__automap_reserved_relationship_names__", set()))
    existing |= reserved

    candidate = base_name
    i = 2
    while candidate in existing:
        candidate = f"{base_name}_{i}"
        i += 1

    # Reserve the chosen name immediately to avoid races/collisions
    reserved.add(candidate)
    setattr(target_cls, "__automap_reserved_relationship_names__", reserved)
    return candidate


def name_for_collection_relationship(
    base: Type[Any],
    local_cls: Type[Any],
    referred_cls: Type[Any],
    constraint: ForeignKeyConstraint,
) -> str:
    """
    Name for a *collection* relationship attribute.

    IMPORTANT: `local_cls` here is the class that will RECEIVE the collection
    attribute (i.e., where the backref will live for a many-to-one), and
    `referred_cls` is the class on the other side of the relationship.

    Strategy:
      1) If the FK has a COMMENT, honor it (explicit beats implicit).
      2) Otherwise: "collection_<LocalClassName>[_by_<child_fk_cols_joined>]"
         (keeps the existing public naming scheme to avoid breaking callers)
      3) Ensure uniqueness on **local_cls** (the receiver of the attribute).
    """
    # 1) explicit label via COMMENT
    explicit = _explicit_label_from_comment(constraint)
    if explicit:
        # Uniqueness must be enforced on the receiver of the attribute.
        return _ensure_unique_name(local_cls, explicit)

    # 2) descriptive scheme based on the class receiving the collection
    local_name = getattr(local_cls, "__name__", "") or _camel_from_table(
        getattr(getattr(local_cls, "__table__", None), "name", "")
    )
    local_name = _sanitize_identifier(local_name)

    suffix = _fk_suffix_by_child_cols(constraint)
    base_name = f"collection_{local_name}"
    if suffix:
        base_name = f"{base_name}_by_{suffix}"

    base_name = _sanitize_identifier(base_name)

    # 3) ensure uniqueness on the receiver class
    return _ensure_unique_name(local_cls, base_name)


def name_for_scalar_relationship(
    base: Type[Any],
    local_cls: Type[Any],
    referred_cls: Type[Any],
    constraint: ForeignKeyConstraint,
) -> str:
    """
    Name for a *scalar* relationship attribute (many-to-one) that will live on local_cls.

    Strategy:
      1) If the FK has a COMMENT, honor it.
      2) Otherwise: "related_<ReferredClassName>[_by_<child_fk_cols_joined>]"
      3) Ensure uniqueness on **local_cls**.
    """
    explicit = _explicit_label_from_comment(constraint)
    if explicit:
        return _ensure_unique_name(local_cls, explicit)

    referred_name = getattr(referred_cls, "__name__", "") or _camel_from_table(
        getattr(getattr(referred_cls, "__table__", None), "name", "")
    )
    referred_name = _sanitize_identifier(referred_name)

    suffix = _fk_suffix_by_child_cols(constraint)
    base_name = f"related_{referred_name}"
    if suffix:
        base_name = f"{base_name}_by_{suffix}"

    base_name = _sanitize_identifier(base_name)
    return _ensure_unique_name(local_cls, base_name)


def _find_relationship_key(
    source_cls: Type[Any],
    target_cls: Type[Any],
    *,
    uselist: Optional[bool] = None,
) -> str:
    """
    Return the relationship attribute name on source_cls that points to target_cls.

    If `uselist` is provided, it must match (True for collections, False for scalars).
    Raises a clear error if none or more than one matches.
    """
    matches: list[RelationshipProperty] = []
    for rel in source_cls.__mapper__.relationships:
        if rel.mapper.class_ is target_cls and (uselist is None or rel.uselist == uselist):
            matches.append(rel)

    if not matches:
        raise RuntimeError(
            f"No relationship found: {source_cls.__name__} -> {target_cls.__name__}"
            + (f" (uselist={uselist})" if uselist is not None else "")
        )
    if len(matches) > 1:
        keys = ", ".join(r.key for r in matches)
        raise RuntimeError(
            f"Ambiguous relationships on {source_cls.__name__} to {target_cls.__name__}: {keys}"
        )
    return matches[0].key
