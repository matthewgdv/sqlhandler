from __future__ import annotations

from typing import Any, Union, Dict, TYPE_CHECKING, Type, cast

import sqlalchemy as alch
from sqlalchemy import Column, true, null, func
from sqlalchemy import types, event
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.orm import backref
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.orm.mapper import Mapper

from sqlalchemy.ext.declarative import declared_attr, DeclarativeMeta

from subtypes import Str

from .field import SubtypesDateTime
from .misc import absolute_namespace, CreateTableAccessor
from .relationship import Relationship
from .query import Query
from .table import Table

if TYPE_CHECKING:
    from sqlhandler.database import Metadata


class ModelMeta(DeclarativeMeta):
    _registry = set()

    __table__ = None
    __table_cls__ = Table
    metadata: Metadata

    def __new__(mcs, name: str, bases: tuple, namespace: dict) -> ModelMeta:
        if name == "BaseModel" and not bases:
            return type(name, bases, namespace)

        abs_ns = absolute_namespace(bases=bases, namespace=namespace)

        if relationships := {key: val for key, val in abs_ns.items() if isinstance(val, Relationship)}:
            table_name = type(name, (), abs_ns).__tablename__
            for attribute, relationship in relationships.items():
                relationship.build(table_name=table_name, bases=bases, namespace=namespace, attribute=attribute)

        cls = cast(Type[BaseModel], type.__new__(mcs, name, bases, namespace))
        cls._registry.add(cls)
        return cls

    def __repr__(cls) -> str:
        return cls.__name__ if cls.__table__ is None else f"{cls.__name__}({', '.join([f'{col.key}={type(col.type).__name__}' for col in cls.__table__.columns])})"

    def __getitem__(cls, item: str) -> InstrumentedAttribute:
        return getattr(cls, item)

    @property
    def query(cls: ModelMeta) -> Query:
        """Create a new Query operating on this class."""
        return cls.metadata.sql.session.query(cls)

    @property
    def create(cls: ModelMeta) -> CreateTableAccessor:
        """Create the table mapped to this class."""
        return CreateTableAccessor(cls)

    @property
    def c(cls: ModelMeta) -> ImmutableColumnCollection:
        """Access the columns (or a specific column if 'colname' is specified) of the underlying table."""
        return cls.__table__.c

    def alias(cls: ModelMeta, name: str, **kwargs: Any) -> AliasedClass:
        """Create a new class that is an alias of this one, with the given name."""
        return alch.orm.aliased(cls, name=name, **kwargs)

    def drop(cls: ModelMeta) -> None:
        """Drop the table mapped to this class."""
        cls.metadata.sql.database.drop_table(cls)


class BaseModel(metaclass=ModelMeta):
    """Custom base class for declarative and automap bases to inherit from. Represents a mapped table in a sql database."""
    __table__: Table
    metadata: Metadata
    __mapper__: Mapper

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{col.name}={repr(getattr(self, col.name))}' for col in type(self).__table__.columns])})"

    def insert(self) -> BaseModel:
        """Emit an insert statement for this object against this model's underlying table."""
        self.metadata.sql.session.add(self)
        return self

    def update(self, argdeltas: dict[Union[str, InstrumentedAttribute], Any] = None, /, **update_kwargs: Any) -> BaseModel:
        """
        Emit an update statement against database record represented by this object in this model's underlying table.
        This method positionally accepts a dict where the keys are the model's class attributes (of type InstrumentedAttribute) and the values are the values to update to.
        Alternatively, if the column names are known they may be set using keyword arguments. Raises AttributeError if invalid keys are provided.
        """
        updates: dict[str, Any] = {}

        clean_argdeltas = {} if argdeltas is None else {(name if isinstance(name, str) else name.key): val for name, val in argdeltas.items()}
        updates.update(clean_argdeltas)
        updates.update(update_kwargs)

        if difference := (set(updates) - set([attr.key for attr in self.__mapper__.all_orm_descriptors])):
            raise AttributeError(f"""Cannot perform update, '{type(self).__name__}' object has no attribute(s): {", ".join([f"'{unknown}'" for unknown in difference])}.""")

        if clean_argdeltas and update_kwargs:
            if intersection := (set(clean_argdeltas) & set(update_kwargs)):
                raise AttributeError(f"""Attribute(s) {", ".join([f"'{dupe}'" for dupe in intersection])} was/were provided twice.""")

        for key, val in updates.items():
            setattr(self, key, val)

        return self

    def delete(self) -> BaseModel:
        """Emit a delete statement for this object against this model's underlying table."""
        self.metadata.sql.session.delete(self)
        return self

    def clone(self, argdeltas: dict[Union[str, InstrumentedAttribute], Any] = None, **update_kwargs: Any) -> BaseModel:
        """Create a clone (new primary_key, but copies of all other attributes) of this object in the detached state. Model.insert() will be required to persist it to the database."""
        valid_cols = [col.name for col in self.__table__.columns if col.name not in self.__table__.primary_key.columns]
        return type(self)(**{col: getattr(self, col) for col in valid_cols}).update(argdeltas, **update_kwargs)


class Model(BaseModel):
    @declared_attr
    def __table_args__(cls):
        schema = cls.__dict__.get("__schema__", cls.metadata.sql.database.default_schema)
        return dict(schema=schema, is_declarative=True)


class AutoModel(Model):
    @declared_attr
    def __tablename__(cls):
        return str(Str(cls.__name__).case.snake())

    id = Column(types.Integer, primary_key=True)
    name = Column(types.String(50), nullable=True, server_default=null())

    @declared_attr
    def created(cls):
        return Column(SubtypesDateTime, nullable=False, server_default=func.NOW())

    @declared_attr
    def modified(cls):
        return Column(SubtypesDateTime, nullable=False, server_default=func.NOW(), onupdate=func.NOW())

    @declared_attr
    def active(cls):
        return Column(types.Boolean, nullable=False, server_default=true())


class ReflectedModel(BaseModel):
    pass


reserved_colnames = set(dir(ModelMeta))


@event.listens_for(Table, "column_reflect")
def on_column_reflect(inspector, table, column_info):
    if (key := column_info["name"]) in reserved_colnames:
        column_info["key"] = f"{key}_"
