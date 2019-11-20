from __future__ import annotations

from typing import Any, Union, Dict

import pandas as pd

import sqlalchemy as alch

from sqlalchemy.schema import CreateTable

from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.orm.attributes import InstrumentedAttribute

from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.dialects.mssql import BIT
from sqlalchemy.ext.declarative import declared_attr, DeclarativeMeta

from sqlalchemy import Column, true, null, func
from sqlalchemy.types import Integer, String, Boolean, DateTime

from subtypes import Str

from .utils import literalstatement


class ModelMeta(DeclarativeMeta):
    def __repr__(cls) -> str:
        return repr(cls.__table__)

    def __str__(cls) -> str:
        return str(CreateTable(cls.__table__)).strip()

    @property
    def query(cls: Model) -> Query:
        """Create a new Query operating on this class."""
        return cls.metadata.bind.sql.session.query(cls)

    @property
    def c(cls: Model) -> ImmutableColumnCollection:
        """Access the columns (or a specific column if 'colname' is specified) of the underlying table."""
        return cls.__table__.c

    def alias(cls: Model, name: str, **kwargs: Any) -> AliasedClass:
        """Create a new class that is an alias of this one, with the given name."""
        return alch.orm.aliased(cls, name=name, **kwargs)

    def create(cls: Model) -> None:
        """Create the table mapped to this class."""
        cls.metadata.sql.create_table(cls)

    def drop(cls: Model) -> None:
        """Drop the table mapped to this class."""
        cls.metadata.sql.drop_table(cls)


class Model:
    """Custom base class for declarative and automap bases to inherit from. Represents a mapped table in a sql database."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{col.name}={repr(getattr(self, col.name))}' for col in type(self).__table__.columns])})"

    def insert(self) -> Model:
        """Emit an insert statement for this object against this model's underlying table."""
        self.metadata.sql.session.add(self)
        return self

    def update(self, argdeltas: Dict[Union[str, InstrumentedAttribute], Any] = None, **update_kwargs: Any) -> Model:
        """
        Emit an update statement against database record represented by this object in this model's underlying table.
        This method positionally accepts a dict where the keys are the model's class attributes (of type InstrumentedAttribute) and the values are the values to update to.
        Alternatively, if the column names are known they may be set using keyword arguments. Raises AttributeError if invalid keys are provided.
        """
        updates: Dict[str, Any] = {}

        clean_argdeltas = {} if argdeltas is None else {(name if isinstance(name, str) else name.key): val for name, val in argdeltas.items()}
        updates.update(clean_argdeltas)
        updates.update(update_kwargs)

        difference = set(updates) - set([attr.key for attr in self.__mapper__.all_orm_descriptors])
        if difference:
            raise AttributeError(f"""Cannot perform update, '{type(self).__name__}' object has no attribute(s): {", ".join([f"'{unknown}'" for unknown in difference])}.""")

        if clean_argdeltas and update_kwargs:
            intersection = set(clean_argdeltas) & set(update_kwargs)
            if intersection:
                raise AttributeError(f"""Attribute(s) {", ".join([f"'{dupe}'" for dupe in intersection])} was/were provided twice.""")

        for key, val in updates.items():
            setattr(self, key, val)

        return self

    def delete(self) -> Model:
        """Emit a delete statement for this object against this model's underlying table."""
        self.metadata.sql.session.delete(self)
        return self

    def clone(self, argdeltas: Dict[Union[str, InstrumentedAttribute], Any] = None, **update_kwargs: Any) -> Model:
        """Create a clone (new primary_key, but copies of all other attributes) of this object in the detached state. Model.insert() will be required to persist it to the database."""
        valid_cols = [col.name for col in self.__table__.columns if col.name not in self.__table__.primary_key.columns]
        return type(self)(**{col: getattr(self, col) for col in valid_cols}).update(argdeltas, **update_kwargs)


class AutoModel(Model):
    @declared_attr
    def __tablename__(cls):
        return str(Str(cls.__name__).case.snake())

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=True, server_default=null())

    @declared_attr
    def active(cls):
        return Column(Boolean, nullable=False, server_default=true())

    @declared_attr
    def created(cls):
        return Column(DateTime, nullable=False, server_default=func.NOW())

    @declared_attr
    def modified(cls):
        return Column(DateTime, nullable=False, server_default=func.NOW(), onupdate=func.NOW())


class Session(alch.orm.Session):
    """Custom subclass of sqlalchemy.orm.Session granting access to a custom Query class through the '.query()' method."""

    def query(self, *entities: Any) -> Query:
        return super().query(*entities)

    def execute(self, *args: Any, autocommit: bool = False, **kwargs: Any) -> alch.engine.ResultProxy:
        """Execute an valid object against this Session. If 'autocommit=True' is passed, the transaction will be commited if the statement completes without errors."""
        res = super().execute(*args, **kwargs)
        if autocommit:
            self.commit()
        return res


class Query(alch.orm.Query):
    """Custom subclass of sqlalchemy.orm.Query with additional useful methods and aliases for existing methods."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def frame(self, labels: bool = False) -> pd.DataFrame:
        """Execute the query and return the result as a pandas DataFrame."""
        return self.session.bind.sql.query_to_frame(self, labels=labels)

    def vector(self) -> list:
        """Transpose all records in a single column into a list. If the query returns more than one column, this will raise a RuntimeError."""
        vals = self.all()
        if all([len(row) == 1 for row in vals]):
            return [row[0] for row in vals]
        else:
            raise RuntimeError("Multiple columns selected. Expected exactly one value per row, got multiple.")

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        return literalstatement(self)

    def from_(self, *from_obj: Any) -> Query:
        """Simple alias for the 'select_from' method. See that method's docstring for documentation."""
        return self.select_from(*from_obj)

    def where(self, *criterion: Any) -> Query:
        """Simple alias for the 'filter' method. See that method's docstring for documentation."""
        return self.filter(*criterion)

    def update(self, values: Any, synchronize_session: Any = "fetch", update_args: dict = None) -> int:
        """Simple alias for the '.update()' method, with the default 'synchronize_session' argument set to 'fetch', rather than 'evaluate'. Check that method for documentation."""
        return super().update(values, synchronize_session=synchronize_session)


class StringLiteral(alch.sql.sqltypes.String):
    def literal_processor(self, dialect: Any) -> Any:
        super_processor = super().literal_processor(dialect)

        def process(value: Any) -> Any:
            if value is None:
                return "NULL"

            result = super_processor(str(value))
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)

            return result
        return process


class BitLiteral(BIT):
    def literal_processor(self, dialect: Any) -> Any:
        super_processor = super().literal_processor(dialect)

        def process(value: Any) -> Any:
            if isinstance(value, bool):
                return str(1) if value else str(0)
            elif isinstance(value, int) and value in (0, 1):
                return value
            else:
                return super_processor(value)

        return process
