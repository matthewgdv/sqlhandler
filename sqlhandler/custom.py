from __future__ import annotations

from typing import Any, Union, Dict, TYPE_CHECKING

import pandas as pd
import sqlalchemy as alch
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.dialects.mssql import BIT

from .utils import literalstatement

if TYPE_CHECKING:
    from .sql import Sql


class Model:
    """Custom base class for declarative and automap bases to inherit from. Represents a mapped table in a sql database."""
    sql: Sql
    __instrumented_attributes: Dict[str, InstrumentedAttribute] = None

    __table__: alch.Table
    columns: ImmutableColumnCollection

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{col.name}={repr(getattr(self, col.name))}' for col in type(self).__table__.columns])})"

    @classmethod
    def alias(cls, name: str, *args: Any, **kwargs: Any) -> AliasedClass:
        """Create a new class that is an alias of this one, with the given name."""
        return alch.orm.aliased(cls, *args, name=name, **kwargs)

    @classmethod
    def join(cls, *args: Any, **kwargs: Any) -> Any:
        """Create a join between the table belonging to this class and another model."""
        return cls.__table__.join(*args, **kwargs)

    @classmethod
    def c(cls, colname: str = None) -> Union[ImmutableColumnCollection, alch.Column]:
        """Access the columns (or a specific column if 'colname' is specified) of the underlying table."""
        return cls.__table__.c if colname is None else cls.__table__.c[colname]

    @classmethod
    def query(cls) -> Query:
        """Create a new Query operating on this class."""
        return cls.sql.session.query(cls)

    @classmethod
    def create(cls) -> None:
        """Create the table mapped to this class."""
        cls.sql.create_table(cls)

    @classmethod
    def drop(cls) -> None:
        """Drop the table mapped to this class."""
        cls.sql.drop_table(cls)

    @classmethod
    def _instrumented_attributes(cls) -> Dict[str]:
        if cls.__instrumented_attributes is None:
            cls.__instrumented_attributes = {key: val for key, val in vars(cls).items() if isinstance(val, InstrumentedAttribute)}

        return cls.__instrumented_attributes

    def insert(self) -> Model:
        """Emit an insert statement for this object against this model's underlying table."""
        self.sql.session.add(self)
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

        difference = set(updates) - set(self._instrumented_attributes())
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
        self.sql.session.delete(self)
        return self

    def clone(self, *args: Any, **kwargs: Any) -> Model:
        """Create a clone (new primary_key, but copies of all other attributes) of this object in the detached state. Model.insert() will be required to persist it to the database."""
        valid_cols = [col.name for col in self.__table__.columns if col.name not in self.__table__.primary_key.columns]
        return type(self)(**{col: getattr(self, col) for col in valid_cols}).update(*args, **kwargs)


class Session(alch.orm.Session):
    """Custom subclass of sqlalchemy.orm.Session granting access to a custom Query class through the '.query()' method."""

    def __init__(self, *args: Any, sql: Sql = None, **kwargs: Any) -> None:
        self.sql = sql
        super().__init__(*args, **kwargs)

    def query(self, *args: Any) -> Query:
        """Return a custom subclass of sqlalchemy.orm.Query with additional useful methods and aliases for existing methods."""
        return Query(*args, sql=self.sql)

    def execute(self, *args: Any, autocommit: bool = False, **kwargs: Any) -> alch.engine.ResultProxy:
        """Execute an valid object against this Session. If 'autocommit=True' is passed, the transaction will be commited if the statement completes without errors."""
        res = super().execute(*args, **kwargs)
        if autocommit:
            self.commit()
        return res


class Query(alch.orm.Query):
    """Custom subclass of sqlalchemy.orm.Query with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, sql: Sql = None) -> None:
        self.sql = sql
        aslist = args[0] if len(args) == 1 and isinstance(args[0], list) else [*args]
        super().__init__(aslist, session=self.sql.session)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def frame(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Execute the query and return the result as a pandas DataFrame."""
        return self.sql.query_to_frame(self, *args, **kwargs)

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

    def from_(self, *args: Any, **kwargs: Any) -> Query:
        """Simple alias for the 'select_from' method. See that method's docstring for documentation."""
        return self.select_from(*args, **kwargs)

    def where(self, *args: Any, **kwargs: Any) -> Query:
        """Simple alias for the 'filter' method. See that method's docstring for documentation."""
        return self.filter(*args, **kwargs)

    def set_(self, *args: Any, synchronize_session: Any = "fetch", **kwargs: Any) -> int:
        """Simple alias for the '.update()' method, with the default 'synchronize_session' argument set to 'fetch', rather than 'evaluate'. Check that method for documentation."""
        return self.update(*args, synchronize_session=synchronize_session, **kwargs)


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
