from __future__ import annotations

from typing import Any, List, Union, TYPE_CHECKING

import pandas as pd
import sqlalchemy as alch
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.dialects.mssql import BIT
import sqlparse
import sqlparse.sql as sqltypes

from maybe import Maybe
from subtypes import Str, Frame, List_

from .utils import AlchemyBound, literalstatement
from .wrapper import ExpressionWrapper

if TYPE_CHECKING:
    from .alchemy import Alchemy


class Base:
    """Custom base class for declarative and automap bases to inherit from."""
    alchemy: Alchemy
    __table__: alch.Table
    columns: ImmutableColumnCollection

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{col.name}={repr(getattr(self, col.name))}' for col in type(self).__table__.columns])})"

    @classmethod
    def alias(cls, name: str, *args: Any, **kwargs: Any) -> AliasedClass:
        return alch.orm.aliased(cls, *args, name=name, **kwargs)

    @classmethod
    def join(cls, *args: Any, **kwargs: Any) -> Any:
        return cls.__table__.join(*args, **kwargs)

    @classmethod
    def joins(cls, *args: Any) -> alch.Table:
        table = cls.__table__
        for join in args:
            table = table.join(join)
        return table

    @classmethod
    def c(cls, colname: str = None) -> Union[ImmutableColumnCollection, alch.Column]:
        return cls.__table__.c if colname is None else cls.__table__.c[colname]

    def frame(self) -> Frame:
        return self.alchemy.orm_to_frame(self)

    def insert(self) -> Base:
        self.alchemy.session.add(self)
        return self


class Session(alch.orm.Session, AlchemyBound):
    """Custom subclass of sqlalchemy.orm.Session granting access to a custom Query class through the '.query()' method."""

    def __init__(self, *args: Any, alchemy: Alchemy = None, **kwargs: Any) -> None:
        self.alchemy = alchemy
        super().__init__(*args, **kwargs)

    def query(self, *args: Any) -> Query:
        """Return a custom subclass of sqlalchemy.orm.Query with additional useful methods and aliases for existing methods."""
        return Query(*args, alchemy=self.alchemy)

    def execute(self, *args: Any, autocommit: bool = False, **kwargs: Any) -> alch.engine.ResultProxy:
        res = super().execute(*args, **kwargs)
        if autocommit:
            self.commit()
        return res


class Query(alch.orm.Query):
    """Custom subclass of sqlalchemy.orm.Query with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, alchemy: Alchemy = None) -> None:
        self.alchemy = alchemy
        aslist = args[0] if len(args) == 1 and isinstance(args[0], list) else [*args]
        super().__init__(aslist, session=self.alchemy.session)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def frame(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Execute the query and return the result as a pandas DataFrame."""
        return self.alchemy.query_to_frame(self, *args, **kwargs)

    def scalar_col(self) -> list:
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


class Select(alch.sql.Select, AlchemyBound):
    """Custom subclass of sqlalchemy.sql.Select with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, alchemy: Alchemy = None, **kwargs: Any) -> None:
        self.alchemy = alchemy
        aslist = args[0] if len(args) == 1 and isinstance(args[0], list) else [*args]
        super().__init__(aslist, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def frame(self) -> pd.DataFrame:
        """Execute the query and return the result as a pandas DataFrame. If the Alchemy object's 'printing' attribute is True, the statement and returning table will be printed."""
        return ExpressionWrapper(self).frame

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        return literalstatement(self)

    def from_(self, *args: Any, **kwargs: Any) -> Select:
        """Simple alias for the 'select_from' method. See that method's docstring for documentation."""
        return self.select_from(*args, **kwargs)


class Update(alch.sql.Update, AlchemyBound):
    """Custom subclass of sqlalchemy.sql.Update with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, alchemy: Alchemy = None, **kwargs: Any) -> None:
        self.alchemy = alchemy
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def resolve(self, silently: bool = False) -> None:
        ExpressionWrapper(self, silently=silently)

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        return literalstatement(self)

    def set_(self, *args: Any, **kwargs: Any) -> Update:
        """Simple alias for the 'values' method. See that method's docstring for documentation."""
        return self.values(*args, **kwargs)


class Insert(alch.sql.Insert, AlchemyBound):
    """Custom subclass of sqlalchemy.sql.Insert with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, alchemy: Alchemy = None, **kwargs: Any) -> None:
        self.alchemy = alchemy
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def resolve(self, silently: bool = False) -> None:
        ExpressionWrapper(self, silently=silently)

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        literal = literalstatement(self)
        if self.select is not None:
            return literal
        else:
            return self._align_values_insert(literal)

    def values(self, *args: Any, **kwargs: Any) -> Insert:
        ret = super().values(*args, **kwargs)
        if isinstance(ret.parameters, list):
            ret.parameters = [{(col.key if isinstance(col, InstrumentedAttribute) else col): (Maybe(val).else_(alch.null()))
                               for col, val in record.items()} for record in ret.parameters]
        return ret

    @staticmethod
    def _align_values_insert(literal: str) -> str:
        def extract_parentheses(text: str) -> List[List[str]]:
            def nested_list_of_vals_from_paren(paren: sqltypes.Parenthesis) -> List[List[str]]:
                targets = [item for item in paren if not any([not item.value.strip(), item.value in (",", "(", ")")])]
                values = [[item.value for item in target if not any([not item.value.strip(), item.value in (",", "(", ")")])] if isinstance(target, sqltypes.IdentifierList)
                          else target.value
                          for target in targets]

                vals = List_(values).flatten()
                return [vals]

            parser = sqlparse.parse(text)[0]
            func, = [item for item in parser if isinstance(item, sqltypes.Function)]
            headerparen, = [item for item in func if isinstance(item, sqltypes.Parenthesis)]
            headers = nested_list_of_vals_from_paren(headerparen)

            parens = [item for item in parser if isinstance(item, sqltypes.Parenthesis)]
            values: List[List[str]] = sum([nested_list_of_vals_from_paren(paren) for paren in parens], [])
            return headers + values

        start = Str(literal).before_first(r"\(")
        sublists = extract_parentheses(literal)
        for sublist in sublists:
            sublist[0] = f"({sublist[0]}"
            sublist[-1] = f"{sublist[-1]})"

        formatted_sublists = List_(sublists).align_nested(fieldsep=", ", linesep=",\n").split("\n")
        formatted_sublists[0] = f"{start}{formatted_sublists[0][:-1]}"
        formatted_sublists[1] = f"VALUES{' ' * (len(start) - 6)}{formatted_sublists[1]}"

        if len(formatted_sublists) > 2:
            for index, sublist in enumerate(formatted_sublists[2:]):
                formatted_sublists[index + 2] = f"{' ' * (len(start))}{sublist}"
        final = '\n'.join(formatted_sublists) + ";"
        return final


class Delete(alch.sql.Delete, AlchemyBound):
    """Custom subclass of sqlalchemy.sql.Delete with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, alchemy: Alchemy = None, **kwargs: Any) -> None:
        self.alchemy = alchemy
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def resolve(self) -> None:
        ExpressionWrapper(self)

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        return literalstatement(self)


class SelectInto(alch.sql.Select, AlchemyBound):
    """Custom subclass of sqlalchemy.sql.Select for 'SELECT * INTO #tmp' syntax with additional useful methods and aliases for existing methods."""

    def __init__(self, columns: list, *arg: Any, table: str = None, schema: str = None, alchemy: Alchemy = None, **kw: Any) -> None:
        self.alchemy = alchemy
        self.into = f"{schema or 'dbo'}.{table}"
        super().__init__(columns, *arg, **kw)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def resolve(self) -> None:
        ExpressionWrapper(self)

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        return literalstatement(self)

    def execute(self, autocommit: bool = False) -> str:
        """Execute this query's statement in the current session."""
        res = self.alchemy.session.execute(self)
        if autocommit:
            self.alchemy.session.commit()
        return res


@compiles(SelectInto)  # type:ignore
def s_into(element: Any, compiler: Any, **kw: Any) -> Any:
    text = compiler.visit_select(element, **kw)
    text = text.replace("FROM", f"INTO {element.into} \nFROM")
    return text


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
