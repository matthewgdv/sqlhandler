from __future__ import annotations

from typing import Any, TYPE_CHECKING, Optional

import sqlalchemy as alch
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.ext.compiler import compiles

from maybe import Maybe
from subtypes import Frame
from miscutils import is_non_string_iterable

from sqlhandler.utils.utils import literal_statement


if TYPE_CHECKING:
    from sqlhandler import Sql


class ExpressionMixin:
    """A mixin providing private methods for logging using expression classes."""
    bind: Any
    table: Any
    select: Any
    into: Any
    _whereclause: Any

    @property
    def sql(self) -> Sql:
        return self.bind.sql

    def execute(self, autocommit: bool = False) -> str:
        """Execute this query's statement in the current session."""
        res = self.sql.session.execute(self)
        if autocommit:
            self.sql.session.commit()
        return res

    def _prepare_tran(self) -> None:
        self.sql.session.rollback()
        self.sql.log.write_sql(f"{'-' * 200}\n\nBEGIN TRAN;", add_newlines=2)

    def _resolve_tran(self, force_commit: bool = False) -> None:
        if self.sql.autocommit or force_commit:
            self.sql.session.commit()
            self.sql.log.write_sql("COMMIT;", add_newlines=2)
        else:
            user_confirmation = input("\nIf you are happy with the above Query/Queries please type COMMIT. Anything else will roll back the ongoing Transaction.\n\n")
            if user_confirmation.upper() == "COMMIT":
                self.sql.session.commit()
                self.sql.log.write_sql("COMMIT;", add_newlines=2)
            else:
                self.sql.session.rollback()
                self.sql.log.write_sql("ROLLBACK;", add_newlines=2)

    def _perform_pre_select(self, silently: bool) -> Optional[Select]:
        if silently:
            return
        else:
            pre_select_object = self.sql.Select(["*"]).from_(self.table)

            if self._whereclause is not None:
                pre_select_object = pre_select_object.where(self._whereclause)

            (pre_select_object.frame if silently else pre_select_object.resolve)()
            return pre_select_object

    def _perform_post_select(self, pre_select_object: Select, silently: bool) -> None:
        if not silently:
            pre_select_object.resolve()

    def _perform_pre_select_from_select(self, silently: bool) -> int:
        return None if self.select is None else len((self.select.frame if silently else self.select.resolve)().index)

    def _execute_expression_and_determine_rowcount(self, rowcount: int = None) -> int:
        result = self.sql.session.execute(self)
        self.sql.log.write_sql(str(self), add_newlines=2)

        if rowcount is None:
            rowcount = result.rowcount

        if rowcount == -1:
            if isinstance(self, Insert):
                if self.select is None:
                    rowcount = len(self.parameters) if isinstance(self.parameters, list) else 1

        self.sql.log.write_comment(f"({rowcount} row(s) affected)", add_newlines=2)

        return rowcount

    def _perform_post_select_inserts(self, rowcount: int, silently: bool) -> None:
        if not silently:
            self.sql.Select(["*"]).select_from(self.table).order_by(getattr(self.table.columns, list(self.table.primary_key)[0].name).desc()).limit(rowcount).resolve()

    def _perform_post_select_all(self, silently: bool) -> None:
        if not silently:
            self.sql.Select(["*"]).select_from(self.sql.text(f"{self.into}")).resolve()


class Select(alch.sql.Select):
    """Custom subclass of sqlalchemy.sql.Select with additional useful methods and aliases for existing methods."""

    def __init__(self, *columns: Any, whereclause: Any = None, from_obj: Any = None, distinct: Any = False, having: Any = None, correlate: Any = True, prefixes: Any = None, suffixes: Any = None, **kwargs: Any) -> None:
        as_single_iterable = columns[0] if len(columns) == 1 and is_non_string_iterable(columns[0]) else [*columns]
        super().__init__(columns=as_single_iterable, whereclause=whereclause, from_obj=from_obj, distinct=distinct, having=having, correlate=correlate, prefixes=prefixes, suffixes=suffixes, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)

    def frame(self) -> Frame:
        """Execute the query and return the result as a subtypes.Frame."""
        return self._select_to_frame()

    def resolve(self) -> Frame:
        """Convert this query into a subtypes.Frame and write an ascii representation of it to the log, then return it."""
        frame = self._select_to_frame()
        self.bind.sql.log.write_sql(str(self), add_newlines=2)
        self.bind.sql.log.write_comment(frame.applymap(lambda val: 1 if val is True else (0 if val is False else ("NULL" if val is None else val))).to_ascii(), add_newlines=2)
        return frame

    def from_(self, *args: Any, **kwargs: Any) -> Select:
        """Simple alias for the 'select_from' method. See that method's docstring for documentation."""
        return self.select_from(*args, **kwargs)

    def _select_to_frame(self) -> Frame:
        result = self.bind.sql.session.execute(self)
        cols = [col[0] for col in result.cursor.description]
        return Frame(result.fetchall(), columns=cols)


class Update(ExpressionMixin, alch.sql.Update):
    """Custom subclass of sqlalchemy.sql.Update with additional useful methods and aliases for existing methods."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)

    def resolve(self, silently: bool = False) -> None:
        """Execute this statement with surrounding Select statements as applicable, and request user confirmation to commit if Sql.autocommit is False, else commit the transaction."""
        self._prepare_tran()
        pre_select_object = self._perform_pre_select(silently=silently)
        self._execute_expression_and_determine_rowcount()
        self._perform_post_select(pre_select_object=pre_select_object, silently=silently)
        self._resolve_tran()

    def set_(self, *args: Any, **kwargs: Any) -> Update:
        """Simple alias for the 'values' method. See that method's docstring for documentation."""
        return self.values(*args, **kwargs)


class Insert(ExpressionMixin, alch.sql.Insert):
    """Custom subclass of sqlalchemy.sql.Insert with additional useful methods and aliases for existing methods."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)

    def resolve(self, silently: bool = False) -> None:
        """Execute this statement with surrounding Select statements as applicable, and request user confirmation to commit if Sql.autocommit is False, else commit the transaction."""
        self._prepare_tran()
        rowcount = self._perform_pre_select_from_select(silently=silently)
        rowcount = self._execute_expression_and_determine_rowcount(rowcount=rowcount)
        self._perform_post_select_inserts(rowcount=rowcount, silently=silently)
        self._resolve_tran()

    def values(self, *args: Any, **kwargs: Any) -> Insert:
        """Insert the given values as either a single dict, or a list of dicts."""
        ret = super().values(*args, **kwargs)
        if isinstance(ret.parameters, list):
            ret.parameters = [{(col.key if isinstance(col, InstrumentedAttribute) else col): (Maybe(val).else_(alch.null()))
                               for col, val in record.items()} for record in ret.parameters]
        return ret


class Delete(ExpressionMixin, alch.sql.Delete):
    """Custom subclass of sqlalchemy.sql.Delete with additional useful methods and aliases for existing methods."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)

    def resolve(self, silently: bool = False) -> None:
        """Execute this statement with surrounding Select statements as applicable, and request user confirmation to commit if Sql.autocommit is False, else commit the transaction."""
        self._prepare_tran()
        self._perform_pre_select(silently=silently)
        self._execute_expression_and_determine_rowcount()
        self._resolve_tran()


class SelectInto(ExpressionMixin, alch.sql.Select):
    """Custom subclass of sqlalchemy.sql.Select for 'SELECT * INTO #tmp' syntax with additional useful methods and aliases for existing methods."""

    def __init__(self, columns: list, *args: Any, table: str = None, schema: str = None, **kwargs: Any) -> None:
        self.into = f"{schema or 'dbo'}.{table}"
        super().__init__(columns, *args, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)

    def resolve(self, silently: bool = False) -> None:
        """Execute this statement with surrounding Select statements as applicable, and request user confirmation to commit if Sql.autocommit is False, else commit the transaction."""
        self._prepare_tran()
        self._execute_expression_and_determine_rowcount()
        self._perform_post_select_all(silently=silently)
        self._resolve_tran(force_commit=True)


@compiles(SelectInto)  # type:ignore
def s_into(element: Any, compiler: Any, **kw: Any) -> Any:
    text = compiler.visit_select(element, **kw)
    text = text.replace("FROM", f"INTO {element.into} \nFROM")
    return text
