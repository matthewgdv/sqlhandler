from __future__ import annotations

from typing import Any, List, TYPE_CHECKING

import pandas as pd
import sqlalchemy as alch
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.ext.compiler import compiles
import sqlparse
import sqlparse.sql as sqltypes

from maybe import Maybe
from subtypes import Str, Frame, List_

from .utils import SqlBoundMixin, literalstatement


if TYPE_CHECKING:
    from .sql import Sql


class ExpressionMixin:
    def _prepare_tran(self) -> None:
        self.sql.session.rollback()
        self.sql.log.write(f"{'-' * 200}\n\nBEGIN TRAN;", add_newlines=2)

    def _resolve_tran(self, force_commit: bool = False) -> None:
        """Request user confirmation to resolve the ongoing transaction."""
        if self.sql.autocommit or force_commit:
            self.sql.session.commit()
            self.sql.log.write("COMMIT;", add_newlines=2)
        else:
            user_confirmation = input("\nIf you are happy with the above Query/Queries please type COMMIT. Anything else will roll back the ongoing Transaction.\n\n")
            if user_confirmation.upper() == "COMMIT":
                self.sql.session.commit()
                self.sql.log.write("COMMIT;", add_newlines=2)
            else:
                self.sql.session.rollback()
                self.sql.log.write("ROLLBACK;", add_newlines=2)

    def _perform_pre_select(self, silently: bool) -> None:
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

    def _perform_pre_select_from_select(self, silently: bool) -> None:
        return None if self.select is None else len((self.select.frame if silently else self.select.resolve)().index)

    def _execute_expression_and_determine_rowcount(self, rowcount: int = None) -> None:
        result = self.sql.session.execute(self)
        self.sql.log.write(str(self), add_newlines=2)

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
            self.sql.Select(["*"]).select_from(self.sql.Text(f"{self.into}")).resolve()


class Select(alch.sql.Select, SqlBoundMixin):
    """Custom subclass of sqlalchemy.sql.Select with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, sql: Sql = None, **kwargs: Any) -> None:
        self.sql = sql
        aslist = args[0] if len(args) == 1 and isinstance(args[0], list) else [*args]
        super().__init__(aslist, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def frame(self) -> pd.DataFrame:
        """Execute the query and return the result as a pandas DataFrame. If the Sql object's 'printing' attribute is True, the statement and returning table will be printed."""
        return self._select_to_frame()

    def resolve(self) -> pd.DataFrame:
        frame = self._select_to_frame()
        self.sql.log.write(str(self), add_newlines=2)
        self.sql.log.write_comment(frame.applymap(lambda val: 1 if val is True else (0 if val is False else ("NULL" if val is None else val))).to_ascii(), add_newlines=2)
        return frame

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        return literalstatement(self)

    def from_(self, *args: Any, **kwargs: Any) -> Select:
        """Simple alias for the 'select_from' method. See that method's docstring for documentation."""
        return self.select_from(*args, **kwargs)

    def _select_to_frame(self) -> None:
        result = self.sql.session.execute(self)
        cols = [col[0] for col in result.cursor.description]
        return Frame(result.fetchall(), columns=cols)


class Update(alch.sql.Update, SqlBoundMixin, ExpressionMixin):
    """Custom subclass of sqlalchemy.sql.Update with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, sql: Sql = None, **kwargs: Any) -> None:
        self.sql = sql
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def resolve(self, silently: bool = False) -> None:
        self._prepare_tran()
        pre_select_object = self._perform_pre_select(silently=silently)
        self._execute_expression_and_determine_rowcount()
        self._perform_post_select(pre_select_object=pre_select_object, silently=silently)
        self._resolve_tran()

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        return literalstatement(self)

    def set_(self, *args: Any, **kwargs: Any) -> Update:
        """Simple alias for the 'values' method. See that method's docstring for documentation."""
        return self.values(*args, **kwargs)


class Insert(alch.sql.Insert, SqlBoundMixin, ExpressionMixin):
    """Custom subclass of sqlalchemy.sql.Insert with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, sql: Sql = None, **kwargs: Any) -> None:
        self.sql = sql
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def resolve(self, silently: bool = False) -> None:
        self._prepare_tran()
        rowcount = self._perform_pre_select_from_select(silently=silently)
        rowcount = self._execute_expression_and_determine_rowcount(rowcount=rowcount)
        self._perform_post_select_inserts(rowcount=rowcount, silently=silently)
        self._resolve_tran()

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        literal = literalstatement(self)
        return literal
        # TODO: fix the alignment
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

        start = Str(literal).slice.before_first(r"\(")
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


class Delete(alch.sql.Delete, SqlBoundMixin, ExpressionMixin):
    """Custom subclass of sqlalchemy.sql.Delete with additional useful methods and aliases for existing methods."""

    def __init__(self, *args: Any, sql: Sql = None, **kwargs: Any) -> None:
        self.sql = sql
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def resolve(self, silently: bool = False) -> None:
        self._prepare_tran()
        self._perform_pre_select(silently=silently)
        self._execute_expression_and_determine_rowcount()
        self._resolve_tran()

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        return literalstatement(self)


class SelectInto(alch.sql.Select, SqlBoundMixin, ExpressionMixin):
    """Custom subclass of sqlalchemy.sql.Select for 'SELECT * INTO #tmp' syntax with additional useful methods and aliases for existing methods."""

    def __init__(self, columns: list, *arg: Any, table: str = None, schema: str = None, sql: Sql = None, **kw: Any) -> None:
        self.sql = sql
        self.into = f"{schema or 'dbo'}.{table}"
        super().__init__(columns, *arg, **kw)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return self.literal()

    def resolve(self, silently: bool = False) -> None:
        self._prepare_tran()
        self._execute_expression_and_determine_rowcount()
        self._perform_post_select_all(silently=silently)
        self._resolve_tran(force_commit=True)

    def literal(self) -> str:
        """Returns this query's statement as raw SQL with inline literal binds."""
        return literalstatement(self)

    def execute(self, autocommit: bool = False) -> str:
        """Execute this query's statement in the current session."""
        res = self.sql.session.execute(self)
        if autocommit:
            self.sql.session.commit()
        return res


@compiles(SelectInto)  # type:ignore
def s_into(element: Any, compiler: Any, **kw: Any) -> Any:
    text = compiler.visit_select(element, **kw)
    text = text.replace("FROM", f"INTO {element.into} \nFROM")
    return text
