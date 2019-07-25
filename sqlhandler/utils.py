from __future__ import annotations

from typing import Any, List, Callable, TypeVar, TYPE_CHECKING

from abc import ABC, abstractmethod
import sqlalchemy as alch
import sqlalchemy.sql.sqltypes
import sqlparse
from sqlalchemy.orm import Query
from pyodbc import ProgrammingError

from subtypes import Frame, Str
from pathmagic import File, PathLike

if TYPE_CHECKING:
    from .sql import Sql


SelfType = TypeVar("SelfType")


class SqlBoundMixin:
    def __init__(self, *args: Any, sql: Sql = None, **kwargs: Any) -> None:
        self.sql = sql

    @classmethod
    def from_sql(cls: SelfType, sql: Sql) -> Callable[[...], SelfType]:
        def wrapper(*args: Any, **kwargs: Any) -> SqlBoundMixin:
            return cls(*args, sql=sql, **kwargs)
        return wrapper


class Executable(SqlBoundMixin, ABC):
    def __init__(self, sql: Sql = None) -> None:
        self.sql = sql
        self.cursor = self.sql.engine.raw_connection().cursor()
        self.args, self.kwargs = (), {}

        self.exception: Exception = None
        self.exceptions: List[Exception] = []
        self.result: List[Frame] = None
        self.results: List[List[Frame]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.args, self.kwargs = args, self.kwargs
        return self

    def __bool__(self) -> bool:
        return self.exception is None

    def __enter__(self) -> Executable:
        self.execute(*self.args, **self.kwargs)
        return self

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        if ex_type is not None:
            self.rollback()
        else:
            if self._trancount() > 0:
                if self:
                    self.commit()
                else:
                    self.rollback()
                    raise self.exception
            else:
                if self.result is not None or self.exception is not None:
                    self._archive_results_and_exceptions()

    def execute(self, *args: Any, **kwargs: Any) -> Frame:
        result = None
        try:
            statement, sql_args = self._compile_sql(*args, **kwargs)
            result = self.cursor.execute(statement, *sql_args)
        except ProgrammingError as ex:
            self.exception = ex

        self.result = self._get_frames_from_result(result) if result is not None else None
        return self.result

    def commit(self) -> None:
        while self._trancount() > 0:
            self.cursor.commit()

        self._archive_results_and_exceptions()

    def rollback(self) -> None:
        while self._trancount() > 0:
            self.cursor.rollback()

        self._archive_results_and_exceptions()

    def _reset_transactional_state(self) -> None:
        while self._trancount() > 0:
            self.cursor.rollback()

    def _archive_results_and_exceptions(self) -> None:
        self.results.append(self.result)
        self.result = None

        self.exceptions.append(self.exception)
        self.exception = None

    @abstractmethod
    def _compile_sql(self, *args: Any, **kwargs: Any) -> None:
        pass

    def _trancount(self) -> int:
        return self.cursor.execute("SELECT @@TRANCOUNT").fetchall()[0][0]

    @staticmethod
    def _get_frames_from_result(result: Any) -> List[Frame]:
        def get_frame_from_result(result: Any) -> Frame:
            try:
                return Frame([tuple(row) for row in result.fetchall()], columns=[info[0] for info in result.description])
            except ProgrammingError:
                return None

        data = [get_frame_from_result(result)]
        while result.nextset():
            data.append(get_frame_from_result(result))

        return [frame for frame in data if frame is not None]


class StoredProcedure(Executable):
    def __init__(self, name: str, schema: str = "dbo", sql: Sql = None) -> None:
        super().__init__(sql=sql)
        self.name, self.schema = name, schema

    def _compile_sql(self, *args: Any, **kwargs: Any) -> Frame:
        return (f"EXEC {self.schema}.{self.name} {', '.join(list('?'*len(args)) + [f'@{arg}=?' for arg in kwargs.keys()])};", [*args, *list(kwargs.values())])


class Script(Executable):
    def __init__(self, path: PathLike, sql: Sql = None) -> None:
        super().__init__(sql=sql)
        self.file = File.from_pathlike(path)

    def _compile_sql(self, *args: Any, **kwargs: Any) -> Frame:
        return (self.file.contents, [])


class TempManager:
    """Context manager class for implementing temptables without using actual temptables (which sqlalchemy doesn't seem to be able to reflect)"""

    def __init__(self, sql: Sql = None) -> None:
        self.sql, self._table, self.name = sql, None, "__tmp__"

    def __enter__(self) -> TempManager:
        self.sql.refresh()
        if self.name in self.sql.meta.tables:
            self.sql.drop_table(self.name)
        return self

    def __exit__(self, exception_type: Any, exception_value: Any, traceback: Any) -> None:
        self.sql.refresh()
        if self.name in self.sql.meta.tables:
            self.sql.drop_table(self.name)

    def __str__(self) -> str:
        return self.name

    def __call__(self) -> alch.Table:
        if self._table is None:
            self._table = self.sql[self.name]
        return self._table


def literalstatement(statement: Any, format_statement: bool = True) -> str:
    """Returns this a query or expression object's statement as raw SQL with inline literal binds."""

    if isinstance(statement, Query):
        statement = statement.statement

    bound = statement.compile(compile_kwargs={'literal_binds': True}).string + ";"
    formatted = sqlparse.format(bound, reindent=True, wrap_after=1000) if format_statement else bound  # keyword_case="upper" (removed arg due to false positives)
    final = Str(formatted).re.sub(r"\bOVER \(\s*", lambda m: m.group().strip()).re.sub(r"(?<=\n)([^\n]*JOIN[^\n]*)(\bON\b[^\n;]*)(?=[\n;])", lambda m: f"  {m.group(1).strip()}\n    {m.group(2).strip()}")
    return str(final)
