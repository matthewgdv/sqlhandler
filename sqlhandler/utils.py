from __future__ import annotations

from typing import Any, List, Callable, TypeVar, TYPE_CHECKING
from abc import ABC, abstractmethod

import sqlalchemy as alch
import sqlalchemy.sql.sqltypes
from sqlalchemy.orm import Query
import sqlparse

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
        self.results: List[List[Frame]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.execute(*args, **kwargs)

    def execute(self, *args: Any, **kwargs: Any) -> List[Frame]:
        statement, bindparams = self._compile_sql(*args, **kwargs)
        cursor = self.sql.session.execute(statement, bindparams).cursor
        if cursor is None:
            return None
        else:
            self.results.append(self._get_frames_from_cursor(cursor))
            return self.results[-1]

    @abstractmethod
    def _compile_sql(self, *args: Any, **kwargs: Any) -> None:
        pass

    @staticmethod
    def _get_frames_from_cursor(cursor: Any) -> List[Frame]:
        def get_frame_from_cursor(cursor: Any) -> Frame:
            try:
                return Frame([tuple(row) for row in cursor.fetchall()], columns=[info[0] for info in cursor.description])
            except Exception:
                return None

        data = [get_frame_from_cursor(cursor)]
        while cursor.nextset():
            data.append(get_frame_from_cursor(cursor))

        return [frame for frame in data if frame is not None] or None


class StoredProcedure(Executable):
    def __init__(self, name: str, schema: str = "dbo", database: str = None, sql: Sql = None) -> None:
        super().__init__(sql=sql)
        self.name, self.schema, self.database = name, schema, database

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name}, schema={self.schema})"

    def _compile_sql(self, *args: Any, **kwargs: Any) -> Frame:
        mappings = {
            **{f"boundarg{index + 1}": {"bind": f":boundarg{index + 1}", "val": val} for index, val in enumerate(args)},
            **{f"boundkwarg{index + 1}": {"bind": f"@{name}=:boundkwarg{index + 1}", "val": val} for index, (name, val) in enumerate(kwargs.items())}
        }
        proc_name = f"EXEC {f'[{self.database}].' if self.database is not None else ''}[{self.schema}].[{self.name}]"
        return (f"{proc_name} {', '.join([arg['bind'] for arg in mappings.values()])}", {name: arg["val"] for name, arg in mappings.items()})


class Script(Executable):
    def __init__(self, path: PathLike, sql: Sql = None) -> None:
        super().__init__(sql=sql)
        self.file = File.from_pathlike(path)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(file={self.file})"

    def _compile_sql(self, *args: Any, **kwargs: Any) -> Frame:
        return (self.file.contents, {})


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

    stage1 = Str(formatted).re.sub(r"\bOVER \(\s*", lambda m: m.group().strip()).re.sub(r"(?<=\n)([^\n]*JOIN[^\n]*)(\bON\b[^\n;]*)(?=[\n;])", lambda m: f"  {m.group(1).strip()}\n    {m.group(2).strip()}")
    stage2 = stage1.re.sub(r"(?<=\bJOIN[^\n]+\n\s+ON[^\n]+\n(?:\s*AND[^\n]+\n)*?)(AND[^\n]+)(?=[\n;])", lambda m: f"    {m.group(1).strip()}")

    return str(stage2)
