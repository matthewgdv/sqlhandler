from __future__ import annotations

from typing import Any, List, Callable, TypeVar, TYPE_CHECKING

import sqlalchemy as alch
import sqlalchemy.sql.sqltypes
import sqlparse
from sqlalchemy.orm import Query
from pyodbc import ProgrammingError

from subtypes import Frame, Str

if TYPE_CHECKING:
    from .alchemy import Alchemy


SelfType = TypeVar("SelfType")


class AlchemyBound:
    def __init__(self, *args: Any, alchemy: Alchemy = None, **kwargs: Any) -> None:
        self.alchemy = alchemy

    @classmethod
    def from_alchemy(cls: SelfType, alchemy: Alchemy) -> Callable[[...], SelfType]:
        def wrapper(*args: Any, **kwargs: Any) -> AlchemyBound:
            return cls(*args, alchemy=alchemy, **kwargs)
        return wrapper


class StoredProcedure(AlchemyBound):
    def __init__(self, name: str, schema: str = "dbo", alchemy: Alchemy = None) -> None:
        self.alchemy, self.name, self.schema = alchemy, name, schema
        self.cursor = self.alchemy.engine.raw_connection().cursor()
        self.result: List[Frame] = None
        self.results: List[List[Frame]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.execute(*args, **kwargs)

    def __enter__(self) -> StoredProcedure:
        return self

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        if ex_type is None:
            self.commit()
        else:
            self.rollback()

    def execute(self, *args: Any, **kwargs: Any) -> Frame:
        result = self.cursor.execute(f"EXEC {self.schema}.{self.name} {', '.join(list('?'*len(args)) + [f'@{arg}=?' for arg in kwargs.keys()])};", *[*args, *list(kwargs.values())])

        self.result = self._get_frames_from_result(result)
        self.results.append(self.result)

        return self

    def commit(self) -> None:
        self.cursor.commit()

    def rollback(self) -> None:
        self.cursor.rollback()

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

        return data


class TempManager:
    """Context manager class for implementing temptables without using actual temptables (which sqlalchemy doesn't seem to be able to reflect)"""

    def __init__(self, alchemy: Alchemy = None) -> None:
        self.alchemy, self._table, self.name = alchemy, None, "__tmp__"

    def __enter__(self) -> TempManager:
        self.alchemy.refresh()
        if self.name in self.alchemy.meta.tables:
            self.alchemy.drop_table(self.name)
        return self

    def __exit__(self, exception_type: Any, exception_value: Any, traceback: Any) -> None:
        self.alchemy.refresh()
        if self.name in self.alchemy.meta.tables:
            self.alchemy.drop_table(self.name)

    def __str__(self) -> str:
        return self.name

    def __call__(self) -> alch.Table:
        if self._table is None:
            self._table = self.alchemy[self.name]
        return self._table


def literalstatement(statement: Any, format_statement: bool = True) -> str:
    """Returns this a query or expression object's statement as raw SQL with inline literal binds."""

    if isinstance(statement, Query):
        statement = statement.statement

    bound = statement.compile(compile_kwargs={'literal_binds': True}).string + ";"
    formatted = sqlparse.format(bound, reindent=True, wrap_after=1000) if format_statement else bound  # keyword_case="upper" (removed arg due to false positives)
    final = Str(formatted).sub(r"\bOVER \(\s*", lambda m: m.group().strip()).sub(r"(?<=\n)([^\n]*JOIN[^\n]*)(\bON\b[^\n]*)(?=\n)", lambda m: f"  {m.group(1).strip()}\n    {m.group(2).strip()}")
    return str(final)
