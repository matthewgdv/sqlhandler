from __future__ import annotations

from typing import Any, TYPE_CHECKING, Tuple, Optional
from abc import ABC, abstractmethod

from miscutils import ParametrizableMixin
from sqlalchemy.orm import Query
import sqlparse

from subtypes import Frame, Str
from pathmagic import File, PathLike

if TYPE_CHECKING:
    from sqlhandler import Sql


# TODO: fix ON clause whitespace in all situations


class Executable(ParametrizableMixin, ABC):
    """An abstract class representing a SQL executable such. Concrete implementations such as scripts or stored procedures must inherit from this. An implementaion of Executable._compile_sql() must be provided."""

    def __init__(self, sql: Sql = None, verbose: bool = False) -> None:
        self.sql: Optional[Sql] = None
        self.results: list[list[Frame]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.execute(*args, **kwargs)

    def parametrize(self, param: Sql) -> ParametrizableMixin:
        self.sql = param
        return self

    def execute(self, params: dict) -> Optional[list[Frame]]:
        """Execute this executable SQL object. Passes on its args and kwargs to Executable._compile_sql()."""
        if (cursor := self._execute(params)) is None:
            return None

        with cursor:
            self.results.append(result := self._get_frames_from_cursor(cursor))
            return result

    @abstractmethod
    def _execute(self, params: dict):
        raise NotImplementedError

    @staticmethod
    def _get_frames_from_cursor(cursor: Any) -> list[Frame]:
        def get_frame_from_cursor(curs: Any) -> Optional[Frame]:
            try:
                return Frame([tuple(row) for row in curs.fetchall()], columns=[info[0] for info in cursor.description])
            except Exception:
                return None

        data = [get_frame_from_cursor(cursor)]
        while cursor.nextset():
            data.append(get_frame_from_cursor(cursor))

        return [frame for frame in data if frame is not None] or None


class StoredProcedure(Executable):
    """A class representing a stored procedure in the database. Can be called to execute the proc. Arguments and keyword arguements will be passed on."""

    def __init__(self, name: str, schema: str = None, database: str = None, sql: Sql = None) -> None:
        super().__init__(sql=sql)
        self.name, self.schema, self.database = name, schema or self.sql.database.default_schema, database

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name}, schema={self.schema})"

    def _execute(self, params: dict) -> Any:
        with self.sql.engine.raw_connection() as con:
            cursor = con.cursor()
            cursor.callproc(f"{self.schema or self.sql.database.default_schema}.{self.name}", params)
            return cursor


class Script(Executable):
    """A class representing a SQL script in the filesystem. Can be called to execute the script."""

    def __init__(self, path: PathLike, sql: Sql = None) -> None:
        super().__init__(sql=sql)
        self.file = File.from_pathlike(path)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(file={self.file})"

    def _execute(self, *args, **kwargs) -> Any:
        return self.sql.session.execute(self.file.content, *args, **kwargs).cursor


def literal_statement(statement: Any, format_statement: bool = True) -> str:
    """Returns this a query or expression object's statement as raw SQL with inline literal binds."""

    if isinstance(statement, Query):
        statement = statement.statement

    bound = statement.compile(compile_kwargs={'literal_binds': True}).string + ";"
    formatted = sqlparse.format(bound, reindent=True) if format_statement else bound  # keyword_case="upper" (removed arg due to false positives)

    stage1 = Str(formatted).re.sub(r"\bOVER\s*\(\s*", lambda m: "OVER (").re.sub(r"OVER \((ORDER\s*BY|PARTITION\s*BY)\s+(\S+)\s+(ORDER\s*BY|PARTITION\s*BY)\s+(\S+)\s*\)", lambda m: f"OVER ({m.group(1)} {m.group(2)} {m.group(3)} {m.group(4)})")
    stage2 = stage1.re.sub(r"(?<=\n)([^\n]*JOIN[^\n]*)(\bON\b[^\n;]*)(?=[\n;])", lambda m: f"  {m.group(1).strip()}\n    {m.group(2).strip()}")
    stage3 = stage2.re.sub(r"(?<=\bJOIN[^\n]+\n\s+ON[^\n]+\n(?:\s*AND[^\n]+\n)*?)(\s*AND[^\n]+)(?=[\n;])", lambda m: f"    {m.group(1).strip()}")

    return str(stage3)
