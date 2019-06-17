from __future__ import annotations

import re as witchcraft
from typing import Any, List, Set, Callable, TYPE_CHECKING

import sqlalchemy as alch
import sqlalchemy.sql.sqltypes
import sqlparse
from sqlalchemy.dialects import mssql
from sqlalchemy.orm import Query

from maybe import Maybe
from subtypes import Frame, DateTime

if TYPE_CHECKING:
    from .alchemy import Alchemy


class AlchemyBound:
    def __init__(self, *args: Any, alchemy: Alchemy = None, **kwargs: Any) -> None:
        self.alchemy = alchemy

    @classmethod
    def from_alchemy(cls, alchemy: Alchemy) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> AlchemyBound:
            return cls(*args, alchemy=alchemy, **kwargs)
        return wrapper


class StoredProcedure(AlchemyBound):
    def __init__(self, name: str, schema: str = "dbo", alchemy: Alchemy = None) -> None:
        self.alchemy, self.name, self.schema = alchemy, name, schema
        self.frames: List[Frame] = None
        self.results: List[List[Frame]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.execute(*args, **kwargs)

    def execute(self, *args: Any, **kwargs: Any) -> Frame:
        cursor = self.alchemy.engine.raw_connection().cursor()
        result = cursor.execute(f"EXEC {self.schema}.{self.name} {', '.join(list('?'*len(args)) + [f'@{arg}=?' for arg in kwargs.keys()])};", *[*args, *list(kwargs.values())])
        self.frames = self._get_frames_from_result(result)
        self.results.append(self.frames)
        return self.frames

    @staticmethod
    def _get_frames_from_result(result: Any) -> List[Frame]:
        def get_frame_from_result(result: Any) -> Frame:
            return Frame([tuple(row) for row in result.fetchall()], columns=[info[0] for info in result.description])

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


class MetadataCacheHelper:
    def __init__(self, alchemy: Alchemy, maximum_cache_days: int = 5) -> None:
        from sqlhandler import resourcedir

        self.cache_file, self.alchemy, self.max_days = resourcedir.newfile("__sql_cache__.pkl"), alchemy, maximum_cache_days
        self.load_metadata_from_cache()

    def load_metadata_from_cache(self) -> None:
        self._ensure_cache()
        self._ensure_database()

    def save_metadata_to_cache(self) -> None:
        self.database.schemas = self.alchemy.schemas
        self.cache_file.contents = self.cache

    def _ensure_cache(self) -> None:
        self.cache: MetadataCacheHelper.Cache = self.cache_file.contents
        if not self.cache:
            self.cache = MetadataCacheHelper.Cache(max_validity_days=self.max_days)

    def _ensure_database(self) -> None:
        db_name = self.alchemy.database
        if db_name not in self.cache:
            self.cache.add_database(name=db_name)
        self.database = self.cache[db_name]

    class Cache:
        def __init__(self, max_validity_days: int = 5) -> None:
            self.max_age, self.dob = max_validity_days, DateTime.today()
            self.databases: List[MetadataCacheHelper.DataBase] = []

        def __repr__(self) -> str:
            return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

        def __bool__(self) -> bool:
            return bool(self.dob > DateTime.today().delta(days=-self.max_age))

        def __contains__(self, other: str) -> bool:
            return other in [db.name for db in self.databases]

        def __getitem__(self, key: str) -> MetadataCacheHelper.DataBase:
            database, = [db for db in self.databases if db.name == key]
            return database

        def add_database(self, name: str) -> None:
            self.databases.append(MetadataCacheHelper.DataBase(name=name))

    class DataBase:
        def __init__(self, name: str) -> None:
            self.name, self.meta = name, alch.MetaData()
            self.schemas: Set[str] = set()

        def __repr__(self) -> str:
            return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"


def literal_statement(statement: Any, format_statement: bool = True) -> str:
    """Returns this a query or expression object's statement as raw SQL with inline literal binds."""

    class StringLiteral(sqlalchemy.sql.sqltypes.String):
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

    class BitLiteral(alch.dialects.mssql.BIT):
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

    class LiteralDialect(mssql.dialect):
        colspecs = {
            alch.sql.sqltypes.String: StringLiteral,
            alch.sql.sqltypes.DateTime: StringLiteral,
            alch.sql.sqltypes.Date: StringLiteral,
            alch.sql.sqltypes.NullType: StringLiteral,
            alch.dialects.mssql.BIT: BitLiteral
        }

    if isinstance(statement, Query):
        statement = statement.statement

    dia = LiteralDialect()
    dia.supports_multivalues_insert = True

    bound = statement.compile(dialect=dia, compile_kwargs={'literal_binds': True, 'dialect': LiteralDialect()}).string + ";"
    formatted = sqlparse.format(bound, reindent=True, wrap_after=1000) if format_statement else bound  # keyword_case="upper" (removed arg due to false positives)
    final = witchcraft.sub(r"\bOVER \(\s*", lambda m: m.group().strip(), formatted)
    return str(final)


def literalstatement(statement: Any, format_statement: bool = True) -> str:
    """Returns this a query or expression object's statement as raw SQL with inline literal binds."""

    if isinstance(statement, Query):
        statement = statement.statement

    bound = statement.compile(compile_kwargs={'literal_binds': True}).string + ";"
    formatted = sqlparse.format(bound, reindent=True, wrap_after=1000) if format_statement else bound  # keyword_case="upper" (removed arg due to false positives)
    final = witchcraft.sub(r"\bOVER \(\s*", lambda m: m.group().strip(), formatted)
    return str(final)


class BaseDatabase:
    def __repr__(self) -> str:
        return f"""{type(self).__name__}(num_tables={len(self)}, num_schemas={len(self.schemas)}, schemas=[{", ".join([f"{type(schema).__name__}(name='{schema.name}', tables={len(schema)})" for name, schema in self.schemas.items()])}])"""

    def __len__(self) -> int:
        return sum([len(schema) for schema in self.schemas])

    def __getitem__(self, val):
        return self.schemas[val]


class BaseSchema:
    def __repr__(self) -> str:
        return f"{type(self).__name__}(num_tables={len(self)}, tables={[table for table in self.tables]})"

    def __len__(self) -> int:
        return len(self.tables)

    def __getitem__(self, val):
        return self.tables[val]


class OrmDatabase(BaseDatabase):
    def __init__(self, sql) -> None:
        schemas = {}
        for table in sql.reflection.classes:
            schemas.setdefault(table.__table__.schema, []).append(table)

        self.schemas = {Maybe(schema_name).else_("dbo"): OrmSchema(name=Maybe(schema_name).else_("dbo"), tables=tables) for schema_name, tables in schemas.items()}

        for schema_name, schema in self.schemas.items():
            if schema_name.isidentifier():
                setattr(self, schema_name, schema)


class OrmSchema(BaseSchema):
    def __init__(self, name: str, tables: list) -> None:
        self.name, self.tables = name, {table.__table__.name: table for table in tables}

        for table_name, table in self.tables.items():
            if table_name.isidentifier():
                setattr(self, table.__table__.name, table)


class Database(BaseDatabase):
    def __init__(self, sql) -> None:
        tables = [sql[table] for table in sql.meta.tables]

        schemas = {}
        for table in tables:
            schemas.setdefault(table.schema, []).append(table)

        self.schemas = {Maybe(schema_name).else_("dbo"): Schema(name=Maybe(schema_name).else_("dbo"), tables=tables) for schema_name, tables in schemas.items()}

        for schema_name, schema in self.schemas.items():
            if schema_name.isidentifier():
                setattr(self, schema_name, schema)


class Schema(BaseSchema):
    def __init__(self, name: str, tables: list) -> None:
        self.name, self.tables = name, {table.name: table for table in tables}

        for table_name, table in self.tables.items():
            if table_name.isidentifier():
                setattr(self, table.name, table)
