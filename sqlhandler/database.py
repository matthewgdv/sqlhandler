from __future__ import annotations

from typing import Any, Union, TYPE_CHECKING

import sqlalchemy as alch
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

from maybe import Maybe
from subtypes import Str
from pathmagic import File
from miscutils import NameSpaceObject, Cache

from sqlhandler import localres
from .custom import Base

if TYPE_CHECKING:
    from .sql import Sql


# TODO: Fix bug with declarative base occasionally getting collisions when dropping and recreating tables or clearing metadata


class Database:
    def __init__(self, sql: Sql) -> None:
        self.sql, self.name, self.cache = sql, sql.engine.url.database, Cache(file=File.from_resource(localres, "sql_cache.pkl"), days=5)
        self.meta = self._get_metadata()
        self.declaration = self.reflection = None  # type: Base

        self._refresh_bases()
        self.orm, self.objects = Schemas(database=self, tables=list(self.reflection.classes)), Schemas(database=self, tables=[self.meta.tables[item] for item in self.meta.tables])
        self.reflect()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self.name)}, orm={repr(self.orm)}, objects={repr(self.objects)}, cache={repr(self.cache)})"

    def reflect(self, schema: str = None) -> None:
        self.meta.reflect(schema=schema, views=True)

        self._refresh_bases()
        self.objects._add_schema(name=schema, tables=[table for table in [self.meta.tables[item] for item in self.meta.tables] if Maybe(table.schema).lower().else_(None) == Maybe(schema).lower().else_(None)])
        self.orm._add_schema(name=schema, tables=[table for table in self.reflection.classes if Maybe(table.__table__.schema).lower().else_(None) == Maybe(schema).lower().else_(None)])

        self.cache[self.name] = self.meta

    def create_table(self, table: alch.schema.Table) -> None:
        table = self._normalize_table(table)
        table.create()
        self.reflect(table.schema)

    def drop_table(self, table: alch.schema.Table) -> None:
        table = self._normalize_table(table)
        table.drop()

        self.meta.remove(table)
        del self.orm[table.schema][table.name]
        del self.objects[table.schema][table.name]

    def refresh_table(self, table: alch.schema.Table) -> None:
        table = self._normalize_table(table)

        self.meta.remove(table)
        del self.orm[table.schema][table.name]
        del self.objects[table.schema][table.name]

        self.reflect(table.schema)

    def clear(self) -> None:
        self.meta.clear()
        self.cache[self.name] = self.meta
        for namespace in (self.orm, self.objects):
            namespace._clear()

    def _refresh_bases(self) -> None:
        self.declaration = declarative_base(bind=self.sql.engine, metadata=self.meta, cls=Base)
        self.declaration.sql = self.sql
        self.reflection = automap_base(declarative_base=self.declaration)
        self.reflection.prepare(name_for_collection_relationship=self._pluralize_collection)

    def _get_metadata(self) -> None:
        meta = self.cache.setdefault(self.name, alch.MetaData())
        meta.bind = self.sql.engine
        return meta

    def _normalize_table(self, table: Union[Base, alch.schema.Table]) -> alch.schema.Table:
        return Maybe(table).__table__.else_(table)

    @staticmethod
    def _pluralize_collection(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
        referred_name = referred_cls.__name__
        return str(Str(referred_name).snake_case().plural())


class Schemas(NameSpaceObject):
    def __init__(self, database: Database, tables: list) -> None:
        super().__init__()
        self._database = database
        self._set_schemas_from_tables(tables=tables)

    def __repr__(self) -> str:
        return f"""{type(self).__name__}(num_schemas={len(self)}, schemas=[{", ".join([f"{type(schema).__name__}(name='{schema._name}', tables={len(schema)})" for name, schema in self])}])"""

    def __getitem__(self, name: str) -> Schema:
        if name is None:
            return self.dbo
        else:
            return super().__getitem__(name)

    def __getattr__(self, attr: str) -> Schema:
        if not attr.startswith("_"):
            self._database.reflect(attr)

        return super().__getattribute__(attr)

    def _set_schemas_from_tables(self, tables: list) -> None:
        schemas = {}
        for table in tables:
            schema = Maybe(table).__table__.else_(table).schema
            schemas.setdefault(schema, []).append(table)

        for schema, tables in schemas.items():
            self._add_schema(schema, tables)

    def _add_schema(self, name: str, tables: list) -> None:
        name = Maybe(name).else_("dbo")
        self[name] = Schema(database=self._database, name=name, tables=tables)


class Schema(NameSpaceObject):
    def __init__(self, database: Database, name: str, tables: list) -> None:
        super().__init__(mappings={Maybe(table).__table__.else_(table).name: table for table in tables})
        self._database, self._name = database, name

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self._name)}, num_tables={len(self)}, tables={[table for table, _ in self]})"

    def __getattr__(self, attr: str) -> Base:
        if not attr.startswith("_"):
            self._database.reflect(self._name)

        return super().__getattribute__(attr)
