from __future__ import annotations

from typing import Any

import sqlalchemy as alch
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

from maybe import Maybe
from subtypes import Str
from pathmagic import File
from miscutils import NameSpace, Cache

from sqlhandler import localres
from .custom import Base


# TODO: Fix bug with declarative base occasionally getting collisions when dropping and recreating tables or clearing metadata


class DatabaseHandler:
    def __init__(self, alchemy) -> None:
        self.alchemy, self.name, self.cache = alchemy, alchemy.engine.url.database, Cache(file=File.from_resource(localres, "sql_cache.pkl"), days=5)
        self.meta = self._get_metadata()
        self.declaration = self.reflection = None  # type: Base

        self._refresh_bases()
        self.orm, self.objects = Database(handler=self, tables=list(self.reflection.classes)), Database(handler=self, tables=[self.meta.tables[item] for item in self.meta.tables])
        self.reflect()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

    def reflect(self, schema: str = None) -> None:
        self.meta.reflect(schema=schema, views=True)

        self._refresh_bases()
        self.objects._add_schema(name=schema, tables=[table for table in [self.meta.tables[item] for item in self.meta.tables] if Maybe(table.schema).lower().else_(None) == Maybe(schema).lower().else_(None)])
        self.orm._add_schema(name=schema, tables=[table for table in self.reflection.classes if Maybe(table.__table__.schema).lower().else_(None) == Maybe(schema).lower().else_(None)])

        self.cache[self.name] = self.meta

    def refresh_table(self, table: alch.schema.Table, schema: str = None) -> None:
        table = self._normalize_table(table=table, schema=schema)
        if table is not None:
            self.drop_table(table)
        self.reflect(Maybe(table).schema.else_(schema))

    def drop_table(self, table: alch.schema.Table, schema: str = None) -> None:
        table = self._normalize_table(table=table, schema=schema)
        table.drop()
        self.meta.remove(table)

        name, schema = table.name, table.schema
        del self.orm[schema][name]
        del self.objects[schema][name]

    def clear(self) -> None:
        self.meta.clear()
        self.cache[self.name] = self.meta
        for namespace in (self.orm, self.objects):
            namespace._clear_namespace()

    def _refresh_bases(self) -> None:
        self.declaration = declarative_base(bind=self.alchemy.engine, metadata=self.meta, cls=Base)
        self.declaration.alchemy = self.alchemy
        self.reflection = automap_base(declarative_base=self.declaration)
        self.reflection.prepare(name_for_collection_relationship=self._pluralize_collection)

    def _get_metadata(self) -> None:
        meta = self.cache.setdefault(self.name, alch.MetaData())
        meta.bind = self.alchemy.engine
        return meta

    def _normalize_table(self, table: Any, schema: str = None) -> alch.schema.Table:
        if hasattr(table, "__table__"):
            table = table.__table__
        elif isinstance(table, str):
            table = self.meta.tables.get(f"{(Maybe(schema) + '.').else_('')}{table}")

        return table

    @staticmethod
    def _pluralize_collection(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
        referred_name = referred_cls.__name__
        return str(Str(referred_name).snake_case().plural())


class Database(NameSpace):
    def __init__(self, handler: DatabaseHandler, tables: list) -> None:
        super().__init__()
        self._handler, self._name = handler, handler.name
        self._set_schemas_from_tables(tables=tables)

    def __repr__(self) -> str:
        return f"""{type(self).__name__}(name={repr(self._name)}, num_tables={sum([len(schema) for schema in self._namespace])}, num_schemas={len(self)}, schemas=[{", ".join([f"{type(schema).__name__}(name='{schema._name}', tables={len(schema)})" for name, schema in self._namespace.items()])}])"""

    def __getitem__(self, name: str) -> Schema:
        if name is None:
            return self.dbo
        else:
            return super().__getitem__(name)

    def __getattr__(self, attr: str) -> Schema:
        if not attr.startswith("_"):
            self._handler.reflect(attr)

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
        self[name] = Schema(handler=self._handler, name=name, tables=tables)


class Schema(NameSpace):
    def __init__(self, handler: DatabaseHandler, name: str, tables: list) -> None:
        super().__init__(mappings={Maybe(table).__table__.else_(table).name: table for table in tables})
        self._handler, self._name = handler, name

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self._name)}, num_tables={len(self)}, tables={[table for table in self._namespace]})"

    def __getattr__(self, attr: str) -> Schema:
        if not attr.startswith("_"):
            self._handler.reflect(self._name)

        return super().__getattribute__(attr)
