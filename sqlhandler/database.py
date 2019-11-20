from __future__ import annotations

from typing import Any, Union, TYPE_CHECKING
import copy

import sqlalchemy as alch
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

from maybe import Maybe
from subtypes import Str, NameSpace
from iotools import Cache

from .custom import Model, ModelMeta

if TYPE_CHECKING:
    from .sql import Sql


class Registry(dict):
    def __setitem__(self, key: Any, val: Any) -> None:
        pass


class Database:
    """A class representing a sql database. Abstracts away database reflection and metadata caching. The cache lasts for 5 days but can be cleared with Database.clear()"""
    _registry = Registry()

    def __init__(self, sql: Sql) -> None:
        self.sql, self.name, self.cache = sql, sql.engine.url.database, Cache(file=sql.config.appdata.new_file("sql_cache", "pkl"), days=5)
        self.meta = self._get_metadata()
        self.declaration: Model = declarative_base(bind=self.sql.engine, metadata=self.meta, cls=Model, metaclass=ModelMeta, name="Model", class_registry=self._registry)
        self.orm, self.objects = Schemas(database=self), Schemas(database=self)
        self.default_schema_name = vars(self.sql.engine.dialect).get("schema_name", "default")

        for schema in {self.meta.tables[table].schema for table in self.meta.tables}:
            self._add_schema_to_namespaces(schema)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self.name)}, orm={repr(self.orm)}, objects={repr(self.objects)}, cache={repr(self.cache)})"

    def reflect(self, schema: str = None) -> None:
        """Reflect the schema with the given name and refresh the 'Database.orm' and 'Database.objects' namespaces."""
        schema = None if schema == self.default_schema_name else schema

        self.meta.reflect(schema=schema, views=True)
        self._add_schema_to_namespaces(schema)

        self._cache_metadata()

    def create_table(self, table: alch.schema.Table) -> None:
        """Emit a create table statement to the database from the given table object."""
        table = self._normalize_table(table)
        table.create()
        self.reflect(table.schema)

    def drop_table(self, table: alch.schema.Table) -> None:
        """Emit a drop table statement to the database for the given table object."""
        table = self._normalize_table(table)
        table.drop()
        self._remove_table_from_metadata_if_exists(table)

    def refresh_table(self, table: alch.schema.Table) -> None:
        """Reflect the given table object again."""
        table = self._normalize_table(table)
        self._remove_table_from_metadata_if_exists(table)
        self.reflect(table.schema)

    def exists_table(self, table: alch.schema.Table) -> bool:
        table = self._normalize_table(table)
        with self.sql.engine.connect() as con:
            return self.sql.engine.dialect.has_table(con, table.name, schema=table.schema)

    def clear(self) -> None:
        """Clear this database's metadata as well as its cache."""
        self.meta.clear()
        self._cache_metadata()
        for namespace in (self.orm, self.objects):
            namespace._clear()

    def _remove_table_from_metadata_if_exists(self, table: alch.schema.Table) -> None:
        if table in self.meta:
            self.meta.remove(table)
            del self.orm[table.schema][table.name]
            del self.objects[table.schema][table.name]

            self._cache_metadata()

    def _add_schema_to_namespaces(self, schema: str) -> None:
        schema = None if schema == self.default_schema_name else schema

        new_meta = copy.deepcopy(self.meta)
        new_meta.sql = self.sql

        invalid_tables = ({table for table in new_meta.tables if new_meta.tables[table].schema is not None}
                          if schema is None else
                          {table for table in new_meta.tables if new_meta.tables[table].schema is None or new_meta.tables[table].schema.lower() != schema})

        for table in invalid_tables:
            new_meta.remove(new_meta.tables[table])

        declaration = declarative_base(bind=self.sql.engine, metadata=new_meta, metaclass=ModelMeta, name="Model", cls=Model, class_registry=self._registry)

        automap = automap_base(declarative_base=declaration)
        automap.prepare(name_for_collection_relationship=self._pluralize_collection)

        self.orm._add_schema(name=schema, tables=list(automap.classes))
        self.objects._add_schema(name=schema, tables=[new_meta.tables[item] for item in new_meta.tables])

    def _get_metadata(self) -> None:
        try:
            meta = self.cache.setdefault(self.name, alch.MetaData())
        except Exception:
            meta = alch.MetaData()

        meta.bind, meta.sql = self.sql.engine, self.sql

        for table in list(meta.tables.values()):
            if not self.exists_table(table):
                meta.remove(table)

        return meta

    def _normalize_table(self, table: Union[Model, alch.schema.Table]) -> alch.schema.Table:
        return Maybe(table).__table__.else_(table)

    def _cache_metadata(self) -> None:
        self.cache[self.name] = self.meta

    @staticmethod
    def _pluralize_collection(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
        return str(Str(referred_cls.__name__).case.snake().case.plural())


class Schemas(NameSpace):
    """A NameSpace class representing a set of database schemas. Individual schemas can be accessed with either attribute or item access. If a schema isn't already cached an attempt will be made to reflect it."""

    def __init__(self, database: Database) -> None:
        super().__init__()
        self._database = database

    def __repr__(self) -> str:
        return f"""{type(self).__name__}(num_schemas={len(self)}, schemas=[{", ".join([f"{type(schema).__name__}(name='{schema._name}', tables={len(schema)})" for name, schema in self])}])"""

    def __getitem__(self, name: str) -> Schema:
        return getattr(self, self._database.default_schema_name) if name is None else super().__getitem__(name)

    def __getattr__(self, attr: str) -> Schema:
        if not attr.startswith("_"):
            self._database.reflect(attr)

        try:
            return super().__getattribute__(attr)
        except AttributeError:
            raise AttributeError(f"{type(self._database).__name__} '{self._database.name}' has no schema '{attr}'.")

    def _add_schema(self, name: str, tables: list) -> None:
        name = Maybe(name).else_(self._database.default_schema_name)
        if name in self:
            self[name]._refresh_from_tables(tables)
        else:
            self[name] = Schema(database=self._database, name=name, tables=tables)


class Schema(NameSpace):
    """A NameSpace class representing a database schema. Models/objects can be accessed with either attribute or item access. If the model/object isn't already cached, an attempt will be made to reflect it."""

    def __init__(self, database: Database, name: str, tables: list) -> None:
        super().__init__({Maybe(table).__table__.else_(table).name: table for table in tables})
        self._database, self._name = database, name

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self._name)}, num_tables={len(self)}, tables={[table for table, _ in self]})"

    def __getattr__(self, attr: str) -> Model:
        if not attr.startswith("_"):
            self._database.reflect(self._name)

        try:
            return super().__getattribute__(attr)
        except AttributeError:
            raise AttributeError(f"{type(self).__name__} '{self._name}' of {type(self._database).__name__} '{self._database.name}' has no object '{attr}'.")

    def _refresh_from_tables(self, tables: list) -> None:
        self._clear()
        for name, table in {Maybe(table).__table__.else_(table).name: table for table in tables}.items():
            self[name] = table
