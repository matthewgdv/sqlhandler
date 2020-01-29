from __future__ import annotations

from typing import Any, Union, Set, Callable, TYPE_CHECKING, cast

import sqlalchemy as alch
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.util import immutabledict

from maybe import Maybe
from subtypes import Str
from iotools import Cache

from .meta import NullRegistry, Metadata
from .name import SchemaName, TableName
from .schema import OrmSchemas, ObjectSchemas

from sqlhandler.custom import Model, AutoModel

if TYPE_CHECKING:
    from sqlhandler import Sql


class Database:
    """A class representing a sql database. Abstracts away database reflection and metadata caching. The cache lasts for 5 days but can be cleared with Database.clear()"""
    _registry = NullRegistry()

    def __init__(self, sql: Sql) -> None:
        self.sql, self.name, self.cache = sql, sql.engine.url.database, Cache(file=sql.config.folder.new_file("sql_cache", "pkl"), days=5)

        self.default_schema = self.sql.engine.dialect.default_schema_name
        self.schemas = self.schema_names()

        self.meta = self._get_metadata()

        self.model = cast(Model, declarative_base(bind=self.sql.engine, metadata=self.meta, cls=self.sql.constructors.Model, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.Model.__name__, class_registry=self._registry))
        self.auto_model = cast(AutoModel, declarative_base(bind=self.sql.engine, metadata=self.meta, cls=self.sql.constructors.AutoModel, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.AutoModel.__name__, class_registry=self._registry))

        self.orm, self.objects = OrmSchemas(database=self), ObjectSchemas(database=self)
        for schema in {table.schema for table in self.meta.tables.values()}:
            self._add_schema_to_namespaces(SchemaName(schema, default=self.default_schema))

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self.name)}, orm={repr(self.orm)}, objects={repr(self.objects)}, cache={repr(self.cache)})"

    def schema_names(self) -> Set[SchemaName]:
        return {SchemaName(name=name, default=self.default_schema) for name in alch.inspect(self.sql.engine).get_schema_names()}

    def object_names(self) -> Set[TableName]:
        names = set()
        for schema in self.schemas:
            inspector = alch.inspect(self.sql.engine)
            for name in [*inspector.get_table_names(schema=schema.nullable_name), *inspector.get_view_names(schema=schema.nullable_name)]:
                names.add(TableName(stem=name, schema=schema))

        return names

    def reflect(self, schema: str = None) -> None:
        """Reflect the schema with the given name and refresh the 'Database.orm' and 'Database.objects' namespaces."""
        schema_name = SchemaName(schema, default=self.default_schema)

        self.meta.reflect(schema=schema_name.nullable_name, views=True)
        self._add_schema_to_namespaces(schema_name)

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
            namespace()

    def _remove_table_from_metadata_if_exists(self, table: alch.schema.Table) -> None:
        if table in self.meta:
            self.meta.remove(table)
            del self.orm[table.schema][table.name]
            del self.objects[table.schema][table.name]

            self._cache_metadata()

    def _add_schema_to_namespaces(self, schema: SchemaName) -> None:
        new_meta = self.meta.copy_schema_subset(schema.nullable_name)
        model = declarative_base(bind=self.sql.engine, metadata=new_meta, cls=self.sql.constructors.Model, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.Model.__name__, class_registry={})

        automap = automap_base(declarative_base=model)
        automap.prepare(classname_for_table=self._table_name(), name_for_scalar_relationship=self._scalar_name(), name_for_collection_relationship=self._collection_name())

        self.orm[schema.name]._refresh(automap=automap, meta=new_meta)
        self.objects[schema.name]._refresh(automap=automap, meta=new_meta)

    def _get_metadata(self) -> Metadata:
        if not self.sql.CACHE_METADATA:
            return self.sql.constructors.Metadata(sql=self.sql)

        try:
            meta = self.cache.setdefault(self.name, self.sql.constructors.Metadata())
        except Exception:
            meta = None

        if not (isinstance(meta, Metadata) and isinstance(meta.tables, immutabledict)):
            meta = self.sql.constructors.Metadata()

        meta.bind, meta.sql = self.sql.engine, self.sql

        existing_objects = {name.name for name in self.object_names()}
        for table in [table for name, table in meta.tables.items() if name not in existing_objects and "information_schema" not in table.schema.lower()]:
            meta.remove(table)

        return meta

    def _normalize_table(self, table: Union[Model, alch.schema.Table, str]) -> alch.schema.Table:
        return self.meta.tables[table] if isinstance(table, str) else Maybe(table).__table__.else_(table)

    def _cache_metadata(self) -> None:
        if self.sql.CACHE_METADATA:
            self.cache[self.name] = self.meta

    def _table_name(self) -> Callable:
        def table_name(base: Any, tablename: Any, table: Any) -> str:
            return tablename

        return table_name

    def _scalar_name(self) -> Callable:
        def scalar_name(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
            return referred_cls.__name__

        return scalar_name

    def _collection_name(self) -> Callable:
        def collection_name(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
            return str(Str(referred_cls.__name__).case.snake().case.plural())

        return collection_name


