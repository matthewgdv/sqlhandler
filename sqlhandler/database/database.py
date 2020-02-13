from __future__ import annotations

import warnings
from typing import Any, Union, Set, Callable, TYPE_CHECKING, cast, Type

import sqlalchemy as alch
from sqlalchemy import Column, Integer
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

from maybe import Maybe
from subtypes import Str
from miscutils import cached_property, PercentagePrinter, Printer
from iotools import Cache

from .meta import NullRegistry, Metadata
from .name import SchemaName, ObjectName, SchemaShape, ViewName, TableName
from .schema import Schemas, ObjectProxy

from sqlhandler.custom import Model, AutoModel, Table

if TYPE_CHECKING:
    from sqlhandler import Sql


class Database:
    """A class representing a sql database. Abstracts away database reflection and metadata caching. The cache lasts for 5 days but can be cleared with Database.clear()"""
    _null_registry = NullRegistry()

    def __init__(self, sql: Sql) -> None:
        self.sql, self.name, self.cache = sql, sql.engine.url.database, Cache(file=sql.config.folder.new_file("sql_cache", "pkl"), days=5)
        self.meta = self._get_metadata()

        self.model = cast(Model, declarative_base(metadata=self.meta, cls=self.sql.constructors.Model, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.Model.__name__, class_registry=self._null_registry))
        self.auto_model = cast(AutoModel, declarative_base(metadata=self.meta, cls=self.sql.constructors.AutoModel, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.AutoModel.__name__, class_registry=self._null_registry))

        self.tables, self.views = Schemas(database=self), Schemas(database=self)
        self._sync_with_db()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self.name)}, tables={repr(self.tables)}, views={repr(self.views)}, cache={repr(self.cache)})"

    def __call__(self) -> Database:
        self._reflect_database()
        return self

    @cached_property
    def default_schema(self) -> str:
        if name := alch.inspect(self.sql.engine).default_schema_name:
            return name
        else:
            name, = alch.inspect(self.sql.engine).get_schema_names() or [None]
            return name

    def schema_names(self) -> Set[SchemaName]:
        return {SchemaName(name=name, default=self.default_schema) for name in alch.inspect(self.sql.engine).get_schema_names()}

    def table_names(self, schema: SchemaName) -> Set[TableName]:
        return {TableName(stem=name, schema=schema) for name in alch.inspect(self.sql.engine).get_table_names(schema=schema.name)}

    def view_names(self, schema: SchemaName) -> Set[ViewName]:
        return {ViewName(stem=name, schema=schema) for name in alch.inspect(self.sql.engine).get_view_names(schema=schema.name)}

    def create_table(self, table: Table) -> None:
        """Emit a create table statement to the database from the given table object."""
        table = self._normalize_table(table)
        table.create()
        self._sync_with_db()
        self._reflect_object_with_autoload(self._name_from_object(table, object_type=TableName))

    def drop_table(self, table: Table) -> None:
        """Emit a drop table statement to the database for the given table object."""
        table = self._normalize_table(table)
        table.drop()
        self._sync_with_db()

    def refresh_table(self, table: Table) -> None:
        """Reflect the given table object again."""
        table = self._normalize_table(table)
        self._sync_with_db()
        self._reflect_object_with_autoload(self._name_from_object(table, object_type=TableName))

    def exists_table(self, table: Table) -> bool:
        table = self._normalize_table(table)
        with self.sql.engine.connect() as con:
            return self.sql.engine.dialect.has_table(con, table.name, schema=table.schema)

    def reset(self) -> None:
        """Clear this database's metadata as well as its cache and reflect everything from scratch."""
        self.meta.clear()
        self._cache_metadata()
        self._sync_with_db()

    def _get_metadata(self) -> Metadata:
        if not self.sql.settings.cache_metadata:
            return self.sql.constructors.Metadata(sql=self.sql, bind=self.sql.engine)

        try:
            meta = self.cache.setdefault(self.name, self.sql.constructors.Metadata())
        except Exception as ex:
            warnings.warn(f"The following exception ocurred when attempting to retrieve the previously cached Metadata, but was supressed:\n\n{ex}\n\nStarting with blank Metadata...")
            meta = self.sql.constructors.Metadata()

        meta.bind, meta.sql = self.sql.engine, self.sql

        return meta

    def _cache_metadata(self) -> None:
        if self.sql.settings.cache_metadata:
            self.cache[self.name] = self.meta

    def _sync_with_db(self) -> None:
        self._determine_shape()
        self._reset_accessors()
        self._prepare_accessors()

        if not self.meta.tables and self.sql.settings.eager_reflection:
            self._reflect_database()
            self._cache_metadata()
        else:
            self._autoload_database()

        self._remove_expired_metadata_objects()

    def _determine_shape(self) -> None:
        self._schemas = self.schema_names()
        self._tables = SchemaShape({schema: {name for name in self.table_names(schema=schema)} for schema in self._schemas})
        self._views = SchemaShape({schema: {name for name in self.view_names(schema=schema)} for schema in self._schemas})

    def _prepare_accessors(self) -> None:
        self._prepare_schema_accessors()
        for schema in self._schemas:
            self._prepare_object_accessors(schema=schema)

    def _prepare_schema_accessors(self) -> None:
        for accessor in (self.tables, self.views):
            for schema in self._schemas:
                if schema not in accessor:
                    accessor[schema.name] = self.sql.constructors.Schema(parent=accessor, name=schema)

    def _prepare_object_accessors(self, schema: SchemaName) -> None:
        for accessor, names in [(self.tables, self._tables), (self.views, self._views)]:
            schema_accessor = accessor[schema.name]
            for name in names.get(schema, set()):
                schema_accessor[name.stem] = ObjectProxy(name=name, parent=schema_accessor, database=self)

    def _reflect_database(self):
        for schema in PercentagePrinter(sorted(self._schemas, key=lambda name: name.name), formatter=lambda name: f"Reflecting schema: {name.name}"):
            if schema.name not in self.sql.settings.lazy_schemas:
                with Printer.from_indentation():
                    self._reflect_schema(schema=schema)

    def _reflect_schema(self, schema: SchemaName):
        names = sum([
            sorted(collection.get(schema, set()), key=lambda name_: name_.name) if condition else []
            for condition, collection in [(self.sql.settings.reflect_tables, self._tables), (self.sql.settings.reflect_views, self._views)]
        ], [])

        for name in PercentagePrinter(names, formatter=lambda name_: f"Reflecting {name_.object_type}: {name_.name}"):
            self._reflect_object(name=name)

        self._autoload_schema(schema=schema)

    def _reflect_object_with_autoload(self, name: ObjectName) -> None:
        self._reflect_object(name=name)
        self._autoload_schema(name.schema)

    # noinspection PyArgumentList
    def _reflect_object(self, name: ObjectName) -> Table:
        if not (table := Table(name.stem, self.meta, schema=name.schema.name, autoload=True)).primary_key:
            table = Table(name.stem, self.meta, Column("__pk__", Integer, primary_key=True), schema=name.schema.name, autoload=True, extend_existing=True)

        return table

    def _autoload_database(self) -> None:
        for schema in self._schemas:
            self._autoload_schema(schema)

    def _autoload_schema(self, schema: SchemaName) -> None:
        model = declarative_base(bind=self.sql.engine,
                                 metadata=self.meta.schema_subset(schema),
                                 cls=self.sql.constructors.ReflectedModel,
                                 metaclass=self.sql.constructors.ModelMeta,
                                 name=self.sql.constructors.ReflectedModel.__name__,
                                 class_registry=(registry := {}))

        automap = automap_base(declarative_base=model)
        automap.prepare(classname_for_table=self._table_name(), name_for_scalar_relationship=self._scalar_name(), name_for_collection_relationship=self._collection_name())
        automap.metadata = self.meta

        self.tables[schema.name]._base = self.views[schema.name]._base = automap
        self.tables[schema.name]._registry = self.views[schema.name]._registry = registry

    def _remove_expired_metadata_objects(self):
        all_objects = self._tables.all_objects() | self._views.all_objects()
        for item in list(self.meta.tables.values()):
            if self._name_from_object(item) not in all_objects:
                self._remove_object_if_exists(item)

    def _remove_object_if_exists(self, table: Table) -> None:
        if table in self.meta:
            self._remove_object_from_metadata(table=table)

        self._remove_object_from_accessors(name=self._name_from_object(table=table))

    def _remove_object_from_metadata(self, table: Table) -> None:
        self.meta.remove(table)
        self._cache_metadata()

    def _remove_object_from_accessors(self, name: ObjectName) -> None:
        for accessor in (self.tables, self.views):
            (schema_accessor := accessor[name.schema.name])._registry.pop(name.stem, None)
            if name.stem in schema_accessor:
                del schema_accessor[name.stem]

    def _reset_accessors(self) -> None:
        for accessor in (self.tables, self.views):
            for schema, _ in accessor:
                del accessor[schema]

    def _name_from_object(self, table: Table, object_type: Type[ObjectName] = ObjectName) -> ObjectName:
        return object_type(stem=table.name, schema=SchemaName(table.schema, default=self.default_schema))

    def _normalize_table(self, table: Union[Model, Table, str]) -> Table:
        return self.meta.tables[table] if isinstance(table, str) else Maybe(table).__table__.else_(table)

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
