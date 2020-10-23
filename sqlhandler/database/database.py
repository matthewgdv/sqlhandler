from __future__ import annotations

import warnings
from contextlib import contextmanager
from typing import Any, Union, Set, Callable, TYPE_CHECKING, cast, Type

import sqlalchemy as alch
from sqlalchemy import Column, Integer, event
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

from maybe import Maybe
from subtypes import Str, Dict
from miscutils import cached_property, PercentagePrinter, Printer
from iotools import Cache

from .meta import NullRegistry, Metadata
from .name import SchemaName, ObjectName, ViewName, TableName
from .shape import DatabaseShape
from .schema import Schemas, TableSchemas, ViewSchemas, SchemaRouter

from sqlhandler.custom import Model, AutoModel, Table

if TYPE_CHECKING:
    from sqlhandler import Sql


class Database:
    """A class representing a sql database. Abstracts away database reflection and metadata caching. The cache lasts for 5 days but can be cleared with Database.clear()"""
    _null_registry = NullRegistry()

    def __init__(self, sql: Sql) -> None:
        self.sql, self.name, self.cache, self._post_reshape_countdown = sql, sql.engine.url.database, Cache(file=sql.config.folder.new_file("sql_cache", "pkl"), days=5), 0
        self.meta = self._get_metadata()

        self.model = cast(Type[Model], declarative_base(metadata=self.meta, cls=self.sql.constructors.Model, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.Model.__name__, class_registry=self._null_registry))
        self.auto_model = cast(Type[AutoModel], declarative_base(metadata=self.meta, cls=self.sql.constructors.AutoModel, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.AutoModel.__name__, class_registry=self._null_registry))

        self.shape = DatabaseShape(database=self)
        self.objects, self.tables, self.views = Schemas(database=self), TableSchemas(database=self), ViewSchemas(database=self)
        self.router = SchemaRouter(database=self).register_all([self.objects, self.tables, self.views])

        self._sync_with_db()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self.name)}, schemas={len(self.shape.schemas)}, tables={len(self.meta.tables)})"

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
        self._reflect_object(self._name_from_object(table, object_type=TableName))

    def drop_table(self, table: Table) -> None:
        """Emit a drop table statement to the database for the given table object."""
        table = self._normalize_table(table)
        table.drop()
        self._sync_with_db()

    def refresh_table(self, table: Table) -> None:
        """Reflect the given table object again."""
        table = self._normalize_table(table)
        self._sync_with_db()
        self._reflect_object(self._name_from_object(table, object_type=TableName))

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

    @contextmanager
    def _post_reshape_soon(self) -> None:
        self._post_reshape_countdown += 1
        yield
        self._post_reshape_countdown -= 1

        if not self._post_reshape_countdown:
            self._cache_metadata()
            self._autoload_models()

    def _sync_with_db(self) -> None:
        with self._post_reshape_soon():
            self.shape.refresh()
            self.router.refresh_accessors()

            if not self.meta.tables and self.sql.settings.eager_reflection:
                self._reflect_database()

            self._remove_expired_metadata_objects()

    def _reflect_database(self):
        with self._post_reshape_soon():
            for schema in PercentagePrinter(sorted(self.shape.schemas, key=lambda name: name.name), formatter=lambda name: f"Reflecting schema: {name.name}"):
                if schema.name not in self.sql.settings.lazy_schemas:
                    with Printer.from_indentation():
                        self._reflect_schema(schema=schema)

    def _reflect_schema(self, schema: SchemaName):
        with self._post_reshape_soon():
            schema_shape = self.shape[schema]
            names = sum([
                sorted(collection, key=lambda name_: name_.name) if condition else []
                for collection, condition in [(schema_shape.tables, self.sql.settings.reflect_tables), (schema_shape.views, self.sql.settings.reflect_views)]
            ], [])

            for name in PercentagePrinter(names, formatter=lambda name_: f"Reflecting {name_.object_type}: {name_.name}"):
                self._reflect_object(name=name)

    # noinspection PyArgumentList
    def _reflect_object(self, name: ObjectName) -> Table:
        with self._post_reshape_soon():
            if not (table := Table(name.stem, self.meta, schema=name.schema.name, autoload=True)).primary_key:
                table = Table(name.stem, self.meta, Column("__pk__", Integer, primary_key=True), schema=name.schema.name, autoload=True, extend_existing=True)

            return table

    def _autoload_models(self) -> None:
        reflected_model = declarative_base(bind=self.sql.engine, metadata=self.meta, cls=self.sql.constructors.ReflectedModel, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.ReflectedModel.__name__, class_registry=(registry := {}))
        automap = automap_base(declarative_base=reflected_model)
        automap.classes = Dict()

        @event.listens_for(automap, "class_instrument", propagate=True)
        def cls_instrument(cls):
            schema = schema if (schema := (table := cls.__table__).schema) is not None else cls.metadata.sql.database.default_schema
            automap.classes.setdefault_lazy(key=schema, factory=Dict)[table.name] = cls

        automap.prepare(classname_for_table=self._table_name(), name_for_scalar_relationship=self._scalar_name(), name_for_collection_relationship=self._collection_name())

        for key, val in automap.classes.items():
            if isinstance(val, Dict):
                self.shape.schemas[key].registry.update(val)

    def _remove_expired_metadata_objects(self):
        with self._post_reshape_soon():
            for item in list(self.meta.tables.values()):
                if self._name_from_object(item) not in self.shape.all_objects:
                    self._remove_object_if_exists(item)

    def _remove_object_if_exists(self, table: Table) -> None:
        if table in self.meta:
            with self._post_reshape_soon():
                self.meta.remove(table)

        self.router.remove_object_if_exists(name=self._name_from_object(table=table, object_type=TableName))

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
