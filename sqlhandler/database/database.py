from __future__ import annotations

from typing import Any, Union, Set, Callable, TYPE_CHECKING, cast

import sqlalchemy as alch
from sqlalchemy import Column, Integer
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

from maybe import Maybe
from subtypes import Str
from miscutils import cached_property
from iotools import Cache

from .meta import NullRegistry, Metadata
from .name import SchemaName, ObjectName
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

        self.model = cast(Model, declarative_base(bind=self.sql.engine, metadata=self.meta, cls=self.sql.constructors.Model, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.Model.__name__, class_registry=self._null_registry))
        self.auto_model = cast(AutoModel, declarative_base(bind=self.sql.engine, metadata=self.meta, cls=self.sql.constructors.AutoModel, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.AutoModel.__name__, class_registry=self._null_registry))

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
            name, = alch.inspect(self.sql.engine).get_schema_names()
            return name

    def schema_names(self) -> Set[SchemaName]:
        return {SchemaName(name=name, default=self.default_schema) for name in alch.inspect(self.sql.engine).get_schema_names()}

    def table_names(self, schema: SchemaName) -> Set[ObjectName]:
        return {ObjectName(stem=name, schema=schema) for name in alch.inspect(self.sql.engine).get_table_names(schema=schema.name)}

    def view_names(self, schema: SchemaName) -> Set[ObjectName]:
        return {ObjectName(stem=name, schema=schema) for name in alch.inspect(self.sql.engine).get_view_names(schema=schema.name)}

    def create_table(self, table: Table) -> None:
        """Emit a create table statement to the database from the given table object."""
        table = self._normalize_table(table)
        table.create()
        self._reflect_object_with_autoload(self._name_from_table(table))

    def drop_table(self, table: Table) -> None:
        """Emit a drop table statement to the database for the given table object."""
        table = self._normalize_table(table)
        table.drop()
        self._remove_object_if_exists(table)

    def refresh_table(self, table: Table) -> None:
        """Reflect the given table object again."""
        table = self._normalize_table(table)
        self._remove_object_if_exists(table)
        self._reflect_object_with_autoload(self._name_from_table(table))

    def exists_table(self, table: Table) -> bool:
        table = self._normalize_table(table)
        with self.sql.engine.connect() as con:
            return self.sql.engine.dialect.has_table(con, table.name, schema=table.schema)

    def clear(self) -> None:
        """Clear this database's metadata as well as its cache."""
        self.meta.clear()
        self._cache_metadata()

        self._reset_accessors()
        self._sync_with_db()

    def _get_metadata(self) -> Metadata:
        if not self.sql.CACHE_METADATA:
            return self.sql.constructors.Metadata(sql=self.sql)

        try:
            meta = self.cache.setdefault(self.name, self.sql.constructors.Metadata())
        except Exception:
            meta = self.sql.constructors.Metadata()

        meta.bind, meta.sql = self.sql.engine, self.sql

        return meta

    def _cache_metadata(self) -> None:
        if self.sql.CACHE_METADATA:
            self.cache[self.name] = self.meta

    def _sync_with_db(self) -> None:
        self._remove_stale_metadata_objects()
        self._prepare_accessors()

    def _prepare_accessors(self) -> None:
        self._prepare_schema_accessors()
        for schema in self.schema_names():
            self._prepare_object_accessors(schema=schema)

    def _prepare_schema_accessors(self) -> None:
        for accessor in (self.tables, self.views):
            for schema in self.schema_names():
                if schema not in accessor:
                    accessor[schema.name] = self.sql.constructors.Schema(parent=accessor, name=schema)

    def _prepare_object_accessors(self, schema: SchemaName) -> None:
        for accessor, names in [(self.tables, self.table_names), (self.views, self.view_names)]:
            schema_accessor = accessor[schema.name]
            for name in names(schema=schema):
                if name not in schema_accessor:
                    schema_accessor[name.stem] = ObjectProxy(name=name, parent=schema_accessor, database=self)

    def _reflect_database(self):
        for schema in self.schema_names():
            self._reflect_schema(schema=schema)

    def _reflect_schema(self, schema: SchemaName):
        for name in (self.table_names(schema=schema) | self.view_names(schema=schema)):
            self._reflect_object(name=name)

        self._autoload_schema(schema=schema)

    def _reflect_object_with_autoload(self, name: ObjectName) -> None:
        self._reflect_object(name=name)
        self._autoload_schema(name.schema)

    # noinspection PyArgumentList
    def _reflect_object(self, name: ObjectName) -> Table:
        print(f"Reflecting table: {name.name} with schema {name.schema} and meta {self.meta}")
        if alch.inspect(self.sql.engine).get_pk_constraint(name.stem, schema=name.schema.name)["constrained_columns"]:
            table = Table(name.stem, self.meta, schema=name.schema.name, autoload=True)
        else:
            table = Table(name.stem, self.meta, Column("__pk__", Integer, primary_key=True), schema=name.schema.name, autoload=True)

        return table

    def _remove_stale_metadata_objects(self):
        db_names = set()
        for schema in self.schema_names():
            db_names |= self.table_names(schema=schema) | self.view_names(schema=schema)

        for _, item in self.meta.tables.items():
            if self._name_from_table(item) not in db_names:
                self._remove_object_if_exists(item)

    def _autoload_schema(self, schema: SchemaName) -> None:
        model = declarative_base(bind=self.sql.engine, metadata=self.meta, cls=self.sql.constructors.Model, metaclass=self.sql.constructors.ModelMeta, name=self.sql.constructors.Model.__name__, class_registry={})

        automap = automap_base(declarative_base=model)
        automap.prepare(schema=schema.name, classname_for_table=self._table_name(), name_for_scalar_relationship=self._scalar_name(), name_for_collection_relationship=self._collection_name())

        if models := {model.__table__.name: model for model in automap.classes}:
            self._prepare_object_accessors(schema=schema)

        for names, accessor in [(self.table_names, self.tables), (self.view_names, self.views)]:
            object_stems = {name.stem for name in names(schema=schema)}
            objects = {stem: model for stem, model in models.items() if stem in object_stems}
            accessor[schema.name]._registry.update(objects)

    def _remove_object_if_exists(self, table: Table) -> None:
        if table in self.meta:
            self._remove_object_from_metadata(table=table)

        self._remove_object_from_accessors(table=table)

    def _remove_object_from_metadata(self, table: Table) -> None:
        self.meta.remove(table)
        self._cache_metadata()

    def _remove_object_from_accessors(self, table: Table) -> None:
        name = self._name_from_table(table=table)
        for accessor in (self.tables, self.views):
            (schema_accessor := accessor[name.schema.name])._registry.pop(name.stem)
            if name.stem in schema_accessor:
                del schema_accessor[name.stem]

    def _reset_accessors(self) -> None:
        for accessor in (self.tables, self.views):
            for schema, _ in accessor:
                del accessor[schema]

    def _name_from_table(self, table: Table) -> ObjectName:
        return ObjectName(stem=table.name, schema=SchemaName(table.schema, default=self.default_schema))

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
