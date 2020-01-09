from __future__ import annotations

from typing import Any, Union, Set, Callable, TYPE_CHECKING, cast, Optional
import copy

import sqlalchemy as alch
from sqlalchemy.ext.automap import automap_base, AutomapBase
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.util import immutabledict

from maybe import Maybe
from subtypes import Str, NameSpace
from iotools import Cache

from .custom import Model, AutoModel

if TYPE_CHECKING:
    from .sql import Sql


class NullRegistry(dict):
    def __setitem__(self, key: Any, val: Any) -> None:
        pass


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

    def table_names(self) -> Set[str]:
        return set(sum([alch.inspect(self.sql.engine).get_table_names(schema=schema.nullable_name) for schema in self.schemas], []))

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

        existing_tables = self.table_names()
        for table in [table for name, table in meta.tables.items() if name not in existing_tables and "information_schema" not in table.schema.lower()]:
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


class Schemas(NameSpace):
    """A NameSpace class representing a set of database schemas. Individual schemas can be accessed with either attribute or item access. If a schema isn't already cached an attempt will be made to reflect it."""

    def __init__(self, database: Database) -> None:
        self._database, self._table_mappings = database, {}
        self._refresh()

    def __repr__(self) -> str:
        return f"""{type(self).__name__}(num_schemas={len(self)}, schemas=[{", ".join([f"{type(schema).__name__}(name='{schema._name}', tables={len(schema) if schema._ready else '?'})" for name, schema in self])}])"""

    def __call__(self, mapping: dict = None, / , **kwargs: Any) -> Schema:
        self._refresh()
        return self

    def __getitem__(self, name: str) -> Schema:
        return getattr(self, SchemaName(name=name, default=self._database.default_schema).name) if name is None else super().__getitem__(name)

    def __getattr__(self, attr: str) -> Schema:
        if not attr.startswith("_"):
            self._refresh()

        try:
            return super().__getattribute__(attr)
        except AttributeError:
            raise AttributeError(f"{type(self._database).__name__} '{self._database.name}' has no schema '{attr}'.")

    def _refresh(self) -> None:
        super().__call__()
        for schema in self._database.schemas:
            self[schema.name] = self.schema_constructor(parent=self, name=schema.name)


class Schema(NameSpace):
    """A NameSpace class representing a database schema. Models/objects can be accessed with either attribute or item access. If the model/object isn't already cached, an attempt will be made to reflect it."""

    def __init__(self, parent: Schemas, name: str) -> None:
        self._database, self._parent, self._name, self._ready = parent._database, parent, name, False

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self._name)}, num_tables={len(self) if self._ready else '?'}, tables={[table for table, _ in self] if self._ready else '?'})"

    def __call__(self, mapping: dict = None, / , **kwargs: Any) -> Schema:
        self._database.reflect(self._name)
        return self

    def __getattr__(self, attr: str) -> Model:
        if not attr.startswith("_"):
            self._database.reflect(self._name)

        try:
            return super().__getattribute__(attr)
        except AttributeError:
            raise AttributeError(f"{type(self).__name__} '{self._name}' of {type(self._database).__name__} '{self._database.name}' has no object '{attr}'.")

    def _refresh(self, automap: Model, meta: Metadata) -> None:
        raise NotImplementedError

    def _pre_refresh(self) -> None:
        super().__call__()
        self._ready = True


class OrmSchema(Schema):
    def _refresh(self, automap: AutomapBase, meta: Metadata) -> None:
        self._pre_refresh()
        for name, table in {table.__table__.name: table for table in automap.classes}.items():
            self[name] = self._parent._table_mappings[name] = table


class ObjectSchema(Schema):
    def _refresh(self, automap: Model, meta: Metadata) -> None:
        self._pre_refresh()
        for name, table in {table.name: table for table in meta.tables.values()}.items():
            self[name] = self._parent._table_mappings[name] = table


class OrmSchemas(Schemas):
    schema_constructor = OrmSchema


class ObjectSchemas(Schemas):
    schema_constructor = ObjectSchema


class Metadata(alch.MetaData):
    def __init__(self, sql: Sql = None) -> None:
        super().__init__()
        self.sql = sql

    def __repr__(self) -> str:
        return f"{type(self).__name__}(tables={len(self.tables)})"

    def copy_schema_subset(self, schema: str) -> Metadata:
        shallow = copy.copy(self)
        shallow.sql, shallow.tables = self.sql, immutabledict({name: table for name, table in self.tables.items() if (schema or "") == (table.schema or "")})
        return shallow


class SchemaName:
    def __init__(self, name: Optional[str], default: str) -> None:
        if default is None:
            self.name, self.nullable_name = "main", None
        else:
            if name is None:
                self.name, self.nullable_name = default, None
            else:
                self.name, self.nullable_name = name, None if name == default else name

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"
