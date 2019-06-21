from __future__ import annotations

from typing import Any, Dict

import sqlalchemy as alch
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

from maybe import Maybe
from subtypes import Str
from miscutils import NameSpace

from .custom import Base


class DatabaseHandler:
    def __init__(self, name: str) -> None:
        self.name, self.meta = name, alch.MetaData()
        self.declaration = self.reflection = None  # type: Base
        self.orm, self.objects = Database(self), Database(self)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

    def bind(self, engine) -> DatabaseHandler:
        self.meta.bind = engine
        return self

    def reflect(self, schema: str = None) -> None:
        schema = Maybe(schema).else_("dbo")
        self.meta.reflect(schema=schema, views=True)

        self.refresh_bases()
        self.objects._add_schema(name=schema, mappings={table.name: table for table in self.meta.tables if table.schema.lower() == schema.lower()})
        self.orm._add_schema(name=schema, mappings={table.__table__.name: table for table in self.reflection.classes if table.__table__.schema.lower() == schema.lower()})

    def refresh_bases(self) -> None:
        self.declaration = declarative_base(bind=self.engine, metadata=self.meta, cls=Base)
        self.declaration.alchemy = self.alchemy
        self.reflection = automap_base(declarative_base=self.declaration)
        self.reflection.prepare(name_for_collection_relationship=self._pluralize_collection)

    def refresh_table(self, table: alch.schema.Table, schema: str = None) -> None:
        table = self._normalize_table(table=table, schema=schema)
        self.drop_table(table)
        self.reflect(table.schema)

    def drop_table(self, table: alch.schema.Table, schema: str = None) -> None:
        table = self._normalize_table(table=table, schema=schema)
        self.meta.remove(table)

        name, schema = table.name, table.schema
        del self.orm[schema][name]
        del self.objects[schema][name]

    def clear(self) -> None:
        self.meta.clear()
        for namespace in (self.orm, self.objects):
            namespace.clear_namespace()

    def _normalize_table(self, table: Any, schema: str = None) -> alch.schema.Table:
        if hasattr(table, "__table__"):
            table = table.__table__
        elif isinstance(table, str):
            table = self.meta.tables[f"{(Maybe(schema) + '.').else_('')}{table}"]

        return table

    @staticmethod
    def _pluralize_collection(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
        referred_name = referred_cls.__name__
        return str(Str(referred_name).snake_case().plural())


class Database(NameSpace):
    def __init__(self, handler: DatabaseHandler) -> None:
        super().__init__()
        self._handler, self._name = handler, handler.name

    def __repr__(self) -> str:
        return f"""{type(self).__name__}(name={repr(self._name)}, num_tables={sum([len(schema) for schema in self._namespace])}, num_schemas={len(self)}, schemas=[{", ".join([f"{type(schema).__name__}(name='{schema.name}', tables={len(schema)})" for name, schema in self._namespace.items()])}])"""

    def __getitem__(self, name: str) -> Schema:
        if name is None:
            return self.dbo
        else:
            return super().__getitem__(name)

    def __getattr__(self, attr: str) -> Schema:
        if attr is None:
            return self.dbo
        else:
            self._handler.reflect(attr)
            return super().__getattr__(attr)

    def _add_schema(self, name: str, mappings: Dict[str, Any]) -> None:
        self[name] = Schema(handler=self._handler, name=name, mappings=mappings)


class Schema(NameSpace):
    def __init__(self, handler: DatabaseHandler, name: str, mappings: Dict[str, Any]) -> None:
        super().__init__(mappings=mappings)
        self._handler, self._name = handler, name

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self._name)}, num_tables={len(self)}, tables={[table for table in self._namespace]})"

    def __getattr__(self, attr: str) -> Schema:
        self._handler.reflect(self._name)
        return super().__getattr__(attr)
