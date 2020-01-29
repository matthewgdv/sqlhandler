from __future__ import annotations

from typing import Any, TYPE_CHECKING

from sqlalchemy.ext.automap import AutomapBase

from subtypes import NameSpace

from .meta import Metadata
from .name import SchemaName

from sqlhandler.custom import Model

if TYPE_CHECKING:
    from .database import Database


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
