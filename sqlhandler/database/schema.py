from __future__ import annotations

from typing import Any, TYPE_CHECKING, Dict, Type, Optional

from subtypes import NameSpace

from .name import SchemaName, ObjectName

from sqlhandler.custom import Model, ReflectedModel

if TYPE_CHECKING:
    from .database import Database


class Schemas(NameSpace):
    """A NameSpace class representing a set of database schemas. Individual schemas can be accessed with either attribute or item access. If a schema isn't already cached an attempt will be made to reflect it."""

    def __init__(self, database: Database) -> None:
        self._database = database

    def __repr__(self) -> str:
        return f"""{type(self).__name__}(num_schemas={len(self)}, schemas=[{", ".join([f"{type(schema).__name__}(name='{name}', tables={len(schema)})" for name, schema in self])}])"""

    def __call__(self, mapping: dict = None, / , **kwargs: Any) -> Schema:
        if mapping is None and not kwargs:
            self._database()
        else:
            super().__call__(mapping, **kwargs)

        return self

    def __getattr__(self, attr: str) -> Schema:
        if attr == "__none__":
            return self[self._database.default_schema]

        if not attr.startswith("_"):
            self._database._prepare_schema_accessors()

        try:
            return super().__getattribute__(attr)
        except AttributeError:
            raise AttributeError(f"{type(self._database).__name__} '{self._database.name}' has no schema '{attr}'.")


class Schema(NameSpace):
    """A NameSpace class representing a database schema. Models/objects can be accessed with either attribute or item access. If the model/object isn't already cached, an attempt will be made to reflect it."""

    def __init__(self, name: SchemaName, parent: Schemas) -> None:
        self._name, self._parent, self._database = name, parent, parent._database
        self._registry: Optional[Dict[str, Model]] = None
        self._base: Optional[Type[ReflectedModel]] = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name='{self._name}', num_tables={len(self)}, tables={[table for table, _ in self]})"

    def __call__(self, mapping: dict = None, / , **kwargs: Any) -> Schema:
        if mapping is None and not kwargs:
            if not self._registry:
                self._database._reflect_schema(self._name)
        else:
            super().__call__(mapping, **kwargs)

        return self

    def __getattr__(self, attr: str) -> Model:
        if not attr.startswith("_"):
            self._database._reflect_object_with_autoload(ObjectName(stem=attr, schema=self._name))

        try:
            return super().__getattribute__(attr)
        except AttributeError:
            raise AttributeError(f"{type(self).__name__} '{self._name}' of {type(self._database).__name__} '{self._database.name}' has no object '{attr}'.")


class ObjectProxy:
    def __init__(self, name: ObjectName, parent: Schema, database: Database) -> None:
        self.name, self.parent, self.database = name, parent, database

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name='{self.name.name}', stem='{self.name.stem}', schema='{self.name.schema}')"

    def __call__(self) -> Model:
        if model := self.parent._registry.get(self.name.stem):
            return model
        else:
            self.database._reflect_object_with_autoload(name=self.name)
            return self.parent._registry.get(self.name.stem)
