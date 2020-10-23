from __future__ import annotations

from typing import Any, TYPE_CHECKING, List, Collection

from subtypes import NameSpace

from .name import SchemaName, ObjectName

from sqlhandler.custom import Model

if TYPE_CHECKING:
    from .database import Database
    from .shape import DatabaseShape, SchemaShape


class BaseSchema(NameSpace):
    """A NameSpace class representing a database schema. Models/objects can be accessed with either attribute or item access. If the model/object isn't already cached, an attempt will be made to reflect it."""

    def __init__(self, name: SchemaName, parent: Schemas) -> None:
        self._name, self._parent, self._database = name, parent, parent._database

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name='{self._name}', num_objects={len(self)}, objects={[table for table, _ in self]})"

    def __call__(self, mapping: dict = None, / , **kwargs: Any) -> Schema:
        if mapping is None and not kwargs:
            if not self._database.shape[self._name].registry:
                self._database._reflect_schema(self._name)
        else:
            super().__call__(mapping, **kwargs)

        return self

    def __getattr__(self, attr: str) -> Model:
        if not attr.startswith("_"):
            self._database._reflect_object(ObjectName(stem=attr, schema=self._name))

        try:
            return super().__getattribute__(attr)
        except AttributeError:
            raise AttributeError(f"{type(self).__name__} '{self._name}' of {type(self._database).__name__} '{self._database.name}' has no object '{attr}'.")

    def _refresh(self, shape: SchemaShape) -> BaseSchema:
        return self


class Schema(BaseSchema):
    def _refresh(self, shape: SchemaShape) -> Schema:
        self({name.stem: ObjectProxy(name, parent=self) for name in shape.objects})
        return self


class TableSchema(BaseSchema):
    def _refresh(self, shape: SchemaShape) -> TableSchema:
        self({name.stem: ObjectProxy(name, parent=self) for name in shape.tables})
        return self


class ViewSchema(BaseSchema):
    def _refresh(self, shape: SchemaShape) -> ViewSchema:
        self({name.stem: ObjectProxy(name, parent=self) for name in shape.views})
        return self


class ObjectProxy:
    def __init__(self, name: ObjectName, parent: BaseSchema) -> None:
        self.name, self.parent, self.database = name, parent, parent._database
        self.registry = self.database.shape.schemas[parent._name].registry

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name='{self.name.name}', stem='{self.name.stem}', schema='{self.name.schema}')"

    def __call__(self) -> Model:
        if model := self.registry.get(self.name.stem):
            return model
        else:
            self.database._reflect_object(name=self.name)
            return self.registry.get(self.name.stem)


class BaseSchemas(NameSpace):
    """A NameSpace class representing a set of database schemas. Individual schemas can be accessed with either attribute or item access. If a schema isn't already cached an attempt will be made to reflect it."""
    _schema_constructor = BaseSchema

    def __init__(self, database: Database) -> None:
        self._database = database

    def __repr__(self) -> str:
        return f"""{type(self).__name__}(num_schemas={len(self)}, schemas=[{", ".join([f"{type(schema).__name__}(name='{name}', tables={len(schema)})" for name, schema in self])}])"""

    def __call__(self, mapping: dict = None, / , **kwargs: Any) -> Schema:
        if mapping is None and not kwargs:
            self._database._reflect_database()
        else:
            super().__call__(mapping, **kwargs)

        return self

    def __getattr__(self, attr: str) -> Schema:
        if attr == "__none__":
            return self[self._database.default_schema]

        if not attr.startswith("_"):
            self._database.router.refresh_accessors()

        try:
            return super().__getattribute__(attr)
        except AttributeError:
            raise AttributeError(f"{type(self._database).__name__} '{self._database.name}' has no schema '{attr}'.")

    def _refresh(self, shape: DatabaseShape) -> None:
        self({schema_name.name: self._schema_constructor(name=schema_name, parent=self)._refresh(schema_shape) for schema_name, schema_shape in shape.schemas.items()})


class Schemas(BaseSchemas):
    _schema_constructor = Schema


class TableSchemas(BaseSchemas):
    _schema_constructor = TableSchema


class ViewSchemas(BaseSchemas):
    _schema_constructor = ViewSchema


class SchemaRouter:
    def __init__(self, database: Database) -> None:
        self.database = database
        self.accessors: list[Schemas] = []

    def register(self, accessor: Schemas) -> SchemaRouter:
        self.accessors.append(accessor)
        return self

    def register_all(self, accessors: Collection[Schemas]) -> SchemaRouter:
        self.accessors += accessors
        return self

    def refresh_accessors(self) -> None:
        for accessor in self.accessors:
            accessor._refresh(self.database.shape)

    def remove_object_if_exists(self, name: ObjectName) -> None:
        for accessor in self.accessors:
            try:
                del accessor[name.schema][name.stem]
            except Exception:
                pass
