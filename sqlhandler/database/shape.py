from __future__ import annotations

from typing import TYPE_CHECKING, Set, Dict, Union

from miscutils import ReprMixin

from .name import SchemaName, ObjectName, TableName, ViewName

if TYPE_CHECKING:
    from .database import Database


class DatabaseShape(ReprMixin):
    def __init__(self, database: Database) -> None:
        self.database = database
        self.schemas: dict[SchemaName, SchemaShape] = {name: SchemaShape(name=name, database=self.database) for name in self.database.schema_names()}
        self.schema_name_mappings = {name.name: name for name in self.schemas}

        self.all_objects = set()

    def __getitem__(self, name: Union[str, SchemaName]) -> SchemaShape:
        return self.schemas[name]

    def refresh(self):
        for schema in self.schemas.values():
            schema.refresh()

        self.all_objects: Set[ObjectName] = {obj for schema in self.schemas.values() for obj in schema.objects}


class SchemaShape(ReprMixin):
    def __init__(self, name: SchemaName, database: Database) -> None:
        self.name, self.database, self.registry = name, database, {}

    def refresh(self) -> None:
        self.tables: Set[TableName] = self.database.table_names(self.name)
        self.views: Set[ViewName] = self.database.view_names(self.name)
        self.objects: Set[ObjectName] = self.tables | self.views
