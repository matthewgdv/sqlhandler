from __future__ import annotations

from typing import Optional, Set


class ObjectName:
    object_type = "object"

    def __init__(self, stem: str, schema: SchemaName) -> None:
        self.stem, self.schema, self.name = stem, schema, stem if schema.name is None else f"{schema.name}.{stem}"

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.stem

    def __eq__(self, other: ObjectName) -> bool:
        return self.name == other.name

    def __hash__(self) -> int:
        return id(self)


class TableName(ObjectName):
    object_type = "table"


class ViewName(ObjectName):
    object_type = "view"


class SchemaName:
    def __init__(self, name: Optional[str], default: str) -> None:
        self.name, self.default = name if name is not None else (None if default is None else default), default
        self.is_default = name is None or name == default

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.name

    def __eq__(self, other: SchemaName) -> bool:
        return self.name == other.name

    def __hash__(self) -> int:
        return id(self)


class SchemaShape(dict):
    def all_objects(self) -> Set[ObjectName]:
        ret = set()

        for names in self.values():
            ret |= names

        return ret
