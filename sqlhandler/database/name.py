from __future__ import annotations

from typing import Optional
from miscutils import ReprMixin


class TableName(ReprMixin):
    def __init__(self, stem: str, schema: SchemaName) -> None:
        self.stem, self.schema, self.full_name = stem, schema, f"{schema.name}.{stem}"
        self.name = stem if schema.nullable_name is None else self.full_name


class SchemaName(ReprMixin):
    def __init__(self, name: Optional[str], default: str) -> None:
        if default is None:
            self.name, self.nullable_name = "main", None
        else:
            if name is None:
                self.name, self.nullable_name = default, None
            else:
                self.name, self.nullable_name = name, None if name == default else name
