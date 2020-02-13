from __future__ import annotations

from typing import TYPE_CHECKING, Any

import sqlalchemy as alch

from sqlhandler.database.name import SchemaName

if TYPE_CHECKING:
    from sqlhandler import Sql


class Metadata(alch.MetaData):
    def __init__(self, sql: Sql = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.sql = sql

    def __repr__(self) -> str:
        return f"{type(self).__name__}(tables={len(self.tables)})"

    def schema_subset(self, schema: SchemaName) -> Metadata:
        meta = type(self)(sql=self.sql, bind=self.sql.engine)
        meta.tables = type(self.tables)({name: table for name, table in self.tables.items() if table.schema == schema})
        return meta


class NullRegistry(dict):
    def __setitem__(self, key: Any, val: Any) -> None:
        pass
