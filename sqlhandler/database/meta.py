from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import MetaData

from miscutils import ParametrizableMixin

from sqlhandler.database.name import SchemaName

if TYPE_CHECKING:
    from sqlhandler import Sql


class Metadata(MetaData, ParametrizableMixin):
    def __repr__(self) -> str:
        return f"{type(self).__name__}(tables={len(self.tables)})"

    def parametrize(self, param: Sql) -> Metadata:
        self.sql = param
        return self

    def schema_subset(self, schema: SchemaName) -> Metadata:
        meta = type(self)(sql=self.sql, bind=self.sql.engine)
        meta.tables = type(self.tables)({name: table for name, table in self.tables.items() if table.schema == schema})
        return meta


class NullRegistry(dict):
    def __setitem__(self, key: Any, val: Any) -> None:
        pass
