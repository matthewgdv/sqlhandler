from __future__ import annotations

import datetime
from typing import Any


from sqlalchemy import Table
from sqlalchemy import types
from sqlalchemy import event


from sqlalchemy.dialects import mssql

from subtypes import DateTime


class StringLiteral(types.String):
    def literal_processor(self, dialect: Any) -> Any:
        super_processor = super().literal_processor(dialect)

        def process(value: Any) -> Any:
            if value is None:
                return "NULL"

            result = super_processor(str(value))
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)

            return result
        return process


class BitLiteral(mssql.BIT):
    def literal_processor(self, dialect: Any) -> Any:
        super_processor = super().literal_processor(dialect)

        def process(value: Any) -> Any:
            if isinstance(value, bool):
                return str(1) if value else str(0)
            elif isinstance(value, int) and value in (0, 1):
                return value
            else:
                return super_processor(value)

        return process


class SubtypesDateTime(types.TypeDecorator):
    impl = types.DateTime

    def process_bind_param(self, value, dialect):
        return None if value is None else str(value)

    def process_result_value(self, value, dialect):
        return None if value is None else DateTime.fromisoformat(str(value))


class SubtypesDate(types.TypeDecorator):
    impl = types.Date

    def process_bind_param(self, value, dialect):
        return None if value is None else str(value if isinstance(value, datetime.date) else DateTime.fromisoformat(str(value)).to_date())

    def process_result_value(self, value, dialect):
        return None if value is None else DateTime.fromisoformat(str(value))


@event.listens_for(Table, "column_reflect")
def _setup_datetimes(inspector, table, column_info):
    if isinstance(column_info["type"], (types.DateTime, types.DATETIME)):
        column_info["type"] = SubtypesDateTime()
    elif isinstance(column_info["type"], (types.Date, types.DATE)):
        column_info["type"] = SubtypesDate()
