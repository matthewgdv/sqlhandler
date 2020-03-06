from __future__ import annotations

from typing import Any

import sqlalchemy as alch
from sqlalchemy.sql.schema import _get_table_key


class Table(alch.Table):
    def __new__(*args: Any, **kwargs: Any) -> Table:
        if len(args) == 1:
            return alch.Table.__new__(*args, **kwargs)

        _, name, meta, *_ = args
        if kwargs.pop("is_declarative", False):
            if (schema := kwargs.get("schema")) is None:
                schema = meta.schema

            if (table := meta.tables.get(_get_table_key(name, schema))) is not None:
                meta.remove(table)

        return alch.Table.__new__(*args, **kwargs)
