from __future__ import annotations

from typing import TYPE_CHECKING, Any
import copy

import sqlalchemy as alch
from sqlalchemy.util import immutabledict

if TYPE_CHECKING:
    from sqlhandler import Sql


class Metadata(alch.MetaData):
    def __init__(self, sql: Sql = None) -> None:
        super().__init__()
        self.sql = sql

    def __repr__(self) -> str:
        return f"{type(self).__name__}(tables={len(self.tables)})"


class NullRegistry(dict):
    def __setitem__(self, key: Any, val: Any) -> None:
        pass
