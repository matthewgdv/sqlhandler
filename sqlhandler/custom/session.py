from __future__ import annotations

from typing import Any, Union

from sqlalchemy import text
from sqlalchemy.orm import Session as BaseSession

from iotools import Log

from .result import Result


class Session(BaseSession):
    """Custom subclass of sqlalchemy.orm.Session granting access to a custom Query class through the '.query()' method."""

    def execute(self, statement: Any, params: Union[list, dict] = None, **kwargs: Any) -> Result:
        """Execute an valid object against this Session."""
        Log.debug(statement)

        raw_result = super().execute(statement=text(statement) if isinstance(statement, str) else statement,
                                     params=params, **kwargs)
        result = Result(raw_result=raw_result)

        Log.debug(f"{result.rowcount} row(s) affected")

        return result
