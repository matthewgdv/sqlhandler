from __future__ import annotations

from typing import Any, TYPE_CHECKING

import sqlalchemy as alch
from sqlalchemy.orm.attributes import InstrumentedAttribute

from maybe import Maybe
from subtypes import Frame
from miscutils import is_non_string_iterable, ParametrizableMixin

from sqlhandler.utils.utils import literal_statement


if TYPE_CHECKING:
    from sqlhandler import Sql


class ExpressionMixin(ParametrizableMixin):
    """A mixin providing private methods for logging using expression classes."""

    def execute(self) -> str:
        """Execute this query's statement in the current session."""
        return self.sql.session.execute(self)

    def parametrize(self, param: Sql) -> ExpressionMixin:
        self.sql = param
        return self


class Select(alch.sql.Select, ExpressionMixin):
    """Custom subclass of sqlalchemy.sql.Select with additional useful methods and aliases for existing methods."""

    def __init__(self, *columns: Any, whereclause: Any = None, from_obj: Any = None, distinct: Any = False, having: Any = None, correlate: Any = True, prefixes: Any = None, suffixes: Any = None, **kwargs: Any) -> None:
        as_single_iterable = columns[0] if len(columns) == 1 and is_non_string_iterable(columns[0]) else [*columns]
        super().__init__(columns=as_single_iterable, whereclause=whereclause, from_obj=from_obj, distinct=distinct, having=having, correlate=correlate, prefixes=prefixes, suffixes=suffixes, **kwargs)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)

    def frame(self) -> Frame:
        """Execute the query and return the result as a subtypes.Frame."""
        return self.sql.query_to_frame(self)

    def from_(self, *args: Any, **kwargs: Any) -> Select:
        """Simple alias for the 'select_from' method. See that method's docstring for documentation."""
        return self.select_from(*args, **kwargs)


class Update(alch.sql.Update, ExpressionMixin):
    """Custom subclass of sqlalchemy.sql.Update with additional useful methods and aliases for existing methods."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)

    def set_(self, *args: Any, **kwargs: Any) -> Update:
        """Simple alias for the 'values' method. See that method's docstring for documentation."""
        return self.values(*args, **kwargs)


class Insert(alch.sql.Insert, ExpressionMixin):
    """Custom subclass of sqlalchemy.sql.Insert with additional useful methods and aliases for existing methods."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)

    def values(self, *args: Any, **kwargs: Any) -> Insert:
        """Insert the given values as either a single dict, or a list of dicts."""
        ret = super().values(*args, **kwargs)
        if isinstance(ret.parameters, list):
            ret.parameters = [{(col.key if isinstance(col, InstrumentedAttribute) else col): (Maybe(val).else_(alch.null()))
                               for col, val in record.items()} for record in ret.parameters]
        return ret


class Delete(alch.sql.Delete, ExpressionMixin):
    """Custom subclass of sqlalchemy.sql.Delete with additional useful methods and aliases for existing methods."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)
