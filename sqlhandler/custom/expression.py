from __future__ import annotations

from typing import Any, TYPE_CHECKING

import sqlalchemy as alch
from sqlalchemy.orm import InstrumentedAttribute

from maybe import Maybe
from miscutils import ParametrizableMixin

from sqlhandler.frame import Frame

from .result import Result
from .utils import literal_statement, clean_entities

if TYPE_CHECKING:
    from sqlhandler import Sql


class ExpressionMixin(ParametrizableMixin):
    """A mixin providing private methods for logging using expression classes."""

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return literal_statement(self)

    def execute(self) -> Result:
        """Execute this query's statement in the current session."""
        return self.sql.session.execute(self)

    def parametrize(self, param: Sql) -> ExpressionMixin:
        self.sql = param
        return self


class Select(ExpressionMixin, alch.sql.Select):
    """Custom subclass of sqlalchemy.sql.Select with additional useful methods and aliases for existing methods."""

    def __init__(self, *entities) -> None:
        self.__dict__ = self._create_select(*clean_entities(entities)).__dict__

    def frame(self) -> Frame:
        """Execute the query and return the result as a subtypes.Frame."""
        result = self.execute()
        return self.sql.Constructors.Frame(result.all, columns=result.columns)

    def from_(self, *args: Any, **kwargs: Any) -> Select:
        """Simple alias for the 'select_from' method. See that method's docstring for documentation."""
        return self.select_from(*args, **kwargs)

    def subquery(self, name: str = None, with_labels: bool = False, reduce_columns: bool = False):
        for col in (sub := super().subquery(name=name)).c:
            setattr(sub, col.name, col)

        return sub


class Update(ExpressionMixin, alch.sql.Update):
    """Custom subclass of sqlalchemy.sql.Update with additional useful methods and aliases for existing methods."""

    def set_(self, *args: Any, **kwargs: Any) -> Update:
        """Simple alias for the 'values' method. See that method's docstring for documentation."""
        return self.values(*args, **kwargs)


class Insert(ExpressionMixin, alch.sql.Insert):
    """Custom subclass of sqlalchemy.sql.Insert with additional useful methods and aliases for existing methods."""

    def values(self, *args: Any, **kwargs: Any) -> Insert:
        """Insert the given values as either a single dict, or a list of dicts."""
        ret = super().values(*args, **kwargs)
        if isinstance(ret.parameters, list):
            ret.parameters = [{(col.key if isinstance(col, InstrumentedAttribute) else col): (Maybe(val).else_(alch.null()))
                               for col, val in record.items()} for record in ret.parameters]
        return ret


class Delete(ExpressionMixin, alch.sql.Delete):
    """Custom subclass of sqlalchemy.sql.Delete with additional useful methods and aliases for existing methods."""
