from __future__ import annotations

from typing import Any

import sqlalchemy as alch
from sqlalchemy.orm import backref

from sqlhandler.frame import Frame

from .utils import literal_statement


class Query(alch.orm.Query):
    """Custom subclass of sqlalchemy.orm.Query with additional useful methods and aliases for existing methods."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(\n{(str(self))}\n)"

    def __str__(self) -> str:
        return literal_statement(self)

    def frame(self, labels: bool = False) -> Frame:
        """Execute the query and return the result as a pandas DataFrame."""
        return self.session.bind.sql.query_to_frame(self, labels=labels)

    def vector(self) -> list:
        """Transpose all records in a single column into a list. If the query returns more than one column, this will raise a RuntimeError."""
        vals = self.all()
        if all(len(row) == 1 for row in vals):
            return [row[0] for row in vals]
        else:
            raise RuntimeError("Multiple columns selected. Expected exactly one value per row, got multiple.")

    def from_(self, *from_obj: Any) -> Query:
        """Simple alias for the 'select_from' method. See that method's docstring for documentation."""
        return self.select_from(*from_obj)

    def where(self, *criterion: Any) -> Query:
        """Simple alias for the 'filter' method. See that method's docstring for documentation."""
        return self.filter(*criterion)

    def update(self, values: Any, synchronize_session: Any = "fetch", update_args: dict = None) -> int:
        """Simple alias for the '.update()' method, with the default 'synchronize_session' argument set to 'fetch', rather than 'evaluate'. Check that method for documentation."""
        return super().update(values, synchronize_session=synchronize_session)

    def delete(self, synchronize_session: Any = "fetch") -> int:
        """Simple alias for the '.delete()' method, with the default 'synchronize_session' argument set to 'fetch', rather than 'evaluate'. Check that method for documentation."""
        return super().delete(synchronize_session=synchronize_session)

    def subquery(self, name: str = None, with_labels: bool = False, reduce_columns: bool = False):
        sub = super().subquery(name=name, with_labels=with_labels, reduce_columns=reduce_columns)
        for col in sub.c:
            setattr(sub, col.name, col)
        return sub
