from __future__ import annotations

from typing import Sequence, TYPE_CHECKING

from sqlalchemy import literal, Column
from sqlalchemy.orm import InstrumentedAttribute

if TYPE_CHECKING:
    from sqlhandler.custom import ModelMeta, Table


def valid_instrumented_attributes(model: ModelMeta) -> list[InstrumentedAttribute]:
    return [val for val in vars(model).values() if isinstance(val, InstrumentedAttribute) and not val.key == "__pk__"]


def valid_columns(table: Table) -> list[Column]:
    return [val for val in table.c if isinstance(val, Column) and not val.key == "__pk__"]


def clean_entities(entities: Sequence) -> list:
    from sqlhandler.custom import ModelMeta, Table

    processed_entities = []

    # TODO: convert to match statement

    for entity in entities:
        if isinstance(entity, ModelMeta):
            if hasattr(entity, '__pk__'):
                for instrumented_attr in valid_instrumented_attributes(model=entity):
                    processed_entities.append(instrumented_attr)
            else:
                processed_entities.append(entity)
        elif isinstance(entity, Table):
            for column in valid_columns(table=entity):
                processed_entities.append(column)
        elif hasattr(entity, "__module__") and entity.__module__.startswith("sqlalchemy."):
            processed_entities.append(entity)
        elif isinstance(entity, (str, int, bool, float)) or entity is None:
            processed_entities.append(literal(entity))
        else:
            processed_entities.append(literal(str(entity)))

    return processed_entities
