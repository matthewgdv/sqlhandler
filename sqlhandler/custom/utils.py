from __future__ import annotations

from typing import Any, Sequence, TYPE_CHECKING

from sqlalchemy import Column, literal
from sqlalchemy.orm import InstrumentedAttribute
import sqlparse

if TYPE_CHECKING:
    from sqlhandler.custom import ModelMeta, Table


def literal_statement(statement: Any, format_statement: bool = True) -> str:
    """Returns this a query or expression object's statement as raw SQL with inline literal binds."""

    bound = statement.compile(compile_kwargs={'literal_binds': True}).string + ";"
    formatted = sqlparse.format(bound, reindent=True) if format_statement else bound  # keyword_case="upper" (removed arg due to false positives)

    # stage1 = Str(formatted).re.sub(r"\bOVER\s*\(\s*", lambda m: "OVER (").re.sub(r"OVER \((ORDER\s*BY|PARTITION\s*BY)\s+(\S+)\s+(ORDER\s*BY|PARTITION\s*BY)\s+(\S+)\s*\)", lambda m: f"OVER ({m.group(1)} {m.group(2)} {m.group(3)} {m.group(4)})")
    # stage2 = stage1.re.sub(r"(?<=\n)([^\n]*JOIN[^\n]*)(\bON\b[^\n;]*)(?=[\n;])", lambda m: f"  {m.group(1).strip()}\n    {m.group(2).strip()}")
    # stage3 = stage2.re.sub(r"(?<=\bJOIN[^\n]+\n\s+ON[^\n]+\n(?:\s*AND[^\n]+\n)*?)(\s*AND[^\n]+)(?=[\n;])", lambda m: f"    {m.group(1).strip()}")

    return formatted


def valid_instrumented_attributes(model: ModelMeta) -> list[InstrumentedAttribute]:
    return [val for val in vars(model).values() if isinstance(val, InstrumentedAttribute) and not val.key == "__pk__"]


def valid_columns(table: Table) -> list[InstrumentedAttribute]:
    return [val for val in vars(table).values() if isinstance(val, InstrumentedAttribute) and not val.key == "__pk__"]


def clean_entities(entities: Sequence) -> list:
    from sqlhandler.custom import ModelMeta, Table

    processed_entities = []
    for entity in entities:
        if isinstance(entity, ModelMeta):
            for instrumented_attr in valid_instrumented_attributes(model=entity):
                processed_entities.append(instrumented_attr)
        elif isinstance(entity, Table):
            for column in valid_columns(table=entity):
                processed_entities.append(column)
        if hasattr(entity, "__module__") and entity.__module__.startswith("sqlalchemy."):
            processed_entities.append(entity)
        else:
            processed_entities.append(literal(entity))

    return processed_entities
