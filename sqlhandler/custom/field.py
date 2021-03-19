from __future__ import annotations

from sqlalchemy import types

from subtypes import DateTime, Date


class BitLiteral(types.TypeDecorator):
    impl = types.DateTime

    def process_literal_param(self, value, dialect):
        return str(int(value))


class SubtypesDateTime(types.TypeDecorator):
    impl = types.DateTime
    string = types.String()

    def process_bind_param(self, value, dialect):
        return None if value is None else DateTime.infer(value).to_isoformat()

    def process_literal_param(self, value, dialect):
        return None if value is None else self.string.literal_processor(dialect)(DateTime.infer(value).to_isoformat())

    def process_result_value(self, value, dialect):
        return None if value is None else DateTime.infer(value)


class SubtypesDate(types.TypeDecorator):
    impl = types.Date
    string = types.String()

    def process_bind_param(self, value, dialect):
        return None if value is None else Date.infer(value).to_isoformat()

    def process_literal_param(self, value, dialect):
        return None if value is None else self.string.literal_processor(dialect)(Date.infer(value).to_isoformat())

    def process_result_value(self, value, dialect):
        return None if value is None else Date.infer(value)
