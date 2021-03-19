from __future__ import annotations

import os

from typing import Any, TYPE_CHECKING, Union

import pandas as pd

import sqlalchemy as alch
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.dialects import mssql

from subtypes import Frame, DateTime
from pathmagic import File
from miscutils import cached_property
from iotools.misc.serializer import LostObject

from .custom import ModelMeta, Model, TemplatedModel, ReflectedModel, Query, Session, Relationship, SubtypesDateTime, SubtypesDate, BitLiteral, Select, Update, Insert, Delete
from .database.schema import TableSchemas, ViewSchemas
from .utils import StoredProcedure, Script
from .database import Database, Metadata, Schemas, Schema
from .utils import Config, Url
from .enums import Dialect

if TYPE_CHECKING:
    from alembic.operations import Operations


class Sql:
    """
    Provides access to the complete sqlalchemy API, with custom functionality added for logging and pandas integration. Handles authentication through config settings.
    The custom expression classes provided have additional useful methods and are modified by the 'autocommit' attribute to facilitate human-supervised queries.
    The custom query class provided by the Alchemy object's 'session' attribute also has additional methods. Many commonly used sqlalchemy objects are bound to this object as attributes for easy access.
    The 'Sql.orm' and 'Sql.objects' attributes provide access via attribute or item access to the reflected database models and underlying table objects, respectively.
    """

    class Enums:
        Dialect, IfExists = Dialect, Frame.Enums.IfExists

    class Constructors:
        ModelMeta, Model, TemplatedModel, ReflectedModel = ModelMeta, Model, TemplatedModel, ReflectedModel
        Database, Metadata, Schemas, Schema = Database, Metadata, Schemas, Schema
        Config, Query, Session = Config, Query, Session
        Select, Update, Insert, Delete = Select, Update, Insert, Delete
        StoredProcedure, Script = StoredProcedure, Script
        Config, Frame = Config, Frame

    class Settings:
        cache_metadata = reflect_tables = reflect_views = True
        eager_reflection = False
        lazy_schemas = {"information_schema"}

    class Declarative:
        Column, ForeignKey, Index, CheckConstraint, Relationship = alch.Column, alch.ForeignKey, alch.Index, alch.CheckConstraint, Relationship
        String, Integer, SmallInteger, BigInteger, Float, Decimal = alch.String, alch.Integer, alch.SmallInteger, alch.BigInteger, alch.Float, alch.Numeric
        Boolean, Binary, Enum = alch.Boolean, alch.Binary, alch.Enum
        Datetime, Date = SubtypesDateTime, SubtypesDate

    Url = Url

    def __init__(self, url: Union[Url, str], config: Config = None) -> None:
        self.settings = self.Settings()
        self.config = self.Constructors.Config() if config is None else config

        self.engine = self._create_engine(url=url)
        self.engine.connect()

        self.session = self.Constructors.Session(bind=self.engine, query_cls=self.Constructors.Query)
        self.database = self.Constructors.Database(self)

        self.StoredProcedure, self.Script = self.Constructors.StoredProcedure[self], self.Constructors.Script[self]
        self.Select, self.Update, self.Insert, self.Delete = self.Constructors.Select[self], self.Constructors.Update[self], self.Constructors.Insert[self], self.Constructors.Delete[self]
        self.transaction = Transaction(self)

        self.AND, self.OR, self.CAST, self.CASE, self.TRUE, self.FALSE = alch.and_, alch.or_, alch.cast, alch.case, alch.true, alch.false
        self.TEXT, self.LITERAL = alch.text, alch.literal

        self.func, self.sqlalchemy = alch.func, alch

    def __repr__(self) -> str:
        return f"{type(self).__name__}(engine={repr(self.engine)}, database={repr(self.database)})"

    def __len__(self) -> int:
        return len(self.database.meta.tables)

    def __enter__(self) -> Transaction:
        return self.transaction.__enter__()

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        self.transaction.__exit__(ex_type=ex_type, ex_value=ex_value, ex_traceback=ex_traceback)

    def __getstate__(self) -> dict:
        return {"engine": LostObject(self.engine), "database": LostObject(self.database)}

    def __setstate__(self, attrs: dict) -> None:
        self.__dict__ = attrs

    @property
    def Model(self) -> Model:
        """Custom base class for declarative and automap bases to inherit from. Represents a mapped table in a sql database."""
        return self.database.model

    @property
    def TemplatedModel(self) -> TemplatedModel:
        return self.database.templated_model

    @property
    def objects(self) -> Schemas:
        """Property controlling access to all database objects."""
        return self.database.objects

    @property
    def tables(self) -> TableSchemas:
        """Property controlling access to mapped tables."""
        return self.database.tables

    @property
    def views(self) -> ViewSchemas:
        """Property controlling access to mapped views."""
        return self.database.views

    @cached_property
    def operations(self) -> Operations:
        """Property controlling access to alembic operations."""
        from alembic.runtime.migration import MigrationContext
        from alembic.operations import Operations

        return Operations(MigrationContext.configure(self.engine.connect()))

    def query(self, *entities: Any) -> Query:
        return self.session.query(*entities)

    def query_to_frame(self, query: Query, labels: bool = False) -> Frame:
        """Convert sqlalchemy.orm.Query object to a pandas DataFrame. Optionally apply table labels to columns and/or print an ascii representation of the DataFrame."""
        query = query.with_labels() if labels else query

        result = self.session.execute(query.statement)
        cols = [col[0] for col in result.cursor.description]
        frame = self.Constructors.Frame(result.fetchall(), columns=cols)

        return frame

    def plaintext_query_to_frame(self, query: str) -> Frame:
        """Convert plaintext SQL to a pandas DataFrame. The SQL statement must be a SELECT that returns rows."""
        return self.Constructors.Frame(pd.read_sql_query(query, self.engine))

    def table_to_frame(self, table: str, schema: str = None) -> Frame:
        """Reads the target table or view (from the specified schema) into a pandas DataFrame."""
        return self.Constructors.Frame(pd.read_sql_table(table, self.engine, schema=schema))

    def excel_to_table(self, filepath: os.PathLike, table: str = "temp", schema: str = None, if_exists: Sql.Enums.IfExists = Enums.IfExists.FAIL, primary_key: str = "id", **kwargs: Any) -> Model:
        """Bulk insert the content of the target '.xlsx' file to the specified table."""
        return self.frame_to_table(dataframe=self.Constructors.Frame.from_excel(filepath, **kwargs), table=table, schema=schema, if_exists=if_exists, primary_key=primary_key)

    def frame_to_table(self, dataframe: pd.DataFrame, table: str, schema: str = None, if_exists: Sql.Enums.IfExists = Enums.IfExists.FAIL, primary_key: str = "id") -> Model:
        """Bulk insert the content of a pandas DataFrame to the specified table."""
        dataframe = self.Constructors.Frame(dataframe)

        has_identity_pk = False
        if primary_key is None:
            dataframe.reset_index(inplace=True)
            primary_key = dataframe.iloc[:, 0].name
        else:
            if primary_key in dataframe.columns:
                dataframe.set_index(primary_key, inplace=True)
            else:
                has_identity_pk = True
                dataframe.reset_index(inplace=True, drop=True)
                dataframe.index.names = [primary_key]
                dataframe.index += 1

            dataframe.reset_index(inplace=True)

        dtypes = self._sql_dtype_dict_from_frame(dataframe)
        if has_identity_pk:
            dtypes[primary_key] = alch.types.INT

        dataframe.to_sql(engine=self.engine, name=table, if_exists=if_exists, index=False, primary_key=primary_key, schema=schema, dtype=dtypes)

        self.database._sync_with_db()
        return self.tables[schema][table]()

    def orm_to_frame(self, orm_objects: Any) -> Frame:
        """Convert a homogeneous list of sqlalchemy.orm instance objects (or a single one) to a pandas DataFrame."""
        if not isinstance(orm_objects, list):
            orm_objects = [orm_objects]

        if not all([type(orm_objects[0]) == type(item) for item in orm_objects]):
            raise TypeError("All sqlalchemy.orm mapped objects passed into this function must have the same type.")

        cols = [col.name for col in list(type(orm_objects[0]).__table__.columns)]
        vals = [[getattr(item, col) for col in cols] for item in orm_objects]

        return self.Constructors.Frame(vals, columns=cols)

    def _create_engine(self, url: Union[Url, str]) -> alch.engine.base.Engine:
        engine = alch.create_engine(str(url), echo=False, dialect=self._customize_dialect(url.get_dialect()())) if isinstance(url, Url) else alch.create_engine(str(url), echo=False)
        engine.sql = self

        return engine

    def _customize_dialect(self, dialect: DefaultDialect) -> DefaultDialect:
        dialect.colspecs.update(
            {
                alch.types.DateTime: SubtypesDateTime,
                alch.types.DATETIME: SubtypesDateTime,
                alch.types.Date: SubtypesDate,
                alch.types.DATE: SubtypesDate,
            }
        )

        if isinstance(dialect, mssql.dialect):
            dialect.supports_multivalues_insert = True
            dialect.colspecs.update({alch.dialects.mssql.BIT: BitLiteral})

        return dialect

    @staticmethod
    def _sql_dtype_dict_from_frame(frame: Frame) -> dict[str, Any]:
        return {name: Sql._sqlalchemy_dtype_from_series(col) for name, col in frame.infer_objects().iteritems() if col.dtype.name in ["int64", "Int64", "object"]}

    @staticmethod
    def _sqlalchemy_dtype_from_series(series: pd.code.series.Series) -> Any:
        if series.dtype.name in ["int64", "Int64"]:
            if series.isnull().all():
                return alch.types.Integer
            else:
                minimum, maximum = series.min(), series.max()

                if 0 <= minimum and maximum <= 255:
                    return alch.dialects.mssql.TINYINT
                elif -2**15 <= minimum and maximum <= 2**15:
                    return alch.types.SmallInteger
                elif -2**31 <= minimum and maximum <= 2**31:
                    return alch.types.Integer
                else:
                    return alch.types.BigInteger
        elif series.dtype.name == "object":
            return alch.types.String(int((series.fillna("").astype(str).str.len().max()//50 + 1)*50))
        else:
            raise TypeError(f"Don't know how to process column type '{series.dtype}' of '{series.name}'.")

    @classmethod
    def from_connection(cls, connection: str = None, database: str = None, log: File = None, autocommit: bool = False) -> Sql:
        config = cls.Constructors.Config()
        url = config.generate_url(connection=connection, database=database)
        return cls(url=url, config=config)

    @classmethod
    def from_memory(cls) -> Sql:
        return cls("sqlite://")


class Transaction:
    def __init__(self, sql: Sql) -> None:
        self.sql, self.raw, self.start = sql, sql.session.transaction, None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(state={repr(self.state)}, start={repr(self.start)})"

    def __enter__(self) -> Transaction:
        self.sql.session.rollback()
        self.now = DateTime.now()
        return self

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        self.sql.session.commit() if ex_type is None else self.sql.session.rollback()
        self.now = None

    @property
    def state(self) -> str:
        return self.raw._state.name
