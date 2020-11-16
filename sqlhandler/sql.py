from __future__ import annotations

import os

from typing import Any, Dict, TYPE_CHECKING


import numpy as np
import pandas as pd

import sqlalchemy as alch
from sqlalchemy.orm import backref, relationship
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.dialects import mssql

from subtypes import Frame, Enum
from pathmagic import File, PathLike
from miscutils import cached_property
from iotools.misc.serializer import LostObject

from sqlhandler.custom import ModelMeta, Model, AutoModel, ReflectedModel, Query, Session, ForeignKey, Relationship, SubtypesDateTime, SubtypesDate, BitLiteral, Select, Update, Insert, Delete, SelectInto
from sqlhandler.database.schema import TableSchemas, ViewSchemas
from sqlhandler.utils import StoredProcedure, Script, SqlLog
from sqlhandler.database import Database, Metadata, Schemas, Schema
from sqlhandler.utils import Config, Url

if TYPE_CHECKING:
    from alembic.operations import Operations


class Sql:
    """
    Provides access to the complete sqlalchemy API, with custom functionality added for logging and pandas integration. Handles authentication through config settings.
    The custom expression classes provided have additional useful methods and are modified by the 'autocommit' attribute to facilitate human-supervised queries.
    The custom query class provided by the Alchemy object's 'session' attribute also has additional methods. Many commonly used sqlalchemy objects are bound to this object as attributes for easy access.
    The 'Sql.orm' and 'Sql.objects' attributes provide access via attribute or item access to the reflected database models and underlying table objects, respectively.
    """

    class IfExists(Enum):
        FAIL, REPLACE, APPEND = "fail", "replace", "append"

    class Constructors:
        ModelMeta, Model, AutoModel, ReflectedModel = ModelMeta, Model, AutoModel, ReflectedModel
        Database, Metadata, Schemas, Schema = Database, Metadata, Schemas, Schema
        Config, Query, Session = Config, Query, Session
        StoredProcedure, Script = StoredProcedure, Script
        Config, Frame = Config, Frame

    class Settings:
        cache_metadata = reflect_tables = reflect_views = True
        eager_reflection = False
        lazy_schemas = {"information_schema"}

    def __init__(self, connection: str = None, database: str = None, log: File = None, autocommit: bool = False) -> None:
        self.constructors, self.settings = self.Constructors(), self.Settings()
        self.config = self.constructors.Config()

        self.engine = self._create_engine(connection=connection, database=database)
        self.engine.connect()

        self.session = self.constructors.Session(bind=self.engine, query_cls=self.constructors.Query)
        self.database = self.constructors.Database(self)

        self.log, self.autocommit = log, autocommit

        self.Select, self.SelectInto, self.Update, self.Insert, self.Delete = Select, SelectInto, Update, Insert, Delete
        self.StoredProcedure, self.Script = self.constructors.StoredProcedure.from_sql(self), self.constructors.Script.from_sql(self)

        self.text, self.literal = alch.text, alch.literal
        self.AND, self.OR, self.CAST, self.CASE, self.TRUE, self.FALSE = alch.and_, alch.or_, alch.cast, alch.case, alch.true(), alch.false()

        self.Table, self.Column, self.ForeignKey, self.Index, self.CheckConstraint, self.Relationship, self.relationship, self.backref = alch.Table, alch.Column, ForeignKey, alch.Index, alch.CheckConstraint, Relationship, relationship, backref
        self.Datetime, self.Date = SubtypesDateTime, SubtypesDate
        self.type, self.func, self.sqlalchemy = alch.types, alch.func, alch

    def __repr__(self) -> str:
        return f"{type(self).__name__}(engine={repr(self.engine)}, database={repr(self.database)})"

    def __len__(self) -> int:
        return len(self.database.meta.tables)

    def __enter__(self) -> Sql:
        self.session.rollback()
        return self

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        self.session.commit() if ex_type is None else self.session.rollback()

    def __getstate__(self) -> dict:
        return {"engine": LostObject(self.engine), "database": LostObject(self.database), "autocommit": self.autocommit}

    def __setstate__(self, attrs: dict) -> None:
        self.__dict__ = attrs

    @property
    def Model(self) -> Model:
        """Custom base class for declarative and automap bases to inherit from. Represents a mapped table in a sql database."""
        return self.database.model

    @property
    def AutoModel(self) -> AutoModel:
        return self.database.auto_model

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

    @property
    def log(self) -> SqlLog:
        """Property controlling access to a special SqlLog class. When setting, a simple file path should be used and an appropriate SqlLog will be created."""
        return self._log

    @log.setter
    def log(self, val: File) -> None:
        self._log = SqlLog(path=val)

    def initialize_log(self, name: str, location: PathLike = None) -> SqlLog:
        """Instantiates a matt.log.SqlLog object from a name and a dirpath, and binds it to this object's 'log' attribute. If 'active' argument is 'False', this method does nothing."""
        self._log = SqlLog.from_details(log_name=name, log_dir=location, active=False)
        return self._log

    # Conversion Methods

    def query_to_frame(self, query: Query, labels: bool = False) -> Frame:
        """Convert sqlalchemy.orm.Query object to a pandas DataFrame. Optionally apply table labels to columns and/or print an ascii representation of the DataFrame."""
        query = query.with_labels() if labels else query

        result = self.session.execute(query.statement)
        cols = [col[0] for col in result.cursor.description]
        frame = self.constructors.Frame(result.fetchall(), columns=cols)

        return frame

    def plaintext_query_to_frame(self, query: str) -> Frame:
        """Convert plaintext SQL to a pandas DataFrame. The SQL statement must be a SELECT that returns rows."""
        return self.constructors.Frame(pd.read_sql_query(query, self.engine))

    def table_to_frame(self, table: str, schema: str = None) -> Frame:
        """Reads the target table or view (from the specified schema) into a pandas DataFrame."""
        return self.constructors.Frame(pd.read_sql_table(table, self.engine, schema=schema))

    def excel_to_table(self, filepath: os.PathLike, table: str = "temp", schema: str = None, if_exists: Sql.IfExists = IfExists.FAIL, primary_key: str = "id", **kwargs: Any) -> Model:
        """Bulk insert the content of the target '.xlsx' file to the specified table."""
        return self.frame_to_table(dataframe=self.constructors.Frame.from_excel(filepath, **kwargs), table=table, schema=schema, if_exists=if_exists, primary_key=primary_key)

    def frame_to_table(self, dataframe: pd.DataFrame, table: str, schema: str = None, if_exists: Sql.IfExists = IfExists.FAIL, primary_key: str = "id") -> Model:
        """Bulk insert the content of a pandas DataFrame to the specified table."""
        dataframe = self.constructors.Frame(dataframe)

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

        return self.constructors.Frame(vals, columns=cols)

    # Private internal methods

    def _create_engine(self, connection: str, database: str) -> alch.engine.base.Engine:
        url = self._create_url(connection=connection, database=database)

        engine = alch.create_engine(str(url), echo=False, dialect=self._customize_dialect(url.get_dialect()()))
        engine.sql = self

        return engine

    def _create_url(self, connection: str, database: str) -> Url:
        return self.config.generate_url(connection=connection, database=database)

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
        def sqlalchemy_dtype_from_series(series: pd.code.series.Series) -> Any:
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

        return {name: sqlalchemy_dtype_from_series(col) for name, col in frame.infer_objects().iteritems() if col.dtype.name in ["int64", "Int64", "object"]}
