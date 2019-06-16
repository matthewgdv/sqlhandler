from __future__ import annotations

import os
from typing import Any, Set, Tuple, Dict, Union

import numpy as np
import pandas as pd
import sqlalchemy as alch
from sqlalchemy.ext.automap import AutomapBase, automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import aliased, backref, make_transient, relationship

from maybe import Maybe
from subtypes import Str, Frame
from pathmagic import File
from miscutils import NullContext

from .custom import Base, Query, Session, Select, Update, Insert, Delete, SelectInto, StringLiteral, BitLiteral
from .utils import MetadataCacheHelper, TempManager, StoredProcedure, OrmDatabase, Database
from .log import SqlLog
from .resources.__config__ import databases


class Alchemy:
    """
    Provides access to the complete sqlalchemy API, with custom functionality added for logging and pandas integration. Handles authentication through config settings and relects all schemas passed to the constructor.
    The custom expression classes provided have additional useful methods and are modified by the 'autocommit' and 'printing' attributes (can be set at construction time) to facilitate human-supervised queries.
    The custom query class provided by the Alchemy object's 'session' attribute also has additional methods. Many commonly used sqlalchemy objects are bound to this object as attributes for easy access.
    """

    def __init__(self, schemas: Set[str] = None, database: str = None, log: File = None, printing: bool = False, autocommit: bool = False) -> None:
        self.server, self.database = self._get_database_connection_credentials(db=database)
        self.engine = self._create_engine()
        self.session = Session.from_alchemy(self)(self.engine)

        self.declaration: Base = None
        self.reflection: AutomapBase = None

        self._cacher = MetadataCacheHelper(alchemy=self)

        self.meta: alch.MetaData = self._cacher.database.meta
        self.meta.bind = self.engine

        self._schemas = self._cacher.database.schemas
        self.schemas = Maybe(schemas).else_({None})

        self.tables, self.objects = OrmDatabase(self), Database(self)

        self.log, self.printing, self.autocommit = log, printing, autocommit

        self.Text, self.Literal, self.Case, self.Trans, self.Alias = alch.text, alch.literal, alch.case, make_transient, aliased
        self.AND, self.OR, self.CAST = alch.and_, alch.or_, alch.cast
        self.Select, self.SelectInto, self.Update = Select.from_alchemy(self), SelectInto.from_alchemy(self), Update.from_alchemy(self)
        self.Insert, self.Delete = Insert.from_alchemy(self), Delete.from_alchemy(self)
        self.StoredProcedure = StoredProcedure.from_alchemy(self)
        self.Table, self.Column, self.ForeignKey, self.Relationship, self.Backref = alch.Table, alch.Column, alch.ForeignKey, relationship, backref
        self.type, self.func, self.sqlalchemy = alch.types, alch.func, alch

        pd.set_option("max_columns", None)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(engine={repr(self.engine)}, Tables={len(self)})"

    def __len__(self) -> int:
        return len(self.meta.tables)

    def __getitem__(self, key: str) -> alch.Table:
        if key not in self.meta.tables:
            self.refresh()
        return self.meta.tables[key]

    def __enter__(self) -> Alchemy:
        self.session.rollback()
        return self

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        if ex_type is None:
            self.session.commit()
        else:
            self.session.rollback()

    @property
    def schemas(self) -> Set[str]:
        return self._schemas

    @schemas.setter
    def schemas(self, val: Set[str]) -> None:
        self._extend_metadata(schemas=val)

    @property
    def log(self) -> SqlLog:
        return self._log

    @log.setter
    def log(self, val: File) -> None:
        self._log = SqlLog(logfile=val, active=False) if val is not None else NullContext()

    def initialize_log(self, logname: str, logdir: str = None) -> SqlLog:
        """Instantiates a matt.log.SqlLog object from a name and a dirpath, and binds it to this object's 'log' attribute. If 'active' argument is 'False', this method does nothing."""
        self._log = SqlLog.from_details(log_name=logname, log_dir=logdir, active=False)

    # Conversion Methods

    def query_to_frame(self, query: Query, labels: bool = False, print_ascii: bool = False) -> Frame:
        """Convert sqlalchemy.orm.Query object to a pandas DataFrame. Optionally apply table labels to columns and/or print an ascii representation of the DataFrame."""
        query = query.with_labels() if labels else query

        result = self.session.execute(query.statement)
        cols = [col[0] for col in result.cursor.description]
        frame = Frame(result.fetchall(), columns=cols)

        if print_ascii:
            print(frame.to_ascii())

        return frame

    def plaintext_query_to_frame(self, query: str) -> Frame:
        """Convert plaintext SQL to a pandas DataFrame. The SQL statement must be a SELECT that returns rows."""
        return Frame(pd.read_sql_query(query, self.engine))

    def table_to_frame(self, table: str, schema: str = None) -> Frame:
        """Reads the target table or view (from the specified schema) into a pandas DataFrame."""
        return Frame(pd.read_sql_table(table, self.engine, schema=schema))

    def excel_to_table(self, filepath: os.PathLike, tablename: str = "temp", schema: str = None, if_exists: str = "fail") -> Base:
        """Bulk insert the contents of the target '.xlsx' file to the specified table. The table is created with Primary Key 'id' field. Options for 'if_exists' are 'fail' (default), 'append', and 'replace'."""
        return self.frame_to_table(self.excel_to_frame(os.fspath(filepath)), table=tablename, schema=schema, if_exists=if_exists)

    def frame_to_table(self, dataframe: pd.DataFrame, table: str, schema: str = None, if_exists: str = "fail", primary_key: str = "id") -> Base:
        """Bulk insert the contents of a pandas DataFrame to the specified table. The table is created with a Primary Key 'id' field. Options for 'if_exists' are 'fail' (default), 'append', and 'replace'."""
        if primary_key in dataframe.columns:
            raise ValueError("DataFrame may not have a column named 'id'.")

        dataframe.infer_dtypes().to_sql(table, self.engine, schema=schema, if_exists=if_exists, index=False, dtype=self._sql_dtype_dict_from_frame(dataframe))

        if if_exists.lower() != "append":
            self.prepend_identity_field_to_table(table=table, schema=schema, field_name=primary_key)

        self.refresh_table(table=f"{(Maybe(schema) + '.').else_('')}{table}")
        return getattr(getattr(self.tables, Maybe(schema).else_('dbo')), table)

    @staticmethod
    def orm_to_frame(orm_objects: Any) -> Frame:
        """Convert a homogeneous list of sqlalchemy.orm instance objects (or a single one) to a pandas DataFrame."""
        if not isinstance(orm_objects, list):
            orm_objects = [orm_objects]

        if not all([type(orm_objects[0]) == type(item) for item in orm_objects]):
            raise TypeError("All sqlalchemy.orm mapped objects passed into this function must have the same type.")

        cols = [col.name for col in list(type(orm_objects[0]).__table__.columns)]
        vals = [[getattr(item, col) for col in cols] for item in orm_objects]

        return Frame(vals, columns=cols)

    @staticmethod
    def excel_to_frame(filepath: os.PathLike, sanitize_colnames: bool = True, **kwargs: Any) -> Frame:
        """Reads in the specified Excel spreadsheet into a pandas DataFrame. Passes on arguments to the pandas read_excel function. Optionally snake_cases column names and strips out non-ascii characters."""
        return Frame.from_excel(os.fspath(filepath), sanitize_colnames=sanitize_colnames, **kwargs)

    def resolve_tran(self, force_autocommit: bool = False) -> None:
        """Request user confirmation to resolve the ongoing transaction."""
        if self.autocommit or force_autocommit:
            self.session.commit()
            if self.printing:
                print("COMMIT;\n")
        else:
            user_confirmation = input("\nIf you are happy with the above Query/Queries please type COMMIT. Anything else will roll back the ongoing Transaction.\n\n")
            if user_confirmation.upper() == "COMMIT":
                self.session.commit()
                if self.printing:
                    print("\nCOMMIT;\n")
            else:
                self.session.rollback()
                if self.printing:
                    print("\nROLLBACK;\n")

    def drop_table(self, table: alch.schema.Table) -> None:
        """Drop a table or the table belonging to to an ORM class and remove it from the metadata."""
        table = self._normalize_table(table)
        table.drop()
        self.meta.remove(table)
        self.refresh()

    def refresh_table(self, table: Union[str, alch.schema.Table, Base]) -> alch.schema.Table:
        table = self._normalize_table(table)
        name, schema = table.name, table.schema
        self.meta.remove(table)
        self.refresh()
        return self[f"{(Maybe(schema) + '.').else_('')}{name}"]

    def refresh(self, clear_first: bool = False) -> None:
        (self._refresh_metadata if clear_first else self._extend_metadata)(self.schemas)

    def prepend_identity_field_to_table(self, table: str, schema: str = None, field_name: str = "id") -> None:
        tableschema = f"{(Maybe(schema) + '.').else_('')}{table}"

        with TempManager(alchemy=self) as tmp:
            self.session.execute(f"ALTER TABLE {tableschema} ADD __tmp_id__ INT IDENTITY(1, 1) NOT NULL;", autocommit=True)

            sqltable = self.refresh_table(tableschema)
            self.SelectInto([sqltable.c.__tmp_id__.label(field_name)] + [sqltable.c[col.name] for col in sqltable.columns if col.name not in [field_name, "__tmp_id__"]], table=str(tmp)).execute(autocommit=True)
            self.drop_table(sqltable)

            self.SelectInto([tmp()], table=table, schema=schema).execute(autocommit=True)
            self.session.execute(f"ALTER TABLE {tableschema} ADD CONSTRAINT pk_{tableschema.replace('.', '_')} PRIMARY KEY CLUSTERED (id);", autocommit=True)

    # Private internal methods

    def _extend_metadata(self, schemas: Set[str]) -> None:
        self._schemas = self.schemas.union(schemas)
        for schema in self.schemas:
            self.meta.reflect(schema=schema, views=True)
        self._refresh_bases()
        self._cacher.save_metadata_to_cache()

    def _refresh_metadata(self, schemas: Set[str]) -> None:
        self.meta.clear()
        self._schemas = set()
        self._extend_metadata(schemas)

    def _refresh_bases(self) -> None:
        self.declaration = declarative_base(bind=self.engine, metadata=self.meta, cls=Base)
        self.declaration.alchemy = self
        self.reflection = automap_base(declarative_base=self.declaration)
        self.reflection.prepare(name_for_collection_relationship=self._pluralize_collection)

    def _normalize_table(self, table: Any) -> alch.schema.Table:
        if hasattr(table, "__table__"):
            table = table.__table__
        if isinstance(table, str):
            table = self[table]
        return table

    def _create_engine(self) -> alch.engine.base.Engine:
        temp_engine = alch.create_engine(fR"mssql+pyodbc://@{self.server}/{self.database}?driver=SQL+Server", echo=False)
        return alch.create_engine(fR"mssql+pyodbc://@{self.server}/{self.database}?driver=SQL+Server", echo=False, dialect=self._create_literal_dialect(type(temp_engine.dialect)))

    def _create_literal_dialect(self, dialect_class: alch.engine.default.DefaultDialect) -> alch.engine.default.DefaultDialect:
        class LiteralDialect(dialect_class):
            supports_multivalues_insert = True

            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                self.colspecs.update(
                    {
                        alch.sql.sqltypes.String: StringLiteral,
                        alch.sql.sqltypes.DateTime: StringLiteral,
                        alch.sql.sqltypes.Date: StringLiteral,
                        alch.sql.sqltypes.NullType: StringLiteral,
                        alch.dialects.mssql.BIT: BitLiteral
                    }
                )

        return LiteralDialect()

    @staticmethod
    def _sql_dtype_dict_from_frame(frame: Frame) -> Dict[str, Any]:
        def isnull(val: Any) -> bool:
            return val is None or np.isnan(val)

        def sqlalchemy_dtype_from_series(series: pd.code.series.Series) -> Any:
            if series.dtype.name in ["int64", "Int64"]:
                nums = [num for num in series if not isnull(num)]
                minimum, maximum = min(nums), max(nums)

                if 0 <= minimum and maximum <= 255:
                    return alch.dialects.mssql.TINYINT
                elif -2**15 <= minimum and maximum <= 2**15:
                    return alch.types.SMALLINT
                elif -2**31 <= minimum and maximum <= 2**31:
                    return alch.types.INT
                else:
                    return alch.types.BIGINT
            elif series.dtype.name == "object":
                return alch.types.String(int((series.fillna("").astype(str).str.len().max()//50 + 1)*50))
            else:
                raise TypeError(f"Don't know how to process column type '{series.dtype}' of '{series.name}'.")

        return {name: sqlalchemy_dtype_from_series(col) for name, col in frame.infer_objects().iteritems() if col.dtype.name in ["int64", "Int64", "object"]}

    @staticmethod
    def _get_database_connection_credentials(db: str) -> Tuple[str, str]:
        """Fetch server and database names from the config file (PC-name-specific)."""
        pc_name = os.environ["COMPUTERNAME"]
        if pc_name in databases:
            server = databases[pc_name]["server"]
            if db is None:
                database = databases[pc_name]["default_database"]
            else:
                if db in databases[pc_name]["databases"]:
                    database = db
                else:
                    raise ValueError(f"""Unrecognized Database: '{db}'. Known databases are: {", ".join([f"'{name}'" for name in databases[pc_name]["databases"]])}.""")
        else:
            raise RuntimeError(f"""Cannot establish database connection implicitly. Must be using one of the following supported PCs:\n\n{", ".join([f"'{pc}'" for pc in databases])}.""")

        return server, database

    @staticmethod
    def _pluralize_collection(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
        """Produce a 'snake_cased', 'pluralized' class name, e.g. 'SomeTerm' -> 'some_terms'"""
        referred_name = referred_cls.__name__
        return str(Str(referred_name).snake_case().plural())
