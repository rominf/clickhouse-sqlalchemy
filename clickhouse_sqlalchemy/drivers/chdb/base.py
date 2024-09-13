import sqlalchemy
from sqlalchemy import pool, TextClause
from sqlalchemy.engine.interfaces import ExecuteStyle
from sqlalchemy.util import asbool

from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from clickhouse_sqlalchemy.drivers.native.base import ClickHouseExecutionContext, ClickHouseNativeSQLCompiler
from clickhouse_sqlalchemy.drivers.chdb import connector
from clickhouse_sqlalchemy.drivers.chdb.ddlcompiler import ClickHouseCHDBDDLCompiler

VERSION = (0, 0, 1, None)


class ClickHouseChdbExecutionContext(ClickHouseExecutionContext):
    def create_default_cursor(self):
        return self._dbapi_connection.cursor()

    def pre_exec(self):
        # Always do executemany on INSERT with VALUES clause.
        if (self.isinsert and self.compiled.statement.select is None and
                self.parameters != [{}]):
            self.execute_style = ExecuteStyle.EXECUTEMANY


class ClickHouseDialect_chdb(ClickHouseDialect):
    driver = "chdb"
    execution_ctx_cls = ClickHouseChdbExecutionContext
    ddl_compiler = ClickHouseCHDBDDLCompiler
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls):
        return connector

    @classmethod
    def get_pool_class(cls, url):
        return pool.SingletonThreadPool

    def _get_server_version_info(self, connection):
        return tuple(int(x) for x in self._execute(connection, "SELECT version()", scalar=True).split("."))

    def _execute(self, connection, sql, scalar=False, **kwargs):
        database = kwargs.get("database", None)
        if database is not None and database != "default":
            connection.execute(TextClause(f"USE {database}"))

        if isinstance(sql, str):
            # Makes sure the query will go through the
            # `ClickHouseExecutionContext` logic.
            sql = TextClause(sql)
        f = connection.scalar if scalar else connection.execute
        return f(sql, kwargs)

    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters)

    def do_executemany(self, cursor, statement, parameters, context=None):
        cursor.executemany(statement, parameters)

    def create_connect_args(self, url):
        if url.username or url.password or url.host or url.port:
            raise sqlalchemy.exc.ArgumentError(
                f"Invalid chDB URL: {url}\n"
                "Valid chDB URL forms is:\n"
                " chdb:///?path=relative/path/to/dir\n"
                " chdb:///database?path=relative/path/to/dir\n"
                " chdb:///?path=/absolute/path/to/dir\n"
                " chdb:///database?path=/absolute/path/to/dir\n"
            )
        url = url.set(drivername='clickhouse')
        self.engine_reflection = asbool(
            url.query.get('engine_reflection', 'true')
        )
        path = url.query.get("path", None)
        return (url.render_as_string(hide_password=False), ), {"database": url.database, "path": path}

dialect = ClickHouseDialect_chdb
