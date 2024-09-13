from clickhouse_sqlalchemy.drivers.compilers.ddlcompiler import ClickHouseDDLCompiler


class ClickHouseCHDBDDLCompiler(ClickHouseDDLCompiler):
    def visit_create_schema(self, create, **kw):
        text = "CREATE DATABASE "
        if create.if_not_exists:
            text += "IF NOT EXISTS "
        return text + self.preparer.format_schema(create.element)

    def visit_drop_schema(self, drop, **kw):
        text = "DROP DATABASE "
        if drop.if_exists:
            text += "IF EXISTS "
        text += self.preparer.format_schema(drop.element)
        if drop.cascade:
            text += " CASCADE"
        return text
