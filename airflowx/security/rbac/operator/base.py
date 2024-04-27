from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflowx.security.rbac.internal.provider import DbProvider
from airflowx.security.rbac.internal.sql import QueryFactory


class ProxySQLExecuteQueryOperator(SQLExecuteQueryOperator):
    def __init__(self, db_provider: DbProvider, role: str, *args, **kwargs):
        self.query_factory = QueryFactory.get_factory(db_provider)
        self.role = role
        self._as_role(kwargs)
        super().__init__(*args, **kwargs)

    def _as_role(self, kwargs):
        sql = kwargs.get("sql")
        if sql:
            if isinstance(sql, str):
                sql = [sql]
            sql.insert(0, self.query_factory.set_role_query(role=self.role))
            kwargs["sql"] = sql
