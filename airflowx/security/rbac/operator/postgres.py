from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflowx.security.rbac.internal.provider import DbProvider
from airflowx.security.rbac.operator.base import ProxyUserSQLExecuteQueryOperator


class ProxyUserPostgresOperator(ProxyUserSQLExecuteQueryOperator, PostgresOperator):
    def __init__(self, role, *args, **kwargs):
        super().__init__(db_provider=DbProvider.POSTGRES, role=role, *args, **kwargs)
