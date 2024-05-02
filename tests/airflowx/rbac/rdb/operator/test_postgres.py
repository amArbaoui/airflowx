from airflowx.security import ProxyUserPostgresOperator
from airflowx.security.rbac.internal.provider import DbProvider
from airflowx.security.rbac.internal.sql import QueryFactory


class TestProxyPostgresOperator:
    def test_proxy_postgres_operator(self):
        test_role = "test_user"
        query_factory = QueryFactory.get_factory(DbProvider.POSTGRES)
        sample_query = query_factory.now_query()
        set_role_query = query_factory.set_role_query(test_role)
        task = ProxyUserPostgresOperator(task_id="test", sql=sample_query, role=test_role)
        assert task.sql[0] == set_role_query
        task = ProxyUserPostgresOperator(task_id="test", sql=[sample_query], role=test_role)
        assert task.sql[0] == set_role_query
