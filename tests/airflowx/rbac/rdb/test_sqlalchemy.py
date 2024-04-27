import pytest
import sqlalchemy
from testcontainers.core.generic import DbContainer

from airflowx.security.rbac.internal.provider import DbProvider
from airflowx.security.rbac.internal.sql import QueryFactory, AbstractQueryFactory
from airflowx.security.rbac.sqlalchemy import ProxyEngineWrapper


class TestProxyEngineWrapper:
    @pytest.mark.parametrize(
        "container,provider,expected_role",
        [
            ("postgres_container", DbProvider.POSTGRES, "test_user"),
            ("mysql_container", DbProvider.MYSQL, "`test_user`@`%`"),
        ],
    )
    def test_as_role(
        self, container: DbContainer, provider: DbProvider, expected_role: str, request
    ):
        query_factory = get_query_factory(provider)
        db = request.getfixturevalue(container)
        engine = ProxyEngineWrapper.rbac_engine_from_url(
            db.get_connection_url(), as_role=expected_role
        )
        with engine.connect() as cnn:
            cnn.execute(sqlalchemy.text(query_factory.now_query()))
            assert get_current_user(query_factory, cnn) == expected_role
        with engine.connect() as cnn:
            cnn.execute(sqlalchemy.text(query_factory.now_query()))
            assert get_current_user(query_factory, cnn) == expected_role

        with engine.connect() as cnn:
            with cnn.begin():
                cnn.execute(sqlalchemy.text(query_factory.now_query()))
                assert get_current_user(query_factory, cnn) == expected_role
            with cnn.begin():
                cnn.execute(sqlalchemy.text(query_factory.now_query()))
                assert get_current_user(query_factory, cnn) == expected_role


def get_query_factory(provider: DbProvider):
    return QueryFactory.get_factory(provider)


def get_current_user(query_factory: AbstractQueryFactory, cnn):
    return cnn.execute(sqlalchemy.text(query_factory.current_user_query())).one()[0]
