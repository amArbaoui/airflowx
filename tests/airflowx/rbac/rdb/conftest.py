import pytest
import sqlalchemy.engine
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from airflowx.security.rbac.internal.provider import DbProvider
from airflowx.security.rbac.internal.sql import QueryFactory


@pytest.fixture
def postgres_container() -> PostgresContainer:
    with PostgresContainer("postgres:16") as postgres:
        prepare_db(db_container=postgres, db_provider=DbProvider.POSTGRES)
        yield postgres


@pytest.fixture
def mysql_container() -> MySqlContainer:
    with MySqlContainer(
        "mysql:8.0.32", username="root", root_password="password"
    ) as mysql:
        prepare_db(mysql, DbProvider.MYSQL)
        yield mysql


def prepare_db(db_container, db_provider: DbProvider):
    test_role = "test_user"
    connection_url = db_container.get_connection_url()
    engine = sqlalchemy.create_engine(connection_url)
    with engine.begin() as cnn:
        query_factory = QueryFactory.get_factory(db_provider)
        cnn.execute(sqlalchemy.text(query_factory.create_role_query(test_role)))
        cnn.execute(
            sqlalchemy.text(
                query_factory.grant_role_query(
                    master=db_container.username, child=test_role
                )
            )
        )
    engine.dispose()
