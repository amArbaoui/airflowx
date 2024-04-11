import pytest
import sqlalchemy.engine
from testcontainers.postgres import PostgresContainer


@pytest.fixture
def get_postgres_container() -> PostgresContainer:
    with PostgresContainer("postgres:16") as postgres:
        connection_url = postgres.get_connection_url()
        engine = sqlalchemy.create_engine(connection_url)
        with engine.begin() as cnn:
            test_role = "test_user"
            cnn.execute(sqlalchemy.text(f"create role {test_role} nosuperuser;"))
            cnn.execute(sqlalchemy.text(f"grant {test_role} to {postgres.username};"))
            print(cnn.execute(sqlalchemy.text("select current_user;")).one()[0])
        engine.dispose()
        yield postgres
