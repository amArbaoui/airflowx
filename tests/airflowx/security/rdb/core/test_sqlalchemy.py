import sqlalchemy
from airflowx.security.rdb.core.sqlalchemy import ProxyEngineWrapper
from testcontainers.postgres import PostgresContainer


class TestProxyEngineWrapper:
    def test_as_role_postgres(self, get_postgres_container: PostgresContainer):
        postgres = get_postgres_container
        engine = ProxyEngineWrapper.role_aware_engine_from_url(postgres.get_connection_url(), as_role="test_user")
        test_user = "test_user"
        with engine.connect() as cnn:
            cnn.execute(sqlalchemy.text("select now()"))
            assert test_user == self.get_current_user(cnn)
        with engine.connect() as cnn:
            cnn.execute(sqlalchemy.text("select now()"))
            assert test_user == self.get_current_user(cnn)

        with engine.connect() as cnn:
            with cnn.begin():
                cnn.execute(sqlalchemy.text("select now()"))
                assert test_user == self.get_current_user(cnn)
            with cnn.begin():
                cnn.execute(sqlalchemy.text("select now()"))
                assert test_user == self.get_current_user(cnn)

    def get_current_user(self, cnn):
        return cnn.execute(sqlalchemy.text("select current_user;")).one()[0]
