import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.event import listen

from airflowx.security.rbac.internal.provider import DbProvider
from airflowx.security.rbac.internal.sql import QueryFactory


class ProxyUserEngineWrapper(object):
    def __init__(self, engine: Engine, role: str):
        self._engine = engine
        self.role = role
        self.provider = DbProvider(engine.name)
        self.query_factory = QueryFactory.get_factory(self.provider)
        self._register_listener()

    @property
    def engine(self):
        return self._engine

    @classmethod
    def proxy_user_engine_from_url(cls, url: str, role: str) -> Engine:
        engine = sqlalchemy.create_engine(url)
        return ProxyUserEngineWrapper(engine, role).engine

    def _register_listener(self):
        listen(self.engine, "connect", self._as_role, retval=True)
        listen(self.engine, "checkout", self._as_role, retval=True)

    def _as_role(self, *args):
        dbapi_connection = args[0]
        role_statement = self.query_factory.set_role_query(role=self.role)
        with dbapi_connection.cursor() as cursor:
            cursor.execute(role_statement)
