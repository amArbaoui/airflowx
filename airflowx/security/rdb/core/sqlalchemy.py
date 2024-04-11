import sqlalchemy
from sqlalchemy.event import listen
from sqlalchemy.engine import Engine


class ProxyEngineWrapper(object):
    def __init__(self, engine: Engine, as_role: str):
        self._engine = engine
        self.role = as_role
        self._register_listener()

    @property
    def engine(self):
        return self._engine

    @classmethod
    def role_aware_engine_from_url(cls, url: str, as_role: str) -> Engine:
        engine = sqlalchemy.create_engine(url)
        return ProxyEngineWrapper(engine, as_role).engine

    def _register_listener(self):
        listen(self.engine, "connect", self._as_role, retval=True)
        listen(self.engine, "checkout", self._as_role, retval=True)

    def _as_role(self, *args):
        dbapi_connection = args[0]
        statement = f"set role {self.role};"
        with dbapi_connection.cursor() as cursor:
            cursor.execute(statement)
