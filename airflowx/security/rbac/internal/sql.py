from abc import ABC, abstractmethod

from airflowx.security.rbac.internal.provider import DbProvider


class AbstractQueryFactory(ABC):
    @staticmethod
    @abstractmethod
    def create_role_query(role: str) -> str:
        pass

    @staticmethod
    @abstractmethod
    def grant_role_query(master: str, child: str) -> str:
        pass

    @staticmethod
    @abstractmethod
    def set_role_query(role: str) -> str:
        pass

    @staticmethod
    @abstractmethod
    def now_query() -> str:
        pass

    @staticmethod
    @abstractmethod
    def current_user_query() -> str:
        pass


class PostgresQueryFactory(AbstractQueryFactory):
    @staticmethod
    def create_role_query(role: str) -> str:
        return f"create role {role} nosuperuser;"

    @staticmethod
    def grant_role_query(master: str, child: str) -> str:
        return f"grant {child} to {master};"

    @staticmethod
    def now_query() -> str:
        return "select now();"

    @staticmethod
    def current_user_query() -> str:
        return "select current_user;"

    @staticmethod
    def set_role_query(role: str) -> str:
        return f"set role {role};"


class MySqlQueryFactory(AbstractQueryFactory):
    @staticmethod
    def create_role_query(role: str) -> str:
        return f"create user '{role}'@'%' identified by 'password';"

    @staticmethod
    def grant_role_query(master: str, child: str) -> str:
        return f"grant '{child}' to '{master}'@'%';"

    @staticmethod
    def now_query() -> str:
        return "select now();"

    @staticmethod
    def current_user_query() -> str:
        return "select current_role();"

    @staticmethod
    def set_role_query(role) -> str:
        return f"set role {role};"


class QueryFactory:
    factory_map = {
        DbProvider.POSTGRES: PostgresQueryFactory,
        DbProvider.MYSQL: MySqlQueryFactory,
    }

    @classmethod
    def get_factory(cls, provider_type: DbProvider):
        return cls.factory_map[provider_type]
