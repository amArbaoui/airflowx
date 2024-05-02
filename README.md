## About The Project
This repository is a collection of sample utils to use with airflow.

### Requirements
Python 3.9+  
Airflow 2.6+
### Features  
#### Proxy database user
Addresses authorization issues in multi-user Airflow environments by allowing multiple users to access the database through a single user (e.g airflowuser)
and execute queries via specific child roles with limited set of access rights.
##### Usage
Proxy sqlalchemy engine
  ```python 
from airflowx.security import ProxyUserEngineWrapper
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="some-conn-id")
db_uri = hook.get_uri()
engine = ProxyUserEngineWrapper.proxy_user_engine_from_url(url=db_uri, role="some-role")
  ```
Proxy postgres operator
  ```python 
from airflowx.security import ProxyUserPostgresOperator
task_postgres_op = ProxyUserPostgresOperator(task_id="some-postgres-task",
                                               role="some-user",
                                               sql="select 1")
  ```