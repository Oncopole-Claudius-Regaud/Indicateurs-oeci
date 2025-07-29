import pyodbc
import cx_Oracle
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable


def connect_to_iris():
    """
    Connexion à IRIS via ODBC en utilisant la connexion Airflow.
    """
    conn = BaseHook.get_connection("iris_odbc")  # Conn ID défini dans Airflow
    dsn = conn.host  # ou conn.extra_dejson.get("dsn") si tu le stockes dans Extra

    connection = pyodbc.connect(
        f"DSN={dsn};UID={conn.login};PWD={conn.password}"
    )
    connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    connection.setencoding(encoding='utf-8')

    return connection


# Connexion Oracle via cx_Oracle
def connect_to_oracle():
    conn = BaseHook.get_connection("oracle_conn")
    lib_dir = conn.extra_dejson.get("lib_dir", "/opt/oracle/instantclient_23_7")

    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except cx_Oracle.ProgrammingError:
        pass  # Already initialized

    return cx_Oracle.connect(
        conn.login,
        conn.password,
        conn.host,
        encoding='UTF-8'
    )


def get_postgres_hook(conn_id=None):
    if not conn_id:
        conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    return PostgresHook(postgres_conn_id=conn_id)
