from datetime import datetime, timedelta
from typing import Union

import pandas as pd
import psycopg2
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor

from config import db_connection
from .tdm_logging import logger, log_error, class_method_name

ENV = 'dev'


class PGSQL:
    class_str = 'PGSQL'
    def __init__(self, **params):
        super().__init__(**params)
        self.conn_str = db_connection(ENV)

    def connect_url(self, db) -> URL:
        class_method = class_method_name()
        try:
            host = self.conn_str['host']
            uid = self.conn_str['uid']
            pwd = self.conn_str['pwd']
            port = self.conn_str['port']
            db = self.conn_str['db']
            url_obj = URL.create(
                "postgresql",
                username=uid,
                password=pwd,
                host=host,
                port=port,
                database=db,
            )
            return url_obj
        except (KeyError, ValueError, TypeError) as err:
            log_error(class_method, type(err).__name__, str(err))
            raise

    def sql_to_df(self, query, params=None, db='portal1', mod=None) -> Union[pd.Series, pd.DataFrame]:
        class_method = class_method_name() if mod is None else mod
        db = db if db else self.conn_str['db']
        logger.info(f'| {class_method} | Query on database "{db}": "{query}"')
        logger.info(f'| {class_method} | Params: "{params}"')
        start_time = datetime.now()

        try:
            engine = create_engine(self.connect_url(db), echo=False, echo_pool='debug', pool_size=1, max_overflow=5)
            with engine.begin() as conn:
                if params:
                    df = pd.read_sql(text(query), conn, params=params)
                else:
                    df = pd.read_sql_query(text(query), conn)

            # Log trace
            end_time = datetime.now()
            logger.info(f"| {class_method} | Executed: {str(end_time - start_time)}")
            return df
        except SQLAlchemyError as err:
            log_error(class_method, type(err).__name__, str(err))
            return pd.DataFrame()
        except Exception as ex:
            log_error(class_method, 'Exception', str(ex))
            return pd.DataFrame()

    def execute_concurrent_queries(self, query1, param1, query2, param2, query3, param3,
                                   db='portal1') -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        class_method = class_method_name()
        try:
            logger.info(f'| {class_method} | Starting concurrent query execution')
            total_start_time = datetime.now()

            # Create a database engine
            engine = create_engine(self.connect_url(db), echo=False, echo_pool='debug', pool_size=1, max_overflow=5)

            # Function to execute query
            def execute_query(query, params=None):
                logger.info(f'| {self.class_str} | Query: "{query}"')
                logger.info(f'| {self.class_str} | Params: "{params}"')
                try:
                    start_time = datetime.now()
                    with engine.connect() as connection:
                        if params:
                            result = connection.execute(text(query), params)
                        else:
                            result = connection.execute(text(query))
                        rows = result.fetchall()
                        columns = result.keys()

                        end_time = datetime.now()
                        logger.info(f"| {class_method} | Executed: {str(end_time - start_time)}")
                        return pd.DataFrame(rows, columns=columns)
                except (SQLAlchemyError, psycopg2.OperationalError) as err:
                    log_error(class_method, type(err).__name__, str(err))
                    return pd.DataFrame()
                except Exception as e:
                    log_error(class_method, 'Exception', str(e))
                    return pd.DataFrame()

            # Using ThreadPoolExecutor to execute queries concurrently
            with ThreadPoolExecutor(max_workers=5) as executor:
                future1 = executor.submit(execute_query, query1, param1)
                future2 = executor.submit(execute_query, query2, param2)
                future3 = executor.submit(execute_query, query3, param3)

                results1 = future1.result()
                results2 = future2.result()
                results3 = future3.result()

            total_end_time = datetime.now()
            logger.info(f'| {class_method} | Concurrent queries executed: {str(total_end_time - total_start_time)}')
            return results1, results2, results3
        except Exception as e:
            log_error(class_method, 'Exception', str(e))
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


class MSSQL:
    str_class = 'MSSQL'
    def __init__(self, **params):
        super().__init__(**params)
        self._sql = db_connection(ENV)

    def connect_url(self, db) -> URL:
        try:
            _host = self._sql['host']
            _uid = self._sql['uid']
            _pwd = self._sql['pwd']
            connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={_host};DATABASE={db};UID={_uid};PWD={_pwd}"
            return URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        except KeyError as key_err:
            log_error(self.str_class, 'KeyError', str(key_err))
            raise

    def sql_to_df(self, query, params=None, db='TDM') -> Union[pd.Series, pd.DataFrame]:
        logger.info(f'| {self.str_class} | Query: "{query}"')
        logger.info(f'| {self.str_class} | Params: "{params}"')
        start_time = datetime.now()

        try:
            engine = create_engine(self.connect_url(db))
            with engine.begin() as conn:
                if params:
                    df = pd.read_sql_query(text(query), conn, params=params)
                else:
                    df = pd.read_sql_query(query, conn)

            end_time = datetime.now()
            logger.info(f"| {str(self.str_class)} | Executed: {str(end_time - start_time)}")
            return df
        except SQLAlchemyError as sql_err:
            log_error(f'{self.str_class}', 'SQLAlchemyError', str(sql_err))
            return pd.DataFrame()
        except Exception as ex:
            log_error(f'{self.str_class}', 'Exception', str(ex))
            return pd.DataFrame()

    def execute_concurrent_queries(self, query1, param1, query2, param2, db='TDM') -> tuple[pd.DataFrame, pd.DataFrame]:
        try:
            logger.info(f'| {self.str_class} | Starting concurrent query execution')
            total_start_time = datetime.now()

            # Create a database engine
            engine = create_engine(self.connect_url(db), echo=True)

            # Function to execute query
            def execute_query(query, params):
                logger.info(f'| {self.str_class} | Query: "{query}"')
                logger.info(f'| {self.str_class} | Params: "{params}"')
                start_time = datetime.now()
                try:
                    with engine.connect() as connection:
                        result = connection.execute(text(query), params)
                        rows = result.fetchall()
                        columns = result.keys()
                        end_time = datetime.now()
                        logger.info(f"| {self.str_class} | Executed: {str(end_time - start_time)}")
                        return pd.DataFrame(rows, columns=columns)
                except SQLAlchemyError as sql_err:
                    log_error(f'{self.str_class}', 'SQLAlchemyError', str(sql_err))
                    return pd.DataFrame()
                except psycopg2.OperationalError as pgsql_err:
                    log_error(f'{self.str_class}', 'PGSQLError', str(pgsql_err))
                    return pd.DataFrame()
                except Exception as ex:
                    log_error(f'{self.str_class}', 'Exception', str(ex))
                    return pd.DataFrame()

            # Using ThreadPoolExecutor to execute queries concurrently
            with ThreadPoolExecutor(max_workers=5) as executor:
                future1 = executor.submit(execute_query, query1, param1)
                future2 = executor.submit(execute_query, query2, param2)

                results1 = future1.result()
                results2 = future2.result()

            total_end_time = datetime.now()
            logger.info(f'| {self.str_class} | Concurrent queries executed: {str(total_end_time - total_start_time)}')
            return results1, results2
        except Exception as ex:
            log_error(f'{self.str_class}', 'Exception', str(ex))
            return pd.DataFrame(), pd.DataFrame()
