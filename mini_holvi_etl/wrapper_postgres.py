from typing import NamedTuple, Union, Callable
from datetime import datetime

import select
import sqlalchemy
from funcy import contextmanager
from multiprocessing import Process
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.result import ResultProxy
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import DDL


class PGWrapperException(Exception):
    """ Library generic exception type
    Use this to implement `catch all` behaviour
    """
    def __init__(self, msg, original_exception):
        super(PGWrapperException, self).__init__(f"{msg}: {original_exception}")
        self.original_exception = original_exception


class KnownException(PGWrapperException):
    """ We did anticipate this one and want to handle it """


class UnknownException(PGWrapperException):
    """ All other things that can go wrong go here """


@contextmanager
def expect_errors(*exceptions):
    """ Wrap the specified exceptions into the library specific type
    for easier troubleshooting

    :param exceptions: a list of exception types (or single exception)
    :return:
    """
    try:
        yield
    except exceptions as ex:
        raise KnownException("Library specific error: ", ex)


class DSN(NamedTuple):
    """ Data type representing PostgreSQL DSN """
    database: str = "postgres"
    user: str = "postgres"
    password: str = "secret"
    host: str = "localhost"
    port: int = 5432
    async: bool = False


def get_engine(dsn: DSN) -> sqlalchemy.engine:
    """ This creates lazy engine objects and should be thread-safe
        NullPool is used since we aim for ETL and don't need persistent open connections
    """
    return create_engine(f"postgresql://{dsn.user}:{dsn.password}@{dsn.host}:{dsn.port}/{dsn.database}", poolclass=NullPool)


def bootstrap_dwh(dsn: DSN, target_db: str, drop_if_exists: bool = False) -> Union[Exception, ResultProxy]:
    with expect_errors(ProgrammingError, OperationalError):
        initial_engine = get_engine(dsn)
        con = initial_engine.connect()

        con.execute("commit")  # commits the connect, so a DB can be created outside of the transaction
        if drop_if_exists:
            con.execute(f"DROP DATABASE IF EXISTS {target_db}")
            con.execute("commit")
            return con.execute(f"CREATE DATABASE {target_db}")

        return con.execute(f"CREATE DATABASE {target_db}")


def create_table(dsn: DSN, sql_inline: str) -> Union[Exception, ResultProxy]:
    with expect_errors(ProgrammingError, OperationalError):
        return get_engine(dsn).connect().execute(DDL(sql_inline))


def create_event_notify_func(dsn: DSN, channel_name: str) -> Union[Exception, ResultProxy]:
    # TODO: exception + drop cascade if exists may be more transparent than replace
    notify_trigger = DDL("CREATE OR REPLACE FUNCTION notify_id_trigger() RETURNS TRIGGER AS $$\n"
                      "    DECLARE \n"
                      "        data json;\n"
                      "        notification json;\n"
                      "    BEGIN\n"
                      "        IF (TG_OP = 'DELETE') THEN\n"
                      "            data = row_to_json(OLD);\n"
                      "        ELSE\n"
                      "            data = row_to_json(NEW);\n"
                      "        END IF;\n"
                      "        \n"
                      "        notification = json_build_object(\n"
                      "                          'table',TG_TABLE_NAME,\n"
                      "                          'action', TG_OP,\n"
                      "                          'data', data);\n"
                      "        \n"
                      "                        \n"
                      f"        PERFORM pg_notify('{channel_name}',notification::text);\n"
                      "        \n"
                      "        RETURN NULL; \n"
                      "    END;\n"
                      "    \n"
                      "$$ LANGUAGE plpgsql;")

    with expect_errors(ProgrammingError, OperationalError):
        con = get_engine(dsn)
        meta = MetaData()
        meta.create_all(con)
        return con.execute(notify_trigger)


def apply_trigger_for_table(dsn: DSN, t: str) -> [Exception, ResultProxy]:
    with expect_errors(ProgrammingError, OperationalError):
        conn = get_engine(dsn).execution_options(autocommit=True).connect()
        return conn.execute(f"CREATE TRIGGER data_modified AFTER insert or update on {t} for each row execute "
                            f"procedure notify_id_trigger();")


def subscribe_to_events_and_dispatch(f: Callable[[str], None], dsn: DSN, channel: str = "test") -> [Exception, None]:
    """ Leverages PostgreSQL Listen and Notify commands to generate events """

    with expect_errors(ProgrammingError, OperationalError):

        engine = get_engine(dsn).execution_options(autocommit=True)
        conn = engine.connect()
        conn.execute(f"LISTEN {channel};")
        print(f"{datetime.utcnow()} Waiting for notifications on channels {channel}")
        while True:
            if select.select([conn.connection], [], [], 5) == ([], [], []):
                pass
            else:
                conn.connection.poll()
                while conn.connection.notifies:
                    notify = conn.connection.notifies.pop()
                    print(notify.payload)
                    p = Process(target=f, args=(notify.payload,))
                    p.start()
                    p.join()
