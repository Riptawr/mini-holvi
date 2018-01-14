from typing import NamedTuple, Union
from datetime import datetime

import select
import sqlalchemy
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.result import ResultProxy
from sqlalchemy.schema import DDL


class DSN(NamedTuple):
    """ Data type representing PostgreSQL DSN """
    database: str = "postgres"
    user: str = "postgres"
    password: str = "secret"
    host: str = "localhost"
    port: int = 5432
    async: bool = False


def get_engine(dsn: DSN) -> sqlalchemy.engine:
    """This creates lazy engine objects and should be thread-safe"""
    return create_engine(f"postgresql://{dsn.user}:{dsn.password}@{dsn.host}:{dsn.port}/{dsn.database}")


def bootstrap_dwh(dsn: DSN, target_db: str, drop_if_exists: bool = False) -> Union[Exception, ResultProxy]:
    initial_engine = get_engine(dsn)
    con = initial_engine.connect()

    con.execute("commit")  # commits the connect, so a DB can be created outside of the transaction
    if drop_if_exists:
        con.execute(f"DROP DATABASE IF EXISTS {target_db}")
        con.execute("commit")
        res = con.execute(f"CREATE DATABASE {target_db}")
        return res

    res = con.execute(f"CREATE DATABASE {target_db}")
    return res


def create_event_notify_func(dsn: DSN, channel_name: str) -> Union[Exception, ResultProxy]:
    # TODO: exception + drop cascade if exists may be more transparent than replace
    notify_trigger = DDL(
        "CREATE OR REPLACE FUNCTION notify_id_trigger() RETURNS trigger AS $$\n"
        "BEGIN\n"
        f"  PERFORM pg_notify('{channel_name}'::text, row_to_json(NEW)::text);\n"
        "  RETURN new;\n"
        "END;\n"
        "$$ LANGUAGE plpgsql;").execute_if(dialect="postgresql")

    con = get_engine(dsn)
    meta = MetaData()
    meta.create_all(con)
    res = con.execute(notify_trigger)

    return res


def subscribe_to_events(dsn: DSN, channel: str = "test") -> None:
    """ Leverages PostgreSQL Listen and Notify commands to generate events """

    engine = get_engine(dsn).execution_options(autocommit=True)
    conn = engine.connect()
    conn.execute(f"LISTEN {channel};")
    print(f"{datetime.utcnow()} Waiting for notifications on channels {channel}")
    while True:
        if select.select([conn.connection], [], [], 5) == ([], [], []):
            print(f"{datetime.utcnow()} Nothing new")
        else:
            conn.connection.poll()
            while conn.connection.notifies:
                notify = conn.connection.notifies.pop()
                print(f"{datetime.utcnow()} Got NOTIFY: {notify.pid}, {notify.channel}, {notify.payload}")
