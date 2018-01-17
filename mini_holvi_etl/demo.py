from typing import List, Any, Type
from uuid import UUID

from funcy import re_find
from sqlalchemy import Table

from wrapper_postgres import *
import model
from model import *

if __name__ == '__main__':

    def bootstrap_source(tables_of_interest: List[str], dsn: DSN=DSN()) -> List[Any]:
        create_event_notify_func(dsn, channel_name="test")
        return [apply_trigger_for_table(dsn, table) for table in tables_of_interest]


    def bootstrap_target(dsn: DSN = DSN()):
        print(f"{datetime.utcnow()} creating target db...")
        bootstrap_dwh(dsn, target_db="targetdb", drop_if_exists=True)

        facts_comp = ("CREATE TABLE facts_company\n"
                      "(inserted_at timestamptz, creator int, creator_tracking_uuid uuid, trade_name text, verified bool, domicile text);")

        facts_user = ("CREATE TABLE facts_user (inserted_at timestamptz, tracking_uuid uuid, mobile_verified bool, email_verified bool,\n"
                      " identity_verified bool, invited bool, country text);")

        facts_rev = ("CREATE TABLE facts_revenue\n"
                     "(inserted_at timestamptz, account int, company int, feature text,\n"
                     " timestamp_paid timestamptz, amount numeric,\n"
                     " account_domicile text, account_tracking_uuid uuid);")

        facts_acc = ("CREATE TABLE facts_account\n"
                     "(inserted_at timestamptz, creator int, company int, tracking_uuid uuid,\n"
                     " creator_tracking_uuid uuid, handle text, archived bool, domicile text);")

        d_target = DSN(database="targetdb")

        tables = [x for x in locals() if x.startswith("facts_")]
        print(f"{datetime.utcnow()} creating fact tables {tables}")
        for table in tables:
            res = create_table(d_target, locals()[table])
            res.close()

    def get_etl_query_for(table: str) -> Union[AssertionError, Type[EtlQuery]]:
        available_queries = model.EtlQuery.__subclasses__()
        for t in available_queries:
            if t.__name__ == table:
                return t

        raise AssertionError(f"No ETL available for table {table}")

    def domain_query_retrieve(dsn: DSN, etl_query: EtlQuery) -> ResultProxy:
        return get_engine(dsn).connect().execute(etl_query.dml())


    def domain_query_insert(items: List[sqlalchemy.engine.result.RowProxy], dsn: DSN, table_name) -> ResultProxy:
        engine = get_engine(dsn)
        metadata = MetaData(engine)
        table = Table(table_name, metadata, autoload=True)

        with engine.connect() as conn:
            for item in items:
                fixed = [datetime.utcnow()]
                for v in item:
                    if type(v) == UUID:
                        # Apparently a sqlalchemy 1.2 bug ?
                        fixed.append(v.hex)
                    else:
                        fixed.append(v)
                return conn.execute(table.insert(fixed))

    def retrieve_and_etl(event: str) -> None:
        target_table = re_find('"table" : "(.+?)"', event)
        try:
            query = get_etl_query_for(target_table)
            print(f"{datetime.utcnow()} parsed table name from event {target_table}. Will retrieve via {query}")
            items = domain_query_retrieve(DSN(database="sourcedb"), query())
            if items:
                facts_table_for_insert = target_table.replace("core", "facts")
                inserts = domain_query_insert(items.fetchall(), DSN(database="targetdb"), table_name=facts_table_for_insert)
                print(f"{datetime.utcnow()} inserted #{inserts.rowcount} rows into {facts_table_for_insert}")

        except AssertionError as ae:
            print(ae)

    def listen_and_etl(channel_name: str, dsn: DSN=DSN()) -> None:
        """ This will block """
        subscribe_to_events_and_dispatch(retrieve_and_etl, dsn, channel_name)

    source_list = ["core_user", "core_account", "core_revenue", "core_company"]
    try:
        bootstrap_source(source_list, dsn=DSN(database="sourcedb"))
        bootstrap_target(DSN())
    except KnownException as ke:
        print(f"{datetime.utcnow()} Warning: skipped errors {ke}")

    listen_and_etl(channel_name="test", dsn=DSN(database="sourcedb"))

