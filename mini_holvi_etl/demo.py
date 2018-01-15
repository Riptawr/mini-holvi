from typing import List, Optional, Any
from uuid import UUID

from funcy import re_find
from sqlalchemy import Table
from sqlalchemy.engine import RowProxy

from wrapper_postgres import *

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

    def domain_query_retrieve(dsn: DSN, table_name: str, start_id="1") -> Optional[List[RowProxy]]:
        """

        :param table_name:
        :param dsn:
        :param start_id: a unique ID for the row (ideally, PK), id of 1 means full table refresh
        :return:
        """
        core_user = ("SELECT tracking_uuid, mobile_verified, email_verified, identity_verified, invited, country\n"
                     f"FROM core_user cu LEFT JOIN auth_user a ON cu.user_ptr_id = a.id AND a.id >= {start_id};")

        core_company = ("SELECT creator_id AS creator,\n"
                        " u.tracking_uuid AS creator_tracking_uuid,\n"
                        " trade_name,\n"
                        " verified,\n"
                        " domicile\n"
                        "FROM core_company AS company\n"
                        f"LEFT JOIN core_user u ON company.creator_id = u.user_ptr_id AND company.id >= {start_id};")

        core_account = ("SELECT ca.creator_id as creator, ca.company_id as company,\n"
                        " ca.tracking_uuid,\n"
                        " u.tracking_uuid as creator_tracking_uuid,\n"
                        " ca.handle, ca.archived, ca.domicile\n"
                        "FROM core_account ca\n"
                        f"LEFT JOIN core_user u ON ca.creator_id = u.user_ptr_id AND ca.id >= {start_id};")

        core_revenue = ("  SELECT\n"
                        "    cr.account_id AS account,\n"
                        "    a.company_id AS company,\n"
                        "    cr.feature,\n"
                        "    cr.timestamp_paid AS timestamp_paid,\n"
                        "    cr.amount,\n"
                        "    a.domicile AS account_domicile,\n"
                        "    a.tracking_uuid AS account_tracking_uuid\n"
                        f"FROM core_revenue AS cr LEFT JOIN core_account a ON cr.account_id = a.id AND cr.id >= {start_id};")

        scoped_queries = [o for o in locals() if o.startswith("core_")]

        if table_name not in scoped_queries:
            print(f"{datetime.utcnow()} Warning: Source table not available for ETL. Available are: {scoped_queries}")
            return None

        return get_engine(dsn).connect().execute(locals().get(table_name)).fetchall()


    def domain_query_insert(items: List[sqlalchemy.engine.result.RowProxy], dsn: DSN, table_name) -> None:
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
                conn.execute(table.insert(fixed))

    def retrieve_and_etl(event: str) -> None:
        target_table = re_find('"table" : "(.+?)"', event)
        print(f"{datetime.utcnow()} sourcing data from table: {target_table}")
        items = domain_query_retrieve(DSN(database="sourcedb"), start_id="1", table_name=target_table)
        if items:
            domain_query_insert(items, DSN(database="targetdb"), table_name=target_table.replace("core", "facts"))

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

