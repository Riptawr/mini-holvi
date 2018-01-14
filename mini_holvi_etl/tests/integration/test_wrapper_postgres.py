from unittest import TestCase

import time
from sqlalchemy import create_engine
from wrapper_postgres import *


class TestWrapperPostgres(TestCase):
    """
    TODO: ResultProxy returned by sqlalchemy and similar ORM does not implement a general "check for success" function
    The tests are full of self.assert(res, msg="Pseudocheck failed, ResultProxy was None")
     which exploits a side-effect of failing ResultProxy objects
     being `None` as a surrogate for proper checks in all cases that i've tested (i.e. incompleteness)
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.initial_dsn = DSN(host="localhost", user="postgres", password="secret", database="postgres")
        self.targetDB = "integrationtestdb"
        self.dsn = DSN(host="localhost", user="postgres", password="secret", database=self.targetDB)
        self.sourceDB = "sourcedb"
        self.db_creation_query = f"CREATE DATABASE {self.targetDB};"
        self.test_table = "test_users"
        self.test_table_nonexistent = "test_does_not_exist"

    def setUp(self):
        # """ We assume sqlalchemy is tested elsewhere """
        # engine = create_engine(f"postgresql://{self.initial_dsn.user}:{self.initial_dsn.password}@{self.initial_dsn.host}:{self.initial_dsn.port}/{self.initial_dsn.database}")
        # con = engine.connect()
        # con.execute("commit")
        # con.execute(self.db_creation_query)
        # con.close()
        pass

    def tearDown(self):
        engine = create_engine(f"postgresql://{self.initial_dsn.user}:{self.initial_dsn.password}@{self.initial_dsn.host}:{self.initial_dsn.port}/{self.initial_dsn.database}")
        con = engine.connect()
        con.execute("commit")
        con.execute(f"DROP DATABASE {self.targetDB};")
        con.close()

    def test_monolithic_wrapper(self):

        def test_get_engine(self):
            con = get_engine(self.initial_dsn)
            res = con.execute("SELECT lanname FROM pg_language").fetchall()
            self.assertEqual(len([r for r in res]), 4)

        def test_bootstrap_dwh(self):
            res = bootstrap_dwh(self.initial_dsn, target_db=self.targetDB, drop_if_exists=True)
            self.assertTrue(res)
            time.sleep(0.9)

            self.assertRaises(KnownException, bootstrap_dwh, dsn=self.initial_dsn, target_db=self.targetDB, drop_if_exists=False)

        def test_create_event_notify_func(self):
            test_query = ("SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position\n"
                          "FROM information_schema.routines\n"
                          "    LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name\n"
                          "WHERE routines.specific_schema='public'\n"
                          "ORDER BY routines.routine_name, parameters.ordinal_position;")

            res = create_event_notify_func(self.dsn, channel_name="integrationtest")
            self.assertTrue(res, msg="Pseudocheck failed, ResultProxy was None")

            triggers_in_schema = get_engine(self.dsn).execute(test_query).fetchall()
            trigger_name = [row[0] for row in triggers_in_schema if "notify_id_trigger" in row[0]]
            self.assertEqual(["notify_id_trigger"], trigger_name)

        def test_create_table(self):
            test_query = ("SELECT EXISTS (\n"
                          "   SELECT 1\n"
                          "   FROM   information_schema.tables\n"
                          "   WHERE  table_schema = 'public'\n"
                          f"   AND    table_name = '{self.test_table}'\n"
                          "   );")

            res = create_table(self.dsn, f"CREATE TABLE {self.test_table} (id int, dummy text);")
            self.assertTrue(res)
            table_exists = get_engine(self.dsn).connect().execute(test_query).first()
            self.assertTrue(table_exists)

        def test_apply_trigger_for_table(self):
            test_query = ("SELECT DISTINCT trigger_name, event_object_table\n"
                          "  FROM information_schema.triggers\n"
                          " WHERE trigger_schema NOT IN\n"
                          "       ('pg_catalog', 'information_schema');")

            res = apply_trigger_for_table(dsn=self.dsn, t=self.test_table)
            self.assertTrue(res, msg="Pseudocheck failed, ResultProxy was None")

            triggers_on_tables = get_engine(self.dsn).execute(test_query).fetchall()

            self.assertIn(self.test_table, [row[1] for row in triggers_on_tables],
                          msg=f"could not find any trigger on {self.test_table}")

            self.assertNotIn("other_table", [row[1] for row in triggers_on_tables],
                             msg="found triggers on tables we didn't modify")

            self.assertRaises(KnownException, apply_trigger_for_table,
                              dsn=self.dsn, t=self.test_table)

        # TODO: split via proper setup/teardown and service classes
        test_get_engine(self)
        test_bootstrap_dwh(self)
        test_create_event_notify_func(self)
        test_create_table(self)
        test_apply_trigger_for_table(self)
        time.sleep(3)  # wait for sessions to close
