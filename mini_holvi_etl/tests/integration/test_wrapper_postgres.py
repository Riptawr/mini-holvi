from unittest import TestCase
from wrapper_postgres import *


class TestWrapperPostgres(TestCase):
    def test_get_engine(self):
        d = DSN()
        con = get_engine(d)
        res = con.execute("SELECT lanname FROM pg_language")
        self.assertEqual(len([r for r in res]), 4)

    def test_bootstrap_dwh(self):
        d = DSN()
        res = bootstrap_dwh(d, target_db="IntegrationTestDB", drop_if_exists=True)
        self.assertTrue(res)
        self.assertRaises(Exception, bootstrap_dwh, target_db="IntegrationTestDB", drop_if_exists=False)

    def test_create_event_notify_trigger(self):
        test_query = ("SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position\n"
                      "FROM information_schema.routines\n"
                      "    LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name\n"
                      "WHERE routines.specific_schema='public'\n"
                      "ORDER BY routines.routine_name, parameters.ordinal_position;")
        d = DSN()

        res = create_event_notify_func(d, channel_name="integrationtest")
        self.assertTrue(res)

        triggers_in_schema = get_engine(d).execute(test_query).fetchall()
        trigger_name = [row[0] for row in triggers_in_schema if "notify_id_trigger" in row[0]]
        self.assertEqual(["notify_id_trigger"], trigger_name)

    def test_apply_trigger_for_table(self):
        d = DSN(database="sourcedb")
        table = "core_revenue"
        res = apply_trigger_for_table(dsn=d, t=table)
        self.assertTrue(res)
        self.assertRaises(Exception, apply_trigger_for_table, dsn=d, t=table)
