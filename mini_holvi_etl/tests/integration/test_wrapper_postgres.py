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
        d = DSN()
        res = create_event_notify_func(d, channel_name="integrationtest")
        self.assertTrue(res)
