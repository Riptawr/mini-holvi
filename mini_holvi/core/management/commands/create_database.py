from typing import Dict

from django.conf import settings
from django.core.management.base import BaseCommand
from mini_holvi_etl.wrapper_postgres import bootstrap_dwh, DSN


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        # TODO: probably easier way would be to duplicate/replace the django NAME key
        # with my database field or update my DSN to be like Django's
        django_dsn = {k.lower(): v for k, v in settings.DATABASES.get("default").items()}
        merged_fields = {k: v for k, v in django_dsn.items() if k in DSN()._asdict().keys()}
        self.create_database(merged_fields, django_dsn.get("name"))

    @staticmethod
    def create_database(merged_fields: Dict[str, str], target: str) -> None:
        bootstrap_dwh(dsn=DSN(**merged_fields, database="postgres"), target_db=target, drop_if_exists=True)

