from django.core.management.base import BaseCommand
from mini_holvi.core.models import User, Company, Account, Revenue
from mini_holvi.core.constants import FEE_FEATURE_CHOICES
import random
import uuid
from decimal import Decimal


COUNTRIES = ['FI', 'DE', 'AT']
FIRST_NAMES = ['John', 'Mary', 'Kevin', 'Barbara']
LAST_NAMES = ['Smith', 'White', 'Miller', 'Brasco']


class Command(BaseCommand):
    def generate_users(self):
        for _ in range(10):
            User.objects.create(
                username=uuid.uuid4(),
                country=random.choice(COUNTRIES),
                first_name=random.choice(FIRST_NAMES),
                last_name=random.choice(LAST_NAMES)
            )

    def generate_companies(self):
        users = list(User.objects.all())
        for i in range(10):
            Company.objects.create(
                creator=random.choice(users),
                trade_name='Test_{}'.format(i),
                domicile=random.choice(COUNTRIES)
            )

    def generate_accounts(self):
        companies = list(Company.objects.all())
        for _ in range(10):
            company = random.choice(companies)
            Account.objects.create(
                company=company,
                creator=company.creator,
                handle=str(uuid.uuid4()),
                domicile=random.choice(COUNTRIES)
            )

    def generate_revenue(self):
        from django.utils import timezone
        features = [name for name, _ in FEE_FEATURE_CHOICES]
        accounts = list(Account.objects.all())
        for _ in range(10):
            Revenue.objects.create(
                account=random.choice(accounts),
                feature=random.choice(features),
                amount=Decimal(random.randrange(5, 50)),
                timestamp_paid=timezone.now()
            )

    def handle(self, *args, **kwargs):
        User.objects.all().delete()
        Company.objects.all().delete()
        Account.objects.all().delete()
        Revenue.objects.all().delete()

        self.generate_users()
        self.generate_companies()
        self.generate_accounts()
        self.generate_revenue()
