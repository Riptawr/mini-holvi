from django.contrib.auth.models import User as AuthUser
from django.db import models
from django_countries.fields import CountryField
from .constants import FEE_FEATURE_CHOICES
import uuid


class User(AuthUser):
    tracking_uuid = models.UUIDField(unique=True, default=uuid.uuid4)
    mobile_verified = models.BooleanField(default=False)
    email_verified = models.BooleanField(default=False)
    identity_verified = models.BooleanField(default=False)
    invited = models.BooleanField(default=False)
    country = CountryField()


class Company(models.Model):
    creator = models.ForeignKey('User')
    trade_name = models.CharField(max_length=255)
    verified = models.BooleanField(default=False)
    domicile = CountryField()


class Account(models.Model):
    creator = models.ForeignKey('User')
    company = models.ForeignKey('Company')
    tracking_uuid = models.UUIDField(unique=True, default=uuid.uuid4)
    handle = models.CharField(max_length=255, unique=True)
    archived = models.BooleanField(default=False)
    domicile = CountryField()


class Revenue(models.Model):
    account = models.ForeignKey('Account')
    feature = models.CharField(max_length=20, choices=FEE_FEATURE_CHOICES)
    timestamp_paid = models.DateTimeField()
    amount = models.DecimalField(max_digits=12, decimal_places=2)
