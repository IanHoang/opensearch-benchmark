# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from abc import ABC, abstractmethod
import random
from enum import Enum
from datetime import datetime, timedelta
import ipaddress

from faker import Faker

fake = Faker()

class BaseGenerator(ABC):
    @abstractmethod
    def generate(self, **kwargs):
        pass

class KeywordGenerator(BaseGenerator):
    def generate(self, **kwargs):
        return fake.word()

class IntegerGenerator(BaseGenerator):
    def generate(self, min_value=0, max_value=100, **kwargs):
        return fake.random_int(min=min_value, max=max_value)

class FloatGenerator(BaseGenerator):
    def generate(self, min_value=0.0, max_value=100.0, **kwargs):
        return fake.pyfloat(min_value=min_value, max_value=max_value)

class DateGenerator(BaseGenerator):
    def generate(self, start_date=None, end_date=None, **kwargs):
        start = datetime.fromisoformat(start_date) if start_date else datetime(2000, 1, 1)
        end = datetime.fromisoformat(end_date) if end_date else datetime.now()
        return fake.date_between_dates(date_start=start, date_end=end).isoformat()

class TimestampGenerator(BaseGenerator):
    def generate(self, **kwargs):
        return fake.iso8601()

class IPAddressGenerator(BaseGenerator):
    def generate(self, **kwargs):
        return fake.ipv4()

class StatusCodeGenerator(BaseGenerator):
    def generate(self, **kwargs):
        return fake.random_element(elements=(200, 201, 204, 400, 401, 403, 404, 500))

class CurrencyGenerator(BaseGenerator):
    def generate(self, min_value=0, max_value=1000, currency="USD", **kwargs):
        amount = fake.pyfloat(min_value=min_value, max_value=max_value, right_digits=2)
        return f"{amount:.2f} {currency}"