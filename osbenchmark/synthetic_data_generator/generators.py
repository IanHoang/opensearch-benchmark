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

# TEST THIS OUT
class NestedGenerator(BaseGenerator):
    def generate(self, num_of_objs=random.randint(1,5), **kwargs):
        import logging
        logger = logging.getLogger(__name__)
        logger.info("num of objs %s", num_of_objs)

        obj_generator = ObjectGenerator()
        nested_objs = []
        for _ in range(num_of_objs):
            logger.info("Nested Generator: Generating object %s", _)
            logger.info("Nested Generator: kwargs %s", kwargs)
            generated_obj = obj_generator.generate(**kwargs)
            nested_objs.append(generated_obj)
            logger.info("nested objs %s", nested_objs)

        return nested_objs

class ObjectGenerator(BaseGenerator):
    def generate(self, **kwargs):
        import logging
        logger = logging.getLogger(__name__)
        fields = kwargs.get('fields', {})
        logger.info("Object Generator: fields %s", fields)
        generated_obj = {}
        logger.info("items %s", fields.items())
        for field, dg_tuple in fields.items():
            data_generator, params = dg_tuple
            logger.info("Field %s, tuple %s", field, dg_tuple)

            generated_obj[field] = data_generator.generate(**params)
        return generated_obj

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
