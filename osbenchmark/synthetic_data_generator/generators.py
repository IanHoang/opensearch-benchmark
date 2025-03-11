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

DATE_FORMATS = ['strict_year_month', 'strict_year_month_day']

class BaseGenerator(ABC):
    @abstractmethod
    def generate(self, **kwargs):
        pass

# Numeric Field Types
class IntegerGenerator(BaseGenerator):
    def generate(self, min_value=0, max_value=100, **kwargs):
        return fake.random_int(min=min_value, max=max_value)

class FloatGenerator(BaseGenerator):
    def generate(self, min_value=0.0, max_value=100.0, **kwargs):
        return fake.pyfloat(min_value=min_value, max_value=max_value)

# Boolean Field Types
class BooleanGenerator(BaseGenerator):
    def generate(self, **kwargs):
        options = ["true", "false"]
        return random.choice(options)

# Date Field Types
class DateGenerator(BaseGenerator):
    def generate(self, start_date=None, end_date=None, **kwargs):
        format = kwargs.get('format', False)
        # TODO Support more formats later. Parser should extract format and add to params.
        # Need a dictionary to handle all
        # of these https://opensearch.org/docs/latest/field-types/supported-field-types/date/
        if format:
            # Check if multiple options and see if valid
            formats = format.split("||")
            for format in formats:
                if format not in DATE_FORMATS:
                    raise ValueError(f"Format {format} does not exist in available {DATE_FORMATS}")

            format_to_generate_with = random.choice(formats)

        else:
            # default way to generate dates
            start = datetime.fromisoformat(start_date) if start_date else datetime(2000, 1, 1)
            end = datetime.fromisoformat(end_date) if end_date else datetime.now()
            return fake.date_between_dates(date_start=start, date_end=end).isoformat()

# IP Address Field Types
class IPAddressGenerator(BaseGenerator):
    def generate(self, **kwargs):
        format = kwargs.get('format', None)
        if format and format.lower() == 'ipv6':
            return fake.ipv6()
        else:
            return fake.ipv4()

# Range Field Types
class IntegerRangeGenerator(BaseGenerator):
    def generate(self, **kwargs):
        greater_than_equal_to_number = fake.random_int()
        less_than_equal_to_number = fake.random_int(max=greater_than_equal_to_number)

        return {
            'gte': greater_than_equal_to_number,
            'lte': less_than_equal_to_number
        }

class DoubleRangeGenerator(BaseGenerator):
    def generate(self, **kwargs):
        greater_than_equal_to_number = fake.random_double()
        less_than_equal_to_number = fake.random_double(max=greater_than_equal_to_number)

        return {
            'gte': greater_than_equal_to_number,
            'lte': less_than_equal_to_number
        }

class FloatRangeGenerator(BaseGenerator):
    def generate(self, **kwargs):
        min_value = kwargs.get('min_value', 0)
        max_value = kwargs.get('max_value', 1000)
        greater_than_equal_to_float = fake.pyfloat(min_value=min_value, max_value=max_value, right_digits=2)
        less_than_equal_to_float = fake.pyfloat(min_value, max_value=greater_than_equal_to_float, right_digits=2)

        return {
            'gte': greater_than_equal_to_float,
            'lte': less_than_equal_to_float
        }

class IPRangeGenerator(BaseGenerator):
    def generate(self, **kwargs):
        ip_address_generator = IPAddressGenerator()

        if kwargs.get('use_cidr', False):
            return ip_address_generator.generate(**kwargs) + '/24'

        def get_smaller_ip_address(ip_address):
            int_ip_address = int(ipaddress.ip_address(ip_address))
            smaller_int_ip_address = max(0, int_ip_address - random.randint(1, 1000))
            return str(ipaddress.ip_address(smaller_int_ip_address))

        greater_than_or_equal_to_ip_address = ip_address_generator.generate(**kwargs)
        less_than_or_equal_to_ip_address = get_smaller_ip_address(greater_than_or_equal_to_ip_address)

        return {
            'gte': greater_than_or_equal_to_ip_address,
            'lte': less_than_or_equal_to_ip_address
        }

class DateRangeGenerator(BaseGenerator):
    # TODO: TEST THIS
    def generate(self, **kwargs):
        date_generator = DateGenerator()

        format = kwargs.get('format', False)

        # Check if multiple options and see if valid
        formats = format.split("||")
        for format in formats:
            if format not in DATE_FORMATS:
                raise ValueError(f"Format {format} does not exist in available {DATE_FORMATS}")

        format_to_generate_with = random.choice(formats)

        gte_date = date_generator.generate(**kwargs)
        lte_date = date_generator.generate(end_date=gte_date)

        return {
            'gte': gte_date,
            'lte': lte_date
        }

# Complex / Object Field Types
class NestedGenerator(BaseGenerator):
    def generate(self, num_of_objs=random.randint(1,5), **kwargs):
        """
        Generates a nested array of JSON objects. This generator is used for Nested field types.
        Nested fields are a special type of Object field and
        the value for nested fields area always an array of JSON objects.
        This is why it leverages the ObjectGenerator().

        :param num_of_objs: generates a random number,
        which will be used to determine the number of objects in the array
        :param kwargs: usually contains 'field' which will be provided to ObjectGenerator()

        :return: a list of generated dictionaries (or JSON objects)
        """
        obj_generator = ObjectGenerator()
        nested_objs = []
        for _ in range(num_of_objs):
            generated_obj = obj_generator.generate(**kwargs)
            nested_objs.append(generated_obj)

        return nested_objs

class ObjectGenerator(BaseGenerator):
    def generate(self, **kwargs):
        """
        Generates a dictionary of fields and generated values.
        This is used for Nested and Object mapping field types.

        :param kwargs: usually 'fields' will be included in the kwargs.
        'fields' is needed to cycle through all fields and use their
        respective data generators to generate values.

        :return: a dictionary, which represents a JSON object
        """
        fields = kwargs.get('fields', {})
        generated_obj = {}
        for field, dg_tuple in fields.items():
            data_generator, params = dg_tuple

            generated_obj[field] = data_generator.generate(**params)
        return generated_obj

# String Field Types
class KeywordGenerator(BaseGenerator):
    def generate(self, **kwargs):
        return fake.word()

# Other Use-Cases
class TimestampGenerator(BaseGenerator):
    def generate(self, **kwargs):
        return fake.iso8601()

class StatusCodeGenerator(BaseGenerator):
    def generate(self, **kwargs):
        return fake.random_element(elements=(200, 201, 204, 400, 401, 403, 404, 500))

class CurrencyGenerator(BaseGenerator):
    def generate(self, min_value=0, max_value=1000, currency="USD", **kwargs):
        amount = fake.pyfloat(min_value=min_value, max_value=max_value, right_digits=2)
        return f"{amount:.2f} {currency}"

class RandomChoice(BaseGenerator):
    def generate(self, **kwargs):
        elements = kwargs.get('elements', None)
        if elements:
            return fake.random_element(elements=elements)
        else:
            raise ValueError('No elements provided for Random Choice Generator')