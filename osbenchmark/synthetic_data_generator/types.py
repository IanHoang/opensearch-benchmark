# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import argparse
from dataclasses import dataclass, field
from typing import List, Optional

from osbenchmark.utils import io, opts, console
from osbenchmark.synthetic_data_generator.generators import *


DEFAULT_MAX_CHUNK_SIZE=40_000_000_000
DEFAULT_BATCH_SIZE=1000
DEFAULT_NESTED_NUM_OF_OBJS = {"data_generator_type":"INTEGER","params":{"min_value": 1,"max_value": 10}}

TYPE_MAPPING = {
    'text': 'KEYWORD',
    'keyword': 'KEYWORD',
    'long': 'INTEGER',
    'integer': 'INTEGER',
    'short': 'INTEGER',
    'byte': 'INTEGER',
    'double': 'FLOAT',
    'float': 'FLOAT',
    'half_float': 'FLOAT',
    'scaled_float': 'FLOAT',
    'date': 'DATE',
    'boolean': 'BOOLEAN',
    'nested': 'NESTED',
    'object': 'OBJECT',
    'binary': 'BINARY',
    'ip': 'IP_ADDRESS',
}

class Modes(Enum):
    STANDARD = "standard"
    ORDERED = "ordered"
    FAST = "fast"

class GeneratorTypes(Enum):
    KEYWORD = KeywordGenerator()
    INTEGER = IntegerGenerator()
    FLOAT = FloatGenerator()
    DATE = DateGenerator()
    NESTED = NestedGenerator()
    OBJECT = ObjectGenerator()
    TIMESTAMP = TimestampGenerator()
    IP_ADDRESS = IPAddressGenerator()
    # Other use-cases
    STATUS_CODE = StatusCodeGenerator()
    CURRENCY = CurrencyGenerator()

    def generate(self, **kwargs):
        return self.value.generate(**kwargs)

@dataclass
class SyntheticDataGeneratorConfig:
    index_name: Optional[str] = None
    index_mappings_path: Optional[str] = None
    template_document_path: Optional[str] = None
    total_documents: Optional[str] = None
    total_size_gb: Optional[int] = None
    mode: Optional[str] = None
    output_path: Optional[str] = None
    checkpoint: Optional[str] = None
    generators: dict = field(default_factory=dict)
    blueprint: dict = None


