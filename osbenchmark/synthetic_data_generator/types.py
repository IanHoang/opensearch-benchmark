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
from enum import Enum

DEFAULT_MAX_CHUNK_SIZE=40_000_000_000
DEFAULT_BATCH_SIZE=1000

@dataclass
class SyntheticDataGeneratorConfig:
    index_name: Optional[str] = None
    index_mappings_path: Optional[str] = None
    custom_module_path: Optional[str] = None
    custom_config_path: Optional[str] = None
    output_path: Optional[str] = None
    total_size_gb: Optional[int] = None
    mode: Optional[str] = None
    checkpoint: Optional[str] = None
    blueprint: dict = None
    generators: dict = field(default_factory=dict)