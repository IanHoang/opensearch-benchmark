# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from unittest import TestCase
from unittest.mock import Mock, patch

from osbenchmark.create_workload.config import CustomWorkload
from osbenchmark.create_workload.extractors import IndexExtractor

class TestCreateWorkloadHelpers(TestCase):

    def setup(self):
        self.mock_custom_workload = Mock(spec=CustomWorkload())


