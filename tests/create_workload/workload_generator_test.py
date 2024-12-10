# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from unittest import TestCase
from unittest.mock import Mock, patch

from osbenchmark import config
from osbenchmark.create_workload.config import CustomWorkload
from osbenchmark.create_workload.extractors import IndexExtractor

class TestCreateWorkloadHelpers(TestCase):

    def setup(self):
        custom_queries = {
            "query": {
                "match_all": {}
            }
        }
        self.mock_custom_workload = Mock(spec=CustomWorkload())
        self.config = config.Config()
        self.cfg.add(config.Scope.applicationOverride, "generator", "indices", ["index_1", "index_2"])
        self.cfg.add(config.Scope.applicationOverride, "generator", "number_of_docs", {"index_1": 2000, "index_2": 2000})
        self.cfg.add(config.Scope.applicationOverride, "generator", "output.path", "/home/billyjoel/")
        self.cfg.add(config.Scope.applicationOverride, "workload", "workload.name", "custom_workload")
        self.cfg.add(config.Scope.applicationOverride, "workload", "custom_queries", custom_queries)


