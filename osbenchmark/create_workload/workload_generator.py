# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import logging
import os

from osbenchmark import PROGRAM_NAME, exceptions
from osbenchmark.client import OsClientFactory
from osbenchmark.create_workload.config import CustomWorkload
from osbenchmark.create_workload.helpers import QueryProcessor, CustomWorkloadWriter, process_indices, validate_index_documents_map
from osbenchmark.create_workload.extractors import IndexExtractor, SequentialCorpusExtractor
from osbenchmark.utils import io, opts, console

class WorkloadGenerator:
    def __init__(self, cfg):
        self.logger = logging.getLogger(__name__)

        self.indices: list = cfg.opts("generator", "indices")
        self.target_hosts: opts.TargetHosts = cfg.opts("client", "hosts")
        self.client_options: opts.ClientOptions = cfg.opts("client", "options")
        # self.document_frequency: int = cfg.opts("generator", "document_frequency") # Enable later
        self.document_frequency: int = 0
        self.number_of_docs: dict = cfg.opts("generator", "number_of_docs")
        self.unprocessed_queries: dict = cfg.opts("workload", "custom_queries")
        self.templates_path: str = os.path.join(cfg.opts("node", "benchmark.root"), "resources")

        validate_index_documents_map(self.indices, self.number_of_docs)

        self.processed_indices = process_indices(self.indices, self.document_frequency, self.number_of_docs)
        self.custom_workload = CustomWorkload(
            workload_name=cfg.opts("workload", "workload.name"),
            output_path=cfg.opts("generator", "output.path"),
            indices=self.processed_indices,
        )
        self.custom_workload.workload_path = os.path.abspath(os.path.join(io.normalize_path(cfg.opts("generator", "output.path")), cfg.opts("workload", "workload.name")))
        self.custom_workload.operations_path = os.path.join(self.custom_workload.workload_path, "operations")
        self.custom_workload.test_procedures_path = os.path.join(self.custom_workload.workload_path, "test_procedures")

        client = OsClientFactory(hosts=self.target_hosts.all_hosts[opts.TargetHosts.DEFAULT],
                                client_options=self.client_options.all_client_options[opts.TargetHosts.DEFAULT]).create()
        info = client.info()
        console.info(f"Connected to OpenSearch cluster [{info['name']}] version [{info['version']['number']}].\n", logger=self.logger)

        # Initialize Processor, Writer, and Extractors
        self.query_processor = QueryProcessor(self.unprocessed_queries)
        self.custom_workload_writer = CustomWorkloadWriter(self.custom_workload, self.templates_path)
        self.index_extractor = IndexExtractor(self.custom_workload, self.client)
        self.corpus_extractor = SequentialCorpusExtractor(self.custom_workload, self.client)

    def create_workload(self):
        # Process Queries
        processed_queries = self.query_processor.process_queries()
        self.custom_workload.queries = processed_queries
        self.logger.info("Processed custom queries [%s]", self.custom_workload.queries)

        # Create Workload Output Path
        self.custom_workload_writer.make_workload_directory()
        self.logger.info("Created workload output path at [%s]", self.custom_workload.workload_path)

        # Extract Index Settings and Mappings
        self.custom_workload.extracted_indices, self.custom_workload.failed_indices = self.index_extractor.extract_indices(self.custom_workload.workload_path)
        self.logger.info("Extracted index settings and mappings from [%s]", self.custom_workload.indices)

        # Extract Corpora
        for index in self.custom_workload.indices:
            index_corpora = self.corpus_extractor.extract_documents(index.name, index.number_of_docs)
            self.custom_workload.corpora.append(index_corpora)
        self.logger.info("Extracted all corpora [%s]", self.custom_workload.corpora)

        if len(self.custom_workload.corpora) == 0:
            raise exceptions.BenchmarkError("Failed to extract corpora for any indices for workload!")

        template_vars = {
            "workload_name": self.custom_workload.workload_name,
            "indices": self.custom_workload.extracted_indices,
            "corpora": self.custom_workload.corpora,
            "custom_queries": self.custom_workload.queries
        }
        self.logger.info("Template vars [%s]", template_vars)

        # Render all templates
        self.custom_workload_writer.render_templates(template_vars, self.custom_workload.queries)

        console.println("")
        console.info(f"Workload {self.custom_workload.workload_name} has been created. Run it with: {PROGRAM_NAME} --workload-path={self.custom_workload.workload_path}")
