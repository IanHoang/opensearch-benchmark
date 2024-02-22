# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import logging
import os
import json
import shutil
import bz2
from abc import ABC, abstractmethod

from opensearchpy import OpenSearchException
from jinja2 import Environment, FileSystemLoader, select_autoescape

from osbenchmark import PROGRAM_NAME, exceptions
from osbenchmark.client import OsClientFactory
from osbenchmark.workload_generator import corpus, index
from osbenchmark.utils import io, opts, console
from osbenchmark.config import Config

class CustomQueryBuilder:
    def __init__(self, queries):
        self.queries = queries

    def validate_queries_format(self):
        if not self.queries:
            return []

        with self.queries as queries:
            try:
                data = json.load(queries)
                if isinstance(data, dict):
                    data = [data]
            except ValueError as err:
                raise exceptions.SystemSetupError(f"Ensure JSON schema is valid and queries are contained in a list: {err}")

        return data

class TemplateProcessor:
    BASE_WORKLOAD_TEMPLATE_FILE = "base-workload"
    CUSTOM_OPERATIONS_TEMPLATE_FILE = "custom-operations"
    CUSTOM_SCENARIOS_TEMPLATE_FILE = "custom-scenarios"
    DEFAULT_OPERATIONS_TEMPLATE_FILE = "default-operations"
    DEFAULT_SCENARIOS_TEMPLATE_FILE = "default-scenarios"

    def __init__(self, output_path, cfg):
        self.output_path = output_path
        self.templates_path = os.path.join(cfg.opts("node", "benchmark.root"), "resources")

    def write_template(self, template_vars, templates_path, output_path, template_file):
        template = self.get_template(template_file, templates_path)
        with open(output_path, "w") as f:
            f.write(template.render(template_vars))

    def get_template(self, template_file, templates_path):
        template_file_name = template_file  + ".json.j2"

        env = Environment(loader=FileSystemLoader(templates_path), autoescape=select_autoescape(['html', 'xml']))

        return env.get_template(template_file_name)

    def render_templates(self, workload_path,
                        operations_path,
                        scenarios_path,
                        templates_path,
                        template_vars,
                        custom_queries):

        self.write_template(template_vars, templates_path, workload_path, "base-workload")

        if custom_queries:
            self.write_template(template_vars, templates_path, operations_path, "custom-operations")
            self.write_template(template_vars, templates_path, scenarios_path, "custom-scenarios")
        else:
            self.write_template(template_vars, templates_path, operations_path, "default-operations")
            self.write_template(template_vars, templates_path, scenarios_path, "default-scenarios")

class CustomWorkload:
    def __init__(
            self,
            cfg: Config,
            os_client: OsClientFactory,
            template_processor: TemplateProcessor,
            custom_query_builder: CustomQueryBuilder
        ):
        self.logger = logging.getLogger(__name__)
        # configurations set by user
        self.workload_name = cfg.opts("workload", "workload.name")
        self.indices = cfg.opts("generator", "indices")
        self.number_of_docs = cfg.opts("generator", "number_of_docs")
        self.output_path = cfg.opts("generator", "output.path")
        self.full_workload_path = os.path.abspath(os.path.join(io.normalize_path(self.output_path), self.workload_name))
        self.operations_path = os.path.join(self.full_workload_path, "operations")
        self.scenarios_path = os.path.join(self.full_workload_path, "scenarios")

        # Dependency injection
        self.os_client = os_client
        self.template_processor = template_processor
        self.custom_query_builder = custom_query_builder

        self.indices, self.corpora = [], []
        self.template_variables = {}
        self.retry_files = []

    def create_workload(self):
        self.make_workload_directory()
        self.custom_query_builder.validate_queries_format()

    def make_workload_directory(self):
        if os.path.exists(self.full_workload_path):
            try:
                self.logger.info("Workload already exists. Removing existing workload [%s] in path [%s]", self.workload_name, self.full_workload_path)
                shutil.rmtree(self.full_workload_path)
            except OSError:
                self.logger.error("Had issues removing existing workload [%s] in path [%s]", self.workload_name, self.full_workload_path)

        io.ensure_dir(self.full_workload_path)
        self.logger.info("Made workload directory [%s]", self.full_workload_path)
        io.ensure_dir(self.operations_path)
        self.logger.info("Created operations directory [%s]", self.operations_path)
        io.ensure_dir(self.scenarios_path)
        self.logger.info("Created test procedures directory [%s]", self.scenarios_path)

    def write_workload_json(self):
        pass


class IndexRetriever:
    def __init__(self):
        self.indices = []

    def update_indices(self, indices):
        self.indices = indices

    def extract_indices_mappings(self):
        pass


class CorpusRetriever(ABC):
    """Basic representation of a corpus retriever."""

    @abstractmethod
    def sequential_extraction(self):
        """Extracts corpus documents sequentially."""

    @abstractmethod
    def concurrent_extraction(self):
        """Extracts corpus documents concurrently."""

class PythonCorpusRetriever(CorpusRetriever):
    DEFAULT_DOC_COMPRESSOR = bz2.BZ2Compressor
    DEFAULT_DOC_EXT = ".bz2"

    def __init__(self, doc_compressor):
        self.doc_compressor = CorpusRetriever.DEFAULT_DOC_COMPRESSOR if doc_compressor is None else doc_compressor # Can be ZSTD and others. Default can be ZSTD for faster compression
        self.corpora = []

    def sequential_extraction(self, client):
        pass

    def concurrent_extraction(self, client):
        pass