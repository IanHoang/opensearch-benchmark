# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import json
import os
import logging
import sys
import shutil

from jinja2 import Environment, FileSystemLoader, select_autoescape

from osbenchmark import exceptions
from osbenchmark.utils import io, console
from osbenchmark.workload_generator.config import CustomWorkload, Index


BASE_WORKLOAD = "base-workload"
CUSTOM_OPERATIONS = "custom-operations"
CUSTOM_TEST_PROCEDURES = "custom-test-procedures"
DEFAULT_OPERATIONS = "default-operations"
DEFAULT_TEST_PROCEDURES = "default-test-procedures"
TEMPLATE_EXT = ".json.j2"

class CustomWorkloadWriter:

    def __init__(self, custom_workload: CustomWorkload, templates_path: str):
        self.custom_workload = custom_workload
        self.templates_path = templates_path

        self.custom_workload.workload_path = os.path.abspath(
            os.path.join(io.normalize_path(self.custom_workload.output_path),
                         self.custom_workload.workload_name))
        self.custom_workload.operations_path = os.path.join(self.custom_workload.workload_path, "operations")
        self.custom_workload.test_procedures_path = os.path.join(self.custom_workload.workload_path, "test_procedures")
        self.logger = logging.getLogger(__name__)

    def make_workload_directory(self):
        if not self._has_write_permission(self.custom_workload.workload_path):
            error_suggestion = "Workload output path does not have write permissions. " \
                + "Please update the permissions for the specified output path or choose a different output path."
            self.logger.error(error_suggestion)
            console.error(error_suggestion)

        # Check if a workload of the same name already exists in output path
        if os.path.exists(self.custom_workload.workload_path):
            try:
                input_text = f"A workload already exists at {self.custom_workload.workload_path}. " \
                + "Would you like to remove it? (y/n): "
                user_decision = input(input_text)
                while user_decision not in ('y', 'n'):
                    user_decision = input("Provide y for yes or n for no. " + input_text)

                if user_decision == "y":
                    self.logger.info("Removing existing workload [%s] in path [%s]",
                                    self.custom_workload.workload_name, self.custom_workload.workload_path)
                    console.info("Removing workload of the same name.")
                    shutil.rmtree(self.custom_workload.workload_path)
                elif user_decision == "n":
                    logging_info = "Keeping workload of the same name at existing path. Cancelling create-workload."
                    self.logger.info(logging_info)
                    console.println("")
                    console.info(logging_info)
                    sys.exit(0)

            except OSError:
                self.logger.error("Had issues removing existing workload [%s] in path [%s]",
                                  self.custom_workload.workload_name, self.custom_workload.workload_path)

        io.ensure_dir(self.custom_workload.workload_path)
        io.ensure_dir(self.custom_workload.operations_path)
        io.ensure_dir(self.custom_workload.test_procedures_path)

    def _has_write_permission(self, directory):
        """
        Verify if output directory for workload has write permissions
        """
        return os.access(directory, os.W_OK)

    def render_templates(self, template_vars: dict, custom_queries: dict):
        workload_file_path = os.path.join(self.custom_workload.workload_path, "workload.json")
        operations_file_path = os.path.join(self.custom_workload.operations_path, "default.json")
        test_procedures_file_path = os.path.join(self.custom_workload.test_procedures_path, "default.json")

        self._write_template(template_vars, BASE_WORKLOAD, workload_file_path)

        if custom_queries:
            self._write_template(template_vars, CUSTOM_OPERATIONS, operations_file_path)
            self._write_template(template_vars, CUSTOM_TEST_PROCEDURES, test_procedures_file_path)
        else:
            self._write_template(template_vars, DEFAULT_OPERATIONS, operations_file_path)
            self._write_template(template_vars, DEFAULT_TEST_PROCEDURES, test_procedures_file_path)

    def _write_template(self, template_vars: dict, template_file: str, output_path: str):
        template = self._get_default_template(template_file)
        with open(output_path, "w") as f:
            f.write(template.render(template_vars))

    def _get_default_template(self, template_file: str):
        template_file_name = template_file  + TEMPLATE_EXT

        env = Environment(loader=FileSystemLoader(self.templates_path), autoescape=select_autoescape(['html', 'xml']))

        return env.get_template(template_file_name)

class QueryProcessor:
    def __init__(self, queries: str):
        self.queries = queries

    def process_queries(self):
        if not self.queries:
            return []

        with self.queries as queries:
            try:
                processed_queries = json.load(queries)
                if isinstance(data, dict):
                    data = [data]
            except ValueError as err:
                raise exceptions.SystemSetupError(f"Ensure JSON schema is valid and queries are contained in a list: {err}")

        return processed_queries

def process_indices(indices, document_frequency, number_of_docs):
    processed_indices = []
    for index_name in indices:
        index = Index(
            name=index_name,
            document_frequency=document_frequency,
            number_of_docs=number_of_docs
        )
        processed_indices.append(index)

    return processed_indices


def validate_index_documents_map(indices, indices_docs_map):
    logger = logging.getLogger(__name__)
    logger.info("Indices Docs Map: [%s]", indices_docs_map)
    documents_limited = indices_docs_map is not None and len(indices_docs_map) > 0
    if not documents_limited:
        return

    if len(indices) < len(indices_docs_map):
        raise exceptions.SystemSetupError(
            "Number of <index>:<doc_count> pairs exceeds number of indices in --indices. " +
            "Ensure number of <index>:<doc_count> pairs is less than or equal to number of indices in --indices."
        )

    for index_name in indices_docs_map:
        if index_name not in indices:
            raise exceptions.SystemSetupError(
                "Index from <index>:<doc_count> pair was not found in --indices. " +
                "Ensure that indices from all <index>:<doc_count> pairs exist in --indices."
            )
