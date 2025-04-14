# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import logging
import os
import json

from osbenchmark import PROGRAM_NAME, exceptions
from osbenchmark.client import OsClientFactory
from osbenchmark.workload_generator.config import CustomWorkload
from osbenchmark.workload_generator.helpers import QueryProcessor, CustomWorkloadWriter, process_indices, validate_index_documents_map
from osbenchmark.workload_generator.extractors import IndexExtractor, SequentialCorpusExtractor
from osbenchmark.utils import io, opts, console

def create_workload(cfg):
    logger = logging.getLogger(__name__)

    # All inputs provided by user
    workload_name: str = cfg.opts("workload", "workload.name")
    indices: list = cfg.opts("generator", "indices")
    output_path: str = cfg.opts("generator", "output.path")
    target_hosts: opts.TargetHosts = cfg.opts("client", "hosts")
    client_options: opts.ClientOptions = cfg.opts("client", "options")
    # document_frequency: int = cfg.opts("generator", "document_frequency") # Enable later
    document_frequency: int = 0
    number_of_docs: dict = cfg.opts("generator", "number_of_docs")
    unprocessed_queries: dict = cfg.opts("workload", "custom_queries")
    templates_path: str = os.path.join(cfg.opts("node", "benchmark.root"), "resources")

    # Validation
    validate_index_documents_map(indices, number_of_docs)

    client = OsClientFactory(hosts=target_hosts.all_hosts[opts.TargetHosts.DEFAULT],
                             client_options=client_options.all_client_options[opts.TargetHosts.DEFAULT]).create()
    info = client.info()
    console.info(f"Connected to OpenSearch cluster [{info['name']}] version [{info['version']['number']}].\n", logger=logger)

    processed_indices = process_indices(indices, document_frequency, number_of_docs)
    logger.info("Processed Indices: %s", processed_indices)

    custom_workload = CustomWorkload(
        workload_name=workload_name,
        output_path=output_path,
        indices=processed_indices,
    )
    custom_workload.workload_path = os.path.abspath(os.path.join(io.normalize_path(output_path), workload_name))
    custom_workload.operations_path = os.path.join(custom_workload.workload_path, "operations")
    custom_workload.test_procedures_path = os.path.join(custom_workload.workload_path, "test_procedures")

    query_processor = QueryProcessor(unprocessed_queries)
    custom_workload_writer = CustomWorkloadWriter(custom_workload, templates_path)
    index_extractor = IndexExtractor(custom_workload, client)
    corpus_extractor = SequentialCorpusExtractor(custom_workload, client)

    # Process Queries
    processed_queries = query_processor.process_queries()
    custom_workload.queries = processed_queries
    logger.info("Processed custom queries [%s]", custom_workload.queries)

    # Create Workload Output Path
    custom_workload_writer.make_workload_directory()
    logger.info("Created workload output path at [%s]", custom_workload.workload_path)

    # Extract Index Settings and Mappings
    custom_workload.extracted_indices, custom_workload.failed_indices = index_extractor.extract_indices(custom_workload.workload_path)
    logger.info("Extracted index settings and mappings from [%s]", custom_workload.indices)

    # Extract Corpora
    for index in custom_workload.indices:
        index_corpora = corpus_extractor.extract_documents(index.name, index.number_of_docs)
        custom_workload.corpora.append(index_corpora)
    logger.info("Extracted all corpora [%s]", custom_workload.corpora)

    if len(custom_workload.corpora) == 0:
        raise exceptions.BenchmarkError("Failed to extract corpora for any indices for workload!")

    template_vars = {
        "workload_name": custom_workload.workload_name,
        "indices": custom_workload.extracted_indices,
        "corpora": custom_workload.corpora,
        "custom_queries": custom_workload.queries
    }
    logger.info("Template vars [%s]", template_vars)

    # Render all templates
    custom_workload_writer.render_templates(template_vars, custom_workload.queries)

    # Write exportable template
    record_path = f"{custom_workload.workload_path}/{custom_workload.workload_name}_record.json"
    if os.path.exists(record_path):
        console.println("Record file already exists. Overriding")

    try:
        with open(record_path, 'w') as file:
            json.dump(template_vars, file, indent=2)
        print(f"Record successfully written to '{record_path}'")
    except Exception as e:
        print(f"An error occurred: {e}")

    console.println("")
    console.info(f"Workload {workload_name} has been created. Run it with: {PROGRAM_NAME} --workload-path={custom_workload.workload_path}")
