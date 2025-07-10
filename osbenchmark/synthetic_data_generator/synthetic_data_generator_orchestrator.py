# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import logging
import sys

import json
import os

from dask.distributed import Client

from osbenchmark.utils import console
from osbenchmark.synthetic_data_generator.input_processor import create_sdg_metadata_from_args, use_custom_synthetic_data_generator, use_mappings_synthetic_data_generator
from osbenchmark.synthetic_data_generator import helpers
from osbenchmark.synthetic_data_generator.synthetic_data_generator import SyntheticDataGenerator
from osbenchmark.synthetic_data_generator.strategies import CustomModuleStrategy, MappingStrategy
from osbenchmark.synthetic_data_generator.types import SyntheticDataGeneratorMetadata

def orchestrate_data_generation(cfg):
    logger = logging.getLogger(__name__)
    sdg_metadata = create_sdg_metadata_from_args(cfg)
    test_document_requested = cfg.opts("synthetic_data_generator", "test_document")

    sdg_config = helpers.load_config(sdg_metadata.custom_config_path) if sdg_metadata.custom_config_path else {}

    # TODO: Move client creation deeper so that cancellation is easier on the command line
    workers = sdg_config.get("settings", {}).get("workers", os.cpu_count())
    dask_client = Client(n_workers=workers, threads_per_worker=1)  # We keep it to 1 thread because generating random data is CPU intensive
    logger.info("Number of workers to use: [%s]", workers)

    console.println(f"[NOTE] ✨ Dashboard link to monitor processes and task streams: [{dask_client.dashboard_link}]")
    console.println("[NOTE] ✨ For users who are running generation on a virtual machine, consider tunneling to localhost to view dashboard.")
    console.println("")

    if helpers.host_has_available_disk_storage(sdg_metadata):
        if use_custom_synthetic_data_generator(sdg_metadata):
            logger.info("Generating data with Custom Module Strategy")
            custom_module = helpers.load_user_module(sdg_metadata.custom_module_path) # load it as a callable
            strategy = CustomModuleStrategy(sdg_metadata, sdg_config, custom_module)

        elif use_mappings_synthetic_data_generator(sdg_metadata):
            logger.info("Generating data with Mapping Strategy")
            raw_mappings = helpers.load_mapping(sdg_metadata.index_mappings_path)
            strategy = MappingStrategy(sdg_metadata, sdg_config, raw_mappings)

        # Initialize SDG with Strategy chosen
        sdg = SyntheticDataGenerator(sdg_metadata, sdg_config, dask_client, strategy)

        if test_document_requested:
            document = sdg.generate_test_document()
            console.println("Generating a single test document:")
            console.println("Please verify that the output is generated as intended. ✅ \n")
            print(json.dumps(document, indent=2))
        else:
            # Generate all documents
            total_time_to_generate_dataset, generated_dataset_details = sdg.generate_dataset()

            helpers.write_record_and_publish_summary_to_console(sdg_metadata, total_time_to_generate_dataset, generated_dataset_details)
    else:
        logger.error("User wants to generate [%s]GB but current host does not have enough available disk storage.", sdg_metadata.total_size_gb)
        console.println(f"User wants to generate [{sdg_metadata.total_size_gb}]GB but current host does not have enough available disk storage")
        sys.exit(1)
