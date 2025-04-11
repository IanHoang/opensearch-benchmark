import logging

import json
import time
import os
import numpy as np
import hashlib
import sys
import importlib.util
import yaml

import dask
from dask.distributed import Client, as_completed, get_client
from multiprocessing import Process, Queue
from mimesis import Generic
from mimesis.schema import Schema
from mimesis.locales import Locale
from mimesis.random import Random
from mimesis import Cryptographic
from mimesis.providers.base import BaseProvider
from mimesis.random import Random
from tqdm import tqdm

from osbenchmark.utils import console
from osbenchmark.synthetic_data_generator.input_processor import create_sdg_config_from_args, use_custom_synthetic_data_generator, use_mappings_synthetic_data_generator
from osbenchmark.synthetic_data_generator.helpers import load_config, write_record_and_publish_summary_to_console
from osbenchmark.synthetic_data_generator.types import DEFAULT_MAX_FILE_SIZE_GB, DEFAULT_CHUNK_SIZE
from osbenchmark.synthetic_data_generator import custom_synthetic_data_generator, mapping_synthetic_data_generator

def orchestrate_data_generation(cfg):
    logger = logging.getLogger(__name__)
    sdg_config = create_sdg_config_from_args(cfg)

    # TODO: Rename custom config
    custom_config = load_config(sdg_config.custom_config_path)
    logger.info("HERE WITH CONFIG %s", custom_config)

    # TODO: Move client creation to outside of orchestrator so that synthetic data generators can call on it
    workers = custom_config.get("workers", os.cpu_count())
    dask_client = Client(n_workers=workers, threads_per_worker=1)  # We keep it to 1 thread because generating random data is CPU intensive
    blueprint = sdg_config.blueprint
    logger.info("Number of workers to use: %s", workers)
    logger.info("Blueprint: %s", json.dumps(blueprint, indent=2))

    console.println(f"[NOTE] Dashboard link to monitor processes and task streams: {dask_client.dashboard_link}")
    console.println("[NOTE] For users who are running generation on a virtual machine, consider tunneling to localhost to view dashboard.")
    console.println("")

    # TODO: Move to two separate functions
    if use_custom_synthetic_data_generator(sdg_config) and cfg.opts("synthetic_data_generator", "test_document"):
        custom_module = custom_synthetic_data_generator.load_user_module(sdg_config.custom_module_path)
        generate_fake_document = custom_module.generate_fake_document
        custom_module_components = custom_config.get('CustomSyntheticDataGenerator', {})
        logger.info("Custom module components %s", custom_module_components)

        custom_lists = custom_module_components.get('custom_lists', {})
        custom_providers = {name: getattr(custom_module, name) for name in custom_module_components.get('custom_providers', [])}
        logger.info("Custom lists %s custom providers %s", custom_lists, custom_providers)
        document = custom_synthetic_data_generator.generate_test_document(generate_fake_document, custom_lists, custom_providers)

        console.println("Generating a single test document:")
        console.println("Please verify that the output is generated as intended. \n")
        print(json.dumps(document, indent=2))

    elif use_custom_synthetic_data_generator(sdg_config):
        custom_module = custom_synthetic_data_generator.load_user_module(sdg_config.custom_module_path)

        print("Starting generation")
        # Generate all documents
        total_time_to_generate_dataset, generated_dataset_details = custom_synthetic_data_generator.generate_dataset_with_user_module(dask_client, sdg_config, custom_module, custom_config)

        write_record_and_publish_summary_to_console(sdg_config, total_time_to_generate_dataset, generated_dataset_details)

    elif use_mappings_synthetic_data_generator(sdg_config) and cfg.opts("synthetic_data_generator", "test_document"):
        # TODO Remove config from this meethod and just load it in the beginning
        raw_mappings, mapping_config = mapping_synthetic_data_generator.load_mapping_and_config(sdg_config.index_mappings_path, sdg_config.custom_config_path)
        document = mapping_synthetic_data_generator.generate_test_document(raw_mappings, mapping_config)

        console.println("Generating a single test document:")
        console.println("Please verify that the output is generated as intended. \n")
        print(json.dumps(document, indent=2))
    elif use_mappings_synthetic_data_generator(sdg_config):
        raw_mappings, mapping_config = mapping_synthetic_data_generator.load_mapping_and_config(sdg_config.index_mappings_path, sdg_config.custom_config_path)

        docs_written, total_time_to_generate_dataset, dataset_size = mapping_synthetic_data_generator.generate_dataset_with_mappings(dask_client, sdg_config, raw_mappings, mapping_config)

        record = {"index-name": sdg_config.index_name, "docs_added": docs_written, "dataset_size": dataset_size, "total_time_in_seconds_to_generate_docs_added": total_time_to_generate_dataset}
        summary = f"Generated {docs_written} docs in {total_time_to_generate_dataset} seconds. Total dataset size is {dataset_size}GB."
        path = os.path.join(sdg_config.output_path, f"{sdg_config.index_name}_record.json")
        with open(path, 'w') as file:
            json.dump(record, file, indent=2)

        console.println("")
        console.println(summary)
        logger.info("Visit the following path to view synthetically generated data: [%s]", sdg_config.output_path)
        console.println(f"Visit the following path to view synthetically generated data: {sdg_config.output_path}")

