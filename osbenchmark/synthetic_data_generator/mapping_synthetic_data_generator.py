# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import json
import random
import datetime
import uuid
import os
import time
import logging
import hashlib
from typing import Dict, Any, Callable

import yaml
from dask.distributed import Client, get_client, as_completed
from mimesis import Generic
from mimesis.locales import Locale
from mimesis.random import Random
from tqdm import tqdm

from osbenchmark.utils import console
from osbenchmark.exceptions import MappingsError, ConfigError
from osbenchmark.synthetic_data_generator.types import SyntheticDataGeneratorMetadata, GB_TO_BYTES
from osbenchmark.synthetic_data_generator.helpers import get_generation_settings, write_chunk, setup_custom_tqdm_formatting

# class MappingGenerationLogicWorker:
#     @staticmethod
#     def generate_documents_from_worker(index_mappings, mapping_config, docs_per_chunk, seed):
#         """
#         Within the scope of a Dask worker. Initially reconstructs the MappingConverter and generates documents.
#         This is because Dask coordinator needs to serialize and deserialize objects when passing them to a worker.
#         Generates the generate_fake_document, which gets invoked N number of times before returning a list of documents.

#         param: mapping_dict (dict): The OpenSearch mapping dictionary.
#         param: config_dict (dict): Optional YAML-based config for value constraints.
#         param: num_docs (int): Number of documents to generate.
#         param: seed (int): Initial number used as starting sequence for random generators

#         Returns: List of generated documents.
#         """
#         # Initialize parameters given to worker
#         mapping_generator = MappingConverter(mapping_config, seed)
#         mappings_with_generators = mapping_generator.transform_mapping_to_generators(index_mappings)

#         documents = [mapping_generator.generate_fake_document(mappings_with_generators) for _ in range(docs_per_chunk)]

#         return documents


# def generate_seeds_for_workers(regenerate=False):
#     client = get_client()
#     workers = client.scheduler_info()['workers']

#     seeds = []
#     for worker_id in workers.keys():
#         hash_object = hashlib.md5(worker_id.encode())

#         if regenerate:
#             # Add current timestamp to each hash to improve uniqueness
#             timestamp = str(time.time()).encode()
#             hash_object.update(timestamp)

#         hash_hex = hash_object.hexdigest()

#         seed = int(hash_hex[:8], 16)
#         seeds.append(seed)

#     return seeds

def get_avg_document_size(index_mappings: dict, mapping_config: dict) -> int:
    document = [generate_test_document(index_mappings, mapping_config)]
    write_chunk(document, '/tmp/test-size.json')

    size = os.path.getsize('/tmp/test-size.json')
    os.remove('/tmp/test-size.json')

    return size

def generate_test_document(index_mappings: dict, mapping_config: dict) -> dict:
    mapping_generator = MappingConverter(mapping_config)
    converted_mappings = mapping_generator.transform_mapping_to_generators(index_mappings)

    return mapping_generator.generate_fake_document(transformed_mapping=converted_mappings)

def generate_dataset_with_mappings(client: Client, sdg_metadata: SyntheticDataGeneratorMetadata, index_mappings: dict, sdg_config: dict):
    """
    param: client: Dask client that performs multiprocessing and creates dashboard to visualize task streams
    param: sdg_metadata: SyntheticDataGenerationConfig instance that houses information related to data corpora to generate
    param: index_mappings: OpenSearch index mappings that user provided
    param: sdg_config: Optional config that specifies custom lists and custom data providers that the custom module uses to generate data.
        This also contains configuration details related to how data is generated (i.e. number of workers to use, max file size in GB, and number of documents in a chunk)

    returns: Does not return results but writes documents to output path
    """
    logger = logging.getLogger(__name__)

    # Fetch settings and input config that user provided
    generation_settings = get_generation_settings(sdg_config)
    mapping_config = sdg_config

    max_file_size_bytes = generation_settings.get('max_file_size_gb') * GB_TO_BYTES
    total_size_bytes = sdg_metadata.total_size_gb * GB_TO_BYTES
    docs_per_chunk = generation_settings.get('docs_per_chunk')
    avg_document_size = get_avg_document_size(index_mappings, mapping_config)

    current_size = 0
    docs_written = 0
    file_counter = 0
    generated_dataset_details = []

    logger.info("Average document size in bytes: %s", avg_document_size)
    logger.info("Chunk size: %s docs", docs_per_chunk)
    logger.info("Total GB to generate: %s", sdg_metadata.total_size_gb)
    logger.info("Max file size in GB: %s", generation_settings.get('max_file_size_gb'))

    console.println(f"Total GB to generate: {sdg_metadata.total_size_gb}\n"
                    f"Average document size in bytes: {avg_document_size}\n"
                    f"Max file size in GB: {generation_settings.get('max_file_size_gb')}\n")

    start_time = time.time()
    with tqdm(total=total_size_bytes,
                unit='B',
                unit_scale=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as progress_bar:

        setup_custom_tqdm_formatting(progress_bar)
        while current_size < total_size_bytes:
            file_path = os.path.join(sdg_metadata.output_path, f"{sdg_metadata.index_name}_{file_counter}.json")
            file_size = 0
            docs_written = 0

            while file_size < max_file_size_bytes:
                generation_start_time = time.time()
                seeds = generate_seeds_for_workers(regenerate=True)
                logger.info("Mapping SDG seeds: %s", seeds)

                futures = [client.submit(
                    MappingGenerationLogicWorker.generate_documents_from_worker,
                    index_mappings,
                    mapping_config,
                    docs_per_chunk,
                    seed) for seed in seeds]

                writing_start_time = time.time()
                for _, data in as_completed(futures, with_results=True):
                    written = write_chunk(data, file_path)
                    docs_written += written
                    written_size = written * avg_document_size
                    current_size += written_size
                    progress_bar.update(written_size)
                writing_end_time = time.time()

                file_size = os.path.getsize(file_path)
                # If it exceeds the max file size, then append this to keep track of record
                if file_size >= max_file_size_bytes:
                    file_name = os.path.basename(file_path)
                    generated_dataset_details.append({
                        "file_name": file_name,
                        "docs": docs_written,
                        "file_size_bytes": file_size
                    })

                generating_took_time = writing_start_time - generation_start_time
                writing_took_time = writing_end_time - writing_start_time
                logger.info("Generating took [%s] seconds", generating_took_time)
                logger.info("Writing took [%s] seconds", writing_took_time)

                if current_size >= total_size_bytes:
                    file_name = os.path.basename(file_path)
                    generated_dataset_details.append({
                        "file_name": file_name,
                        "docs": docs_written,
                        "file_size_bytes": file_size
                    })
                    break

            file_counter += 1

        end_time = time.time()
        total_time_to_generate_dataset = round(end_time - start_time)
        progress_bar.update(total_size_bytes - progress_bar.n)

        logger.info("Generated dataset in %s seconds. Dataset generation details: %s", total_time_to_generate_dataset, generated_dataset_details)

        return total_time_to_generate_dataset, generated_dataset_details
