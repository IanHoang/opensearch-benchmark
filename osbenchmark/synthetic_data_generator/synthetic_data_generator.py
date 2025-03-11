# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import json
import logging
import multiprocessing
import time
from typing import Dict, Any, List
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import dask
from dask import delayed
import dask.bag as db
from dask.distributed import Client, Lock, wait

from osbenchmark.utils import console
from osbenchmark.synthetic_data_generator.types import GeneratorTypes
from osbenchmark.synthetic_data_generator.input_processor import create_sdg_config_from_args

class SyntheticDataGenerator:
    @staticmethod
    def compile_blueprint(blueprint: Dict[str, Any]) -> str:
        """
        Recursively traverse mappings or template and compile a blueprint
        """
        lines = []
        for key, value in blueprint.items():
            if isinstance(value, dict) and 'data_generator_type' in value:
                generator = f"GeneratorTypes.{value['data_generator_type'].upper()}.value.generate"
                params = ', '.join(f"{k}={v}" for k, v in value.get('params', {}).items())
                lines.append(f"'{key}': {generator}({params})")
            elif isinstance(value, dict):
                nested_dict = SyntheticDataGenerator.compile_blueprint(value)
                lines.append(f"'{key}': {nested_dict}")
            elif isinstance(value, list):
                nested_list = [SyntheticDataGenerator.compile_blueprint(item) if isinstance(item, dict) else repr(item) for item in value]
                lines.append(f"'{key}': [{', '.join(nested_list)}]")
            else:
                lines.append(f"'{key}': {repr(value)}")

        return f"{{{', '.join(lines)}}}"

    @staticmethod
    def create_sdg_function(blueprint: Dict[str, Any]) -> callable:
        """
        Create a callable function containing all the Data Generators.
        The created function can be called and will generate the random values.
        """
        compiled_blueprint = SyntheticDataGenerator.compile_blueprint(blueprint)
        func_def = f"def generate_document():\n    return {compiled_blueprint}"

        namespace = {}
        exec(func_def, globals(), namespace)
        return namespace['generate_document']

    @staticmethod
    def generate_data(generate_document: callable, chunk_size: int) -> List[Dict[str, Any]]:
        return [generate_document() for _ in range(chunk_size)]

def generate_one_document_and_get_size(sdg_function: callable):
    output = [sdg_function()]
    write_chunk(output, '/tmp/test-size.json')

    size = os.path.getsize('/tmp/test-size.json')
    os.remove('/tmp/test-size.json')
    console.println(f"Size of one document: {size}")
    return size

def generate_compiled_data_chunk(generate_document: callable, chunk_size: int) -> List[Dict[str, Any]]:
        return SyntheticDataGenerator.generate_data(generate_document, chunk_size)

def generate_dataset_with_compilation(total_size_gb: float, output_dir: str, blueprint: Dict[str, Any], workers: int):
    logger = logging.getLogger(__name__)
    start_time = time.time()

    chunk_size = 10000  # Increased chunk size for better performance
    total_size_bytes = total_size_gb * 1024 * 1024 * 1024
    current_size = 0
    file_counter = 0

    # Compile the blueprint once
    generate_document = SyntheticDataGenerator.create_sdg_function(blueprint)

    approximate_doc_size = generate_one_document_and_get_size(generate_document)

    client = Client(n_workers=8, threads_per_worker=1)

    # TESTING HERE
    # output = [generate_document()]
    # write_chunk(output, '/home/ec2-user/test-size.json')

    # size = os.path.getsize('/home/ec2-user/test-size.json')
    # console.println(f"size: {size}")
    # output = generate_compiled_data_chunk(generate_document=generate_document, chunk_size=10000)
    # console.println(len(output))
    # len_of_written_data = write_chunk(output, '/home/ec2-user/sdg-output.json')
    # print(f"Number of docs written: {len_of_written_data}")
    # print(f"Estimated size in bytes: {len_of_written_data * 1000}")
    # size = os.path.getsize('/home/ec2-user/sdg-output.json')
    # print(f"Size according to OS library: {size}")


    while current_size < total_size_bytes:
        file_path = os.path.join(output_dir, f"data_{file_counter}.json")
        file_size = 0

        while file_size < 40 * 1024 * 1024 * 1024 and current_size < total_size_bytes:
            # Submit
            futures = [client.submit(generate_compiled_data_chunk, generate_document, chunk_size) for _ in range(8)]
            results = client.gather(futures)

            # Write data
            for data in results:
                written = write_chunk(data, file_path)
                current_size += written * approximate_doc_size

            logger.info(f"Current Size {current_size} Bytes and {current_size / (1024 * 1024 * 1024):.2f} GB. Requested for {total_size_gb} GB")

            if current_size >= total_size_bytes:
                break

        file_counter += 1


    client.close()
    end_time = time.time()
    logger.info(f"Generated {current_size / (1024 * 1024 * 1024):.2f} GB dataset in {end_time - start_time:.2f} seconds")

def write_chunk(data, file_path):
    with open(file_path, 'a') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
    return len(data)

def get_cores_in_lg_host():
    return os.cpu_count()

def orchestrate_data_generation(cfg):
    logger = logging.getLogger(__name__)
    sdg_config = create_sdg_config_from_args(cfg)

    blueprint = sdg_config.blueprint
    logger.info("Blueprint: %s", json.dumps(blueprint, indent=2))

    total_size_gb = sdg_config.total_size_gb
    output_dir = sdg_config.output_path
    workers = get_cores_in_lg_host()

    generate_dataset_with_compilation(total_size_gb, output_dir, blueprint, workers)


# def user_confirmed_accurate_setup(sdg_config):
#     logger = logging.getLogger(__name__)
#     logger.info("Synthetic Data Generation Config: %s", sdg_config.index_name)
#     logger.info("Generation Blue Print: %s", json.dumps(sdg_config.blueprint, indent=2))

#     user_confirmed = input(f"Synthetic Data Generation Blueprints for {[sdg_config.index_name]}. Continue? [y/n]: ")
#     valid_responses = ['y', 'n']
#     while user_confirmed not in valid_responses:
#         user_confirmed = input(f"Synthetic Data Generation Blueprints for {[sdg_config.index_name]}. Continue? [y/n]: ")

#     return user_confirmed == 'y'