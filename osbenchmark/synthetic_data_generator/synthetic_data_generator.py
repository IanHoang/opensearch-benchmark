import logging

import json
import time
import os
import numpy as np
import hashlib
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
from osbenchmark.synthetic_data_generator.input_processor import create_sdg_config_from_args, use_custom_module
from osbenchmark.synthetic_data_generator.types import DEFAULT_MAX_FILE_SIZE_GB, DEFAULT_CHUNK_SIZE

def load_user_module(file_path):
    spec = importlib.util.spec_from_file_location("user_module", file_path)
    user_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(user_module)
    return user_module

def load_config(config_path):
    if config_path.endswith('.yml') or config_path.endswith('.yaml'):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    else:
        return {}

def write_chunk(data, file_path):
    with open(file_path, 'a') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
    return len(data)

# We just need to ensure that reseeding is after custom data providers are added. But we also have ot ensure that custom providers have reseed abilities
def instantiate_all_providers(custom_providers):
    g = Generic(locale=Locale.DEFAULT)
    r = Random()

    if custom_providers:
        # Updates generic provider to have custom providers. These providers must extend BaseProvider in Mimesis
        g = add_custom_providers(g, custom_providers)

    provider_instances = {
        'generic': g,
        'random': r
    }

    return provider_instances

def seed_providers(providers, seed=None):
    for key, provider_instance in providers.items():
        if key in ['generic']:
            provider_instance.reseed(seed)
        elif key in ['random']:
            provider_instance.seed(seed)

    return providers

def add_custom_providers(generic, custom_providers):
    for name, provider_class in custom_providers.items():
        if issubclass(provider_class, BaseProvider):
            generic.add_provider(provider_class)
        else:
            # If it's not a Mimesis provider, we'll add it as is
            setattr(generic, name, provider_class())
    return generic

def generate_seeds_for_workers(regenerate=False):
    client = get_client()
    workers = client.scheduler_info()['workers']

    seeds = []
    for worker_id in workers.keys():
        hash_object = hashlib.md5(worker_id.encode())

        if regenerate:
            # Add current timestamp to each hash to improve uniqueness
            timestamp = str(time.time()).encode()
            hash_object.update(timestamp)

        hash_hex = hash_object.hexdigest()

        seed = int(hash_hex[:8], 16)
        seeds.append(seed)

    return seeds

def get_avg_document_size(generate_fake_document: callable, custom_providers: dict, custom_lists: dict) -> int:
    providers = instantiate_all_providers(custom_providers)
    providers = seed_providers(providers)
    document = generate_fake_document(providers=providers, **custom_lists)

    output = [document]
    write_chunk(output, '/tmp/test-size.json')

    size = os.path.getsize('/tmp/test-size.json')
    os.remove('/tmp/test-size.json')

    return size

# In the real Program, this should not change. This is a worker. But should this be split to where the instantiation of objects are separate from generating large chunks?
def generate_data_chunk(user_defined_function: callable, chunk_size: int, custom_lists, custom_providers, seed=None):
    """
    Synthetic Data Generator Worker that calls a function that generates a single document.
    The worker will call the function N number of times to generate N docs of data before returning results

    :param user_defined_function: This is the callable that the user defined in their module.
        The callable should be named 'generate_fake_document()'
    :param chunk_size: The number of documents the worker needs to generate before returning them in a list
    :param custom_lists (optional): These are custom lists that the user_defined_function uses to generate random values
    :param custom_providers (optional): These are custom providers (written in Mimesis or Faker) that generate data in a specific way.
        Users define this in the same file as generate_fake_document() function

    :returns List of documents to be written to disk
    """
    providers = instantiate_all_providers(custom_providers)
    providers = seed_providers(providers, seed)

    return [user_defined_function(providers=providers, **custom_lists) for _ in range(chunk_size)]


def generate_dataset_with_user_module(client, sdg_config, user_module, user_config):
    """
    This is used whenever a user has provided their own custom module to generate fake data with.
    This module must contain a function called generate_fake_document(), which houses the definitions of a single synthetic document. It can also
    contain a custom data generators or data providers. It's recommended that custom data generators or data providers are written in Mimesis but they
    can also be written in Faker or use other Python libraries. For best performance, libraries other than Mimesis or Faker should be highly-performant libraries.
    For example, if we want to have a custom data provider that generates a list of random values, we should leverage random library as it bypasses python's GIL.
    For some business use-cases, it might be hard to find highly-performant libraries so writing any code to generate logic is fine but understand that there might be performance limitations

    param: client: Dask client that performs multiprocessing and creates dashboard to visualize task streams
    param: sdg_config: SyntheticDataGenerationConfig instance that houses information related to data corpora to generate
    param: user_module: Python module that user supplies containing logic to generate synthetic documents
    param: user_config: Optional config that specifies custom lists and custom data providers that the custom module uses to generate data.
        This also contains configuration details related to how data is generated (i.e. number of workers to use, max file size in GB, and number of documents in a chunk)

    returns: Does not return results but writes documents to output path
    """
    logger = logging.getLogger(__name__)

    custom_lists = user_config.get('custom_lists', {})
    custom_providers = {name: getattr(user_module, name) for name in user_config.get('custom_providers', [])}
    max_file_size_bytes = user_config.get('max_file_size_gb', DEFAULT_MAX_FILE_SIZE_GB) * 1024 * 1024 * 1024
    total_size_bytes = sdg_config.total_size_gb * 1024 * 1024 * 1024
    chunk_size = user_config.get('chunk_size', DEFAULT_CHUNK_SIZE)  # Adjust this based on your memory constraints

    generate_fake_document = user_module.generate_fake_document
    avg_document_size = get_avg_document_size(generate_fake_document, custom_providers, custom_lists)

    current_size = 0
    docs_written = 0
    file_counter = 0

    logger.info("Average document size: %s", avg_document_size)
    logger.info("Chunk size: %s docs", chunk_size)
    logger.info("Total GB to generate: %s", sdg_config.total_size_gb)

    # from dask.distributed import performance_report
    start_time = time.time()
    with tqdm(total=total_size_bytes) as progress_bar:
        while current_size < total_size_bytes:
            file_path = os.path.join(sdg_config.output_path, f"{sdg_config.index_name}_{file_counter}.json")
            file_size = 0

            while file_size < max_file_size_bytes:  # 40GB make this configurable in the benchmark.ini or generation.ini or config.yml
                generation_start_time = time.time()
                seeds = generate_seeds_for_workers(regenerate=True)
                logger.info("Using seeds: %s", seeds)

                # Test if 40GB works by removing seed and just doing for _ in range(workers)
                # with performance_report(filename="financial_mimesis_10GB.html"):
                futures = [client.submit(generate_data_chunk, generate_fake_document, chunk_size, custom_lists, custom_providers, seed) for seed in seeds]
                results = client.gather(futures) # if using AS_COMPLETED remove this line

                writing_start_time = time.time()
                for data in results:
                    written = write_chunk(data, file_path)
                    docs_written += written
                    written_size = written * avg_document_size
                    current_size += written_size
                    progress_bar.update(written_size)

                file_size = os.path.getsize(file_path)
                writing_end_time = time.time()
                generating_took_time = writing_start_time - generation_start_time
                writing_took_time = writing_end_time - writing_start_time
                logger.info("Generating took [%s] seconds", generating_took_time)
                logger.info("Writing took [%s] seconds", writing_took_time)
                if current_size >= total_size_bytes:
                    break

            file_counter += 1

        end_time = time.time()
        total_time_to_generate_data = end_time - start_time
        progress_bar.update(total_size_bytes - progress_bar.n)
        logger.info("Generated %s docs and %s GB dataset in %s seconds", docs_written, file_size, total_time_to_generate_data)
        console.println(f"Generated {docs_written} docs and {file_size}GB dataset in {end_time - start_time:.2f} seconds")

def orchestrate_data_generation(cfg):
    logger = logging.getLogger(__name__)
    sdg_config = create_sdg_config_from_args(cfg)
    custom_module = load_user_module(sdg_config.custom_module_path)
    custom_config = load_config(sdg_config.custom_config_path)

    workers = custom_config.get("workers", os.cpu_count())
    dask_client = Client(n_workers=workers, threads_per_worker=1)  # We keep it to 1 thread because generating random data is CPU intensive
    logger.info("Number of workers to use: %s", workers)
    blueprint = sdg_config.blueprint
    logger.info("Blueprint: %s", json.dumps(blueprint, indent=2))
    print(sdg_config)
    print(custom_module)
    print(custom_config)

    #TODO: Create Dask Dashboard
    console.println(f"Dashboard link to monitor processes and task streams: {dask_client.dashboard_link}")
    console.println("For users who are running generation on a virtual machine, consider tunneling to localhost to view dashboard.")
    console.println("")

    #TODO: Add way to modify default chunk size and worker count
    # should it be benchmark.ini file or generation.ini file or config.yml

    if use_custom_module(sdg_config):
        # my_seed = 1
        # generate_fake_document = custom_module.generate_fake_document
        # custom_lists = custom_config.get('custom_lists', {})
        # custom_providers = {name: getattr(custom_module, name) for name in custom_config.get('custom_providers', [])}
        # # Instantiates, adds, and seeds providers them
        # providers = instantiate_all_providers(custom_providers)
        # providers = seed_providers(providers, my_seed)

        # # print("Float Number: ", providers['generic'].numeric.float_number(start=0, end=10))
        # # print("Numeric String: ", providers['generic'].numeric_string.generate(length=9))

        # document = generate_fake_document(providers=providers, **custom_lists)
        # print(json.dumps(document, indent=2))

        print("Starting generation")
        # Generate all documents
        generate_dataset_with_user_module(dask_client, sdg_config, custom_module, custom_config)
    else:
        # Automated method
        pass

    logger.info("Visit the following path to view synthetically generated data: [%s]", sdg_config.output_path)
    console.println("")
    console.println(f"Visit the following path to view synthetically generated data: {sdg_config.output_path}")