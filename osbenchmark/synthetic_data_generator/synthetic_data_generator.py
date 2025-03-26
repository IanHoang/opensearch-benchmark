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

from osbenchmark.utils import console
from osbenchmark.synthetic_data_generator.input_processor import create_sdg_config_from_args, use_manual_method

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

def write_chunk(data, file_path):
    with open(file_path, 'a') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
    return len(data)

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
    # Instantiates, adds, and seeds providers them
    providers = instantiate_all_providers(custom_providers)
    providers = seed_providers(providers, seed)

    return [user_defined_function(providers=providers, **custom_lists) for _ in range(chunk_size)]


# This is most ideal since we do not need to put pure=False and also we use actual seeds
def generate_dataset_with_user_module(client, total_size_gb, output_dir, user_module, user_config):
    # Fetch custom lists and providers user provided via Config.yml and the generate_fake_document() function
    custom_lists = user_config.get('custom_lists', {})
    custom_providers = {name: getattr(user_module, name) for name in user_config.get('custom_providers', [])}
    generate_fake_document = user_module.generate_fake_document

    start_time = time.time()
    chunk_size = 10000  # Adjust this based on your memory constraints
    avg_document_size = get_avg_document_size(generate_fake_document, custom_providers, custom_lists)
    total_size_bytes = total_size_gb * 1024 * 1024 * 1024
    current_size = 0
    docs_written = 0
    file_counter = 0

    from dask.distributed import performance_report
    # Start Generating Data
    while current_size < total_size_bytes:
        file_path = os.path.join(output_dir, f"stocks_{file_counter}.json")
        file_size = 0

        while file_size < 40 * 1024 * 1024 * 1024:  # 40GB
            generation_start_time = time.time()
            seeds = generate_seeds_for_workers(regenerate=True)
            print("Seeds: ", seeds)

            # Test if 40GB works by removing seed and just doing for _ in range(workers)
            with performance_report(filename="financial_mimesis_10GB.html"):
                futures = [client.submit(generate_data_chunk, generate_fake_document, chunk_size, custom_lists, custom_providers, seed) for seed in seeds]
                results = client.gather(futures) # if using AS_COMPLETED remove this line

            writing_start_time = time.time()
            for data in results:
                written = write_chunk(data, file_path)
                docs_written += written
                current_size += written * avg_document_size

            file_size = os.path.getsize(file_path)
            writing_end_time = time.time()
            print(f"Generating took {writing_start_time - generation_start_time:.2f} seconds")
            print(f"Writing took {writing_end_time - writing_start_time:.2f} seconds")

            if current_size >= total_size_bytes:
                break

        file_counter += 1

    end_time = time.time()
    print(f"Generated {docs_written} docs and {file_size}GB dataset in {end_time - start_time:.2f} seconds")

def orchestrate_data_generation(cfg):
    logger = logging.getLogger(__name__)
    sdg_config = create_sdg_config_from_args(cfg)

    blueprint = sdg_config.blueprint
    logger.info("Blueprint: %s", json.dumps(blueprint, indent=2))

    total_size_gb = sdg_config.total_size_gb
    output_dir = sdg_config.output_path
    workers = os.cpu_count()
    dask_client = Client(n_workers=workers, threads_per_worker=1)  # We keep it to 1 thread because generating random data is CPU intensive
    # Detect if using automated index mapping mode or manual module + config

    # Load in module user provided and config.yml
    custom_module = load_user_module(sdg_config.custom_module_path)
    custom_config = load_config(sdg_config.custom_config_path)
    print(sdg_config)
    print(custom_module)
    print(custom_config)

    # Create Dask Dashboard
    if use_manual_method(sdg_config):
        my_seed = 1
        generate_fake_document = custom_module.generate_fake_document
        custom_lists = custom_config.get('custom_lists', {})
        custom_providers = {name: getattr(custom_module, name) for name in custom_config.get('custom_providers', [])}
        # Instantiates, adds, and seeds providers them
        providers = instantiate_all_providers(custom_providers)
        providers = seed_providers(providers, my_seed)

        # print("Float Number: ", providers['generic'].numeric.float_number(start=0, end=10))
        # print("Numeric String: ", providers['generic'].numeric_string.generate(length=9))

        document = generate_fake_document(providers=providers, **custom_lists)
        print(json.dumps(document, indent=2))

        # Generate all documents
        # generate_dataset_with_user_module(dask_client, total_size_gb, output_dir, custom_module, custom_config)