import json
import random
import datetime
import uuid
import yaml
import os
import time
import logging
import hashlib
from typing import Dict, Any, Callable, Optional

import dask
from dask.distributed import Client, as_completed, get_client
from mimesis import Generic
from mimesis.schema import Schema
from mimesis.locales import Locale
from mimesis.random import Random
from mimesis import Cryptographic
from mimesis.providers.base import BaseProvider
from tqdm import tqdm

from osbenchmark.utils import console
from osbenchmark.synthetic_data_generator.types import DEFAULT_MAX_FILE_SIZE_GB, DEFAULT_CHUNK_SIZE, SyntheticDataGeneratorConfig
from osbenchmark.synthetic_data_generator.helpers import get_generation_settings

def write_chunk(data, file_path):
    with open(file_path, 'a') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
    return len(data)

class MappingSyntheticDataGenerator:
    def __init__(self, mapping_config=None):
        self.mapping_config = mapping_config or {}
        # self.locale = self.mapping_config.get('mimesis_locale', 'DEFAULT')
        seed = 1
        self.generic = Generic(locale=Locale.EN)
        self.random = Random()

        self.generic.reseed(seed)
        self.random.seed(seed)

        # seed these

        self.type_generators = {
            "text": self.generate_text,
            "keyword": self.generate_keyword,
            "long": self.generate_long,
            "integer": self.generate_integer,
            "short": self.generate_short,
            "byte": self.generate_byte,
            "double": self.generate_double,
            "float": self.generate_float,
            "boolean": self.generate_boolean,
            "date": self.generate_date,
            "ip": self.generate_ip,
            "object": self.generate_object,
            "nested": self.generate_nested,
            "geo_point": self.generate_geo_point,
        }

    def generate_text(self, field_def: Dict[str, Any],  **params) -> str:
        choices = params.get('must_include', None)
        analyzer = field_def.get("analyzer", "standard")

        text = ""
        if choices:
            term = random.choice(choices)
            text += f"{term} "
        if analyzer == "keyword":
            text += f"keyword_{uuid.uuid4().hex[:8]}"
            return text

        text += f"Sample text for {random.randint(1, 100)}"
        return text


    def generate_keyword(self, field_def: Dict[str, Any], **params) -> str:
        choices = params.get('choices', None)
        if choices:
            keyword = random.choice(choices)
            return keyword
        else:
            return f"key_{uuid.uuid4().hex[:8]}"

    def generate_long(self, field_def: Dict[str, Any]) -> int:
        return random.randint(-9223372036854775808, 9223372036854775807)

    def generate_integer(self, field_def: Dict[str, Any], **params) -> int:
        min = params.get('min', -2147483648)
        max = params.get('max', 2147483647)

        return random.randint(min, max)

    def generate_short(self, field_def: Dict[str, Any]) -> int:
        return random.randint(-32768, 32767)

    def generate_byte(self, field_def: Dict[str, Any]) -> int:
        return random.randint(-128, 127)

    def generate_double(self, field_def: Dict[str, Any]) -> float:
        return random.uniform(-1000000, 1000000)

    def generate_float(self, field_def: Dict[str, Any], **params) -> float:
        min = params.get('min', 0)
        max = params.get('max', 1000)
        return random.uniform(-1000, 1000)

    def generate_boolean(self, field_def: Dict[str, Any]) -> bool:
        return random.choice([True, False])

    def generate_date(self, field_def: Dict[str, Any], **params) -> str:
        # Default to ISO format unless a specific format is defined
        date_format = field_def.get("format", "yyyy-mm-dd")

        # Get a date range (from mapping config or default)
        format = params.get("format", "yyyy-MM-dd'T'HH:mm:ssZ")
        start_date = params.get("start_date", "2000-01-01")
        end_date = params.get("end_date", "2030-12-31")

        start_dt = datetime.datetime.fromisoformat(start_date)
        end_dt = datetime.datetime.fromisoformat(end_date)
        random_date = start_dt + datetime.timedelta(
            days=random.randint(0, (end_dt - start_dt).days)
        )

        # Apply formatting
        if date_format == "yyyy-MM-dd":
            return random_date.strftime("%Y-%m-%d")
        elif date_format == "yyyy-MM-dd'T'HH:mm:ssZ":
            return random_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        return random_date.isoformat()  # Default ISO format

    def generate_ip(self, field_def: Dict[str, Any]) -> str:
        return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

    def generate_geo_point(self, field_def: Dict[str, Any]) -> Dict[str, float]:
        return {
            "lat": random.uniform(-90, 90),
            "lon": random.uniform(-180, 180)
        }

    def generate_object(self, field_def: Dict[str, Any]) -> Dict[str, Any]:
        # This will be replaced by the nested fields generator
        return {}

    def generate_nested(self, field_def: Dict[str, Any]) -> list:
        # Will be replaced by a list of nested objects
        return []

    def transform_mapping_to_generators(self, mapping_dict: Dict[str, Any]) -> Dict[str, Callable[[], Any]]:
        """
        Transform an OpenSearch mapping into a dictionary of generator functions.
        Supports config-based overrides and type-based defaults.

        Args:
            mapping_dict: The OpenSearch mapping.
            config_dict: The optional config dict loaded from YAML.

        Returns:
            A dictionary mapping field names to generator functions.
        """
        generator_dict = {}

        # Extract default generators and overrides from config
        # TODO: Update this to be within the mapping config already? or just use the self.mapping_config and ensure that it is alreaday pointing to contents of MappingSyntheticDataGenerator
        config = self.mapping_config.get("MappingSyntheticDataGenerator", {}) if self.mapping_config else {}
        default_generators = config.get("default_generators", {})
        overrides = config.get("overrides", {})

        if "mappings" in mapping_dict:
            properties = mapping_dict["mappings"].get("properties", {})
        else:
            properties = mapping_dict.get("properties", mapping_dict)

        for field_name, field_def in properties.items():
            field_type = field_def.get("type")

            # Handle special cases like object and nested
            if field_type in {"object", "nested"} and "properties" in field_def:
                nested_generator = self.transform_mapping_to_generators(field_def)
                if field_type == "object":
                    generator_dict[field_name] = lambda f=field_def, ng=nested_generator: self._generate_obj(f, ng)
                else:  # nested
                    generator_dict[field_name] = lambda f=field_def, ng=nested_generator: self._generate_nested_array(f, ng)
                continue

            # Does not handle nested field overrides yet.
            if field_name in overrides:
                override = overrides[field_name]
                gen_name = override.get("generator")
                gen_func = getattr(self, gen_name, None)
                if gen_func:
                    params = override.get("params", {})
                    generator_dict[field_name] = lambda f=field_def, gen=gen_func, p=params: gen(f, **p)
                    continue

            # Use default generator config for this field type (if any)
            default_params = default_generators.get(field_type, {})
            generator_func = self.type_generators.get(field_type, lambda f, **_: "unknown_type")

            generator_dict[field_name] = lambda f=field_def, gen=generator_func, p=default_params: gen(f, **p)

        return generator_dict


    def _generate_obj(self, field_def: Dict[str, Any], nested_generators: Dict[str, Callable]) -> Dict[str, Any]:
        """Generate an object using nested generators"""
        result = {}
        for field_name, generator in nested_generators.items():
            result[field_name] = generator()
        return result

    def _generate_nested_array(self, field_def: Dict[str, Any], nested_generators: Dict[str, Callable], min_items=1, max_items=3) -> list:
        """Generate nested array of objects"""
        count = random.randint(min_items, max_items)
        result = []
        for _ in range(count):
            obj = {}
            for field_name, generator in nested_generators.items():
                obj[field_name] = generator()
            result.append(obj)
        return result

    def generate_fake_document(self, generator_dict: Dict[str, Callable]) -> Dict[str, Any]:
        """
        Generate a document using the generator functions

        Args:
            generator_dict: Dictionary of generator functions

        Returns:
            document containing lambdas that cna be invoked to generate data
        """
        document = {}
        for field_name, generator in generator_dict.items():
            document[field_name] = generator()

        return document


def load_mapping_and_config(mapping_file_path, config_path=None):
    """
    Creates a document generator from an OpenSearch mapping file.

    Args:
        mapping_file_path (str): Path to the index mappings JSON file
        config_path (str): Path to the YAML file specifying data ranges.

    Returns:
        mapping and config as dicts
    """
    with open(mapping_file_path, "r") as f:
        mapping_dict = json.load(f)

    config_dict = None
    if config_path:
        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)

    return mapping_dict, config_dict

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

def format_size(bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024
    return f"{bytes:.2f} PB"

def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)}m {int(seconds)}s"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

def setup_custom_tqdm_formatting(progress_bar):
    """Set up custom formatting for the tqdm progress bar."""
    progress_bar.format_dict['n_fmt'] = lambda n: format_size(n)
    progress_bar.format_dict['total_fmt'] = lambda t: format_size(t)
    progress_bar.format_dict['elapsed'] = lambda e: format_time(e)
    progress_bar.format_dict['remaining'] = lambda r: format_time(r)

def get_avg_document_size(index_mappings: dict, mapping_config: dict) -> int:
    document = [generate_test_document(index_mappings, mapping_config)]
    write_chunk(document, '/tmp/test-size.json')

    size = os.path.getsize('/tmp/test-size.json')
    os.remove('/tmp/test-size.json')

    return size

def generate_test_document(index_mappings: dict, mapping_config: dict) -> dict:
    mapping_generator = MappingSyntheticDataGenerator(mapping_config)
    converted_mappings = mapping_generator.transform_mapping_to_generators(index_mappings)

    return mapping_generator.generate_fake_document(generator_dict=converted_mappings)


class MappingSyntheticDataGeneratorWorker:
    @staticmethod
    def generate_documents_from_worker(index_mappings, mapping_config, chunk_size):
        """
        Within the scope of a Dask worker. Initially reconstructs the MappingSyntheticDataGenerator and generates documents.
        This is because Dask coordinator needs to serialize and deserialize objects when passing them to a worker.
        Generates the generate_fake_document, which gets invoked N number of times before returning a list of documents.

        param: mapping_dict (dict): The OpenSearch mapping dictionary.
        param: config_dict (dict): Optional YAML-based config for value constraints.
        param: num_docs (int): Number of documents to generate.

        Returns: List of generated documents.
        """
        # Initialize parameters given to worker
        mapping_generator = MappingSyntheticDataGenerator(mapping_config)
        mappings_with_generators = mapping_generator.transform_mapping_to_generators(index_mappings)

        documents = [mapping_generator.generate_fake_document(mappings_with_generators) for _ in range(chunk_size)]

        return documents


def generate_dataset_with_mappings(client: Client, sdg_config: SyntheticDataGeneratorConfig, index_mappings: dict, input_config: dict):
        """
        param: client: Dask client that performs multiprocessing and creates dashboard to visualize task streams
        param: sdg_config: SyntheticDataGenerationConfig instance that houses information related to data corpora to generate
        param: index_mappings: OpenSearch index mappings that user provided
        param: user_config: Optional config that specifies custom lists and custom data providers that the custom module uses to generate data.
            This also contains configuration details related to how data is generated (i.e. number of workers to use, max file size in GB, and number of documents in a chunk)

        returns: Does not return results but writes documents to output path
        """
        logger = logging.getLogger(__name__)

        # Fetch settings and input config that user provided
        generation_settings = get_generation_settings(input_config)
        print(generation_settings)
        mapping_config = input_config

        max_file_size_bytes = generation_settings.get('max_file_size_gb') * 1024 * 1024 * 1024
        total_size_bytes = sdg_config.total_size_gb * 1024 * 1024 * 1024
        chunk_size = generation_settings.get('chunk_size')  # Adjust this based on your memory constraints
        avg_document_size = get_avg_document_size(index_mappings, mapping_config)

        current_size = 0
        docs_written = 0
        file_counter = 0

        logger.info("Average document size: %s", avg_document_size)
        logger.info("Chunk size: %s docs", chunk_size)
        logger.info("Total GB to generate: %s", sdg_config.total_size_gb)
        logger.info("Max file size in GB: %s", generation_settings.get('max_file_size_gb'))

        console.println(f"Total GB to generate: {sdg_config.total_size_gb}\n"
                        f"Max file size in GB: {generation_settings.get('max_file_size_gb')}\n")

        start_time = time.time()
        with tqdm(total=total_size_bytes,
                  unit='B',
                  unit_scale=True,
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as progress_bar:

            setup_custom_tqdm_formatting(progress_bar)
            while current_size < total_size_bytes:
                file_path = os.path.join(sdg_config.output_path, f"{sdg_config.index_name}_{file_counter}.json")
                file_size = 0

                while file_size < max_file_size_bytes:  # 40GB make this configurable in the benchmark.ini or generation.ini or config.yml
                    generation_start_time = time.time()
                    seeds = generate_seeds_for_workers(regenerate=True)
                    logger.info("Using seeds: %s", seeds)

                    # Test if 40GB works by removing seed and just doing for _ in range(workers)
                    # with performance_report(filename="financial_mimesis_10GB.html"):
                    futures = [client.submit(MappingSyntheticDataGeneratorWorker.generate_documents_from_worker, index_mappings, mapping_config, chunk_size) for seed in seeds]
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
            total_time_to_generate_dataset = round(end_time - start_time)
            progress_bar.update(total_size_bytes - progress_bar.n)

            dataset_size = current_size
            logger.info("Generated %s docs in %s seconds. Total dataset size is %s GB", docs_written, total_time_to_generate_dataset, dataset_size)

            return docs_written, total_time_to_generate_dataset, dataset_size