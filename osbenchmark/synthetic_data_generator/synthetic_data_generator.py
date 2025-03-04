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

import dask
from dask.distributed import Client

from osbenchmark.utils import console
from osbenchmark.synthetic_data_generator.types import GeneratorTypes
from osbenchmark.synthetic_data_generator.input_processor import create_sdg_config_from_args

class SyntheticDataGenerator:
    @staticmethod
    def hydrate_blueprint_with_lazy_evaluation(blueprint: Dict[str, Any]) -> Dict[str, Any]:
        """
        A raw blueprint was created via the input files that user provided (index mappings, template document).
        This method converts that raw blueprint into a template that can be used to generate synthetic data.

        :param blueprint: raw blueprint that needs to be hydrated. Hydration refers to replacing instances
          of {'generator': 'DataGeneratorType', 'params': {}} with specific DataGenerators.

        :return: a dictionary with values containing tuples with a DataGenerator and params user provided
        """
        if isinstance(blueprint, dict):
            if SyntheticDataGenerator._is_value_a_data_generator(blueprint):
                data_generator = GeneratorTypes[blueprint["data_generator_type"].upper()]
                data_generator_params = blueprint["params"]

                # Recursively hydrate fields in nested or object mapping fields
                if blueprint["data_generator_type"].upper() in ["NESTED", "OBJECT"]:
                    data_generator_params["fields"] = SyntheticDataGenerator.hydrate_blueprint_with_lazy_evaluation(data_generator_params.get("fields", {}))

                # Insert tuple with DataGenerator and its params
                return (data_generator, data_generator_params)

            if SyntheticDataGenerator._is_value_static(blueprint):
                return blueprint["value"]

            return {key: SyntheticDataGenerator.hydrate_blueprint_with_lazy_evaluation(value) for key, value in blueprint.items()}

        elif isinstance(blueprint, list):
            return [SyntheticDataGenerator.hydrate_blueprint_with_lazy_evaluation(item) for item in blueprint]

        else:
            return blueprint

    @staticmethod
    def generate_data(hydrated_blueprint: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively traverses through hydrated blueprint.
        If encounters a DataGenerator, will invoke it to generate data.

        :param hydrated_blueprint: hydrated blueprint produced by hydrate_blueprint_with_lazy_evaluation() method

        :return: a dictionary representing a document with synthetic generated data
        """
        logger = logging.getLogger(__name__)
        if isinstance(hydrated_blueprint, dict):
            logger.info("Inside Generate Data Dict: %s", hydrated_blueprint)
            return {key: SyntheticDataGenerator.generate_data(value) for key, value in hydrated_blueprint.items()}

        elif isinstance(hydrated_blueprint, list):
            logger.info("Inside Generate Data List: %s", hydrated_blueprint)
            return [SyntheticDataGenerator.generate_data(item) for item in hydrated_blueprint]

        elif isinstance(hydrated_blueprint, tuple):
            # Generate data with discovered Data Generator Type
            data_generator_type = hydrated_blueprint[0]

            if isinstance(data_generator_type, GeneratorTypes):
                if data_generator_type in [GeneratorTypes.NESTED, GeneratorTypes.OBJECT]:
                    # For nested and object mapping field types, the second entry in the tuple is the fields for the object. Provide them to the NestedGenerator and ObjectGenerator.
                    object_fields = hydrated_blueprint[1]['fields']

                    return data_generator_type.generate(fields=object_fields)
                else:
                    # All other generator types, the second entry in the tuple is just the params. Provide these two all other GeneratorTypes
                    params = hydrated_blueprint[1]
                    return data_generator_type.generate(**params)
        else:
            # just return the value as is
            return hydrated_blueprint

    @staticmethod
    def generate_data_chunk(chunk_size: int, hydrated_blueprint: Any) -> List[Any]:
        """
        Generates a chunk of data using the hydrated blueprint

        :param chunk_size: number of docs in a chunk to generate. By default, is 1000. Should adjust based on RAM size
        :param hydrated_blueprint: hydrated blueprint produced by hydrate_blueprint_with_lazy_evaluation() method

        :return: a list of dictionaries. This list represents a chunk ofsynthetically generated documents.
        """
        return [SyntheticDataGenerator.generate_data(hydrated_blueprint) for _ in range(chunk_size)]

    @staticmethod
    def _is_value_a_data_generator(blueprint):
        return True if "data_generator_type" in blueprint and "params" in blueprint else False

    @staticmethod
    def _is_value_static(blueprint):
        return True if "type" in blueprint and blueprint["type"] == "STATIC" else False

def user_confirmed_accurate_setup(sdg_config):
    console.println("Synthetic Data Generation Config: ", sdg_config.index_name)
    console.println("Generation Blue Print: ", json.dumps(sdg_config.blueprint, indent=2))

    user_confirmed = input(f"Synthetic Data Generation Blueprints for {[sdg_config.index_name]}. Continue? [y/n]: ")
    valid_responses = ['y', 'n']
    while user_confirmed not in valid_responses:
        user_confirmed = input(f"Synthetic Data Generation Blueprints for {[sdg_config.index_name]}. Continue? [y/n]: ")

    return True if user_confirmed == 'y' else False

def get_cores_in_lg_host():
    return multiprocessing.cpu_count()

def orchestrate_data_generation(cfg):
    logger = logging.getLogger(__name__)
    sdg_config = create_sdg_config_from_args(cfg)

    if user_confirmed_accurate_setup(sdg_config):
        user_hydrated_blueprint = SyntheticDataGenerator.hydrate_blueprint_with_lazy_evaluation(sdg_config.blueprint)
        print(user_hydrated_blueprint)
        # print(json.dumps(user_hydrated_blueprint, indent=2, default=str))

        chunk_size = 1000
        total_size = sdg_config.total_size_gb * 1024 * 1024 * 1024
        current_size = 0
        file_counter = 0
        workers = get_cores_in_lg_host()
        dask_client = Client(n_workers=12, threads_per_worker=1)

        logger.info("Generating!")

        results = SyntheticDataGenerator.generate_data_chunk(2, user_hydrated_blueprint)

        for result in results:
            print(json.dumps(result, indent=2))

    else:
        console.println("User has cancelled synthetic data generation.")

