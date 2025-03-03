# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
import yaml

from osbenchmark.utils import io, opts, console

from osbenchmark.synthetic_data_generator.types import SyntheticDataGeneratorConfig, Modes
from osbenchmark.synthetic_data_generator.parsers import MappingParser, TemplateDocumentParser
from osbenchmark.exceptions import ConfigError

def create_sdg_config_from_args(cfg) -> SyntheticDataGeneratorConfig:
    """
    Creates a Synthetic Data Generator Config based on the user's inputs

    :param cfg: Contains the command line configuration

    :return: A dataclass that contains configuration and information user provided
    """
    try:
        index_mappings_path = cfg.opts("synthetic_data_generator", "index_mappings")
        template_document_path = cfg.opts("synthetic_data_generator", "template_document")
        raw_blueprint = process_input_file_into_raw_blueprint(index_mappings_path, template_document_path)

        return SyntheticDataGeneratorConfig(
            index_name = cfg.opts("synthetic_data_generator", "index_name"),
            index_mappings_path = index_mappings_path,
            template_document_path = template_document_path,
            total_documents = cfg.opts("synthetic_data_generator", "total_documents"),
            total_size_gb= cfg.opts("synthetic_data_generator", "total_size"),
            mode = cfg.opts("synthetic_data_generator", "mode"),
            output_path = cfg.opts("synthetic_data_generator", "output_path"),
            blueprint = raw_blueprint
        )

    except ConfigError as e:
        raise ConfigError("Config error when building SyntheticDataGeneratorConfig: ", e)

def process_input_file_into_raw_blueprint(index_mappings_path, template_document_path):
    """
    Processes either the index mappings or template document that user provides.
    Produces a raw blueprint that the Synthetic Data Generator will use to generate documents.

    :param index_mappings_path: Path where index mappings file is located
    :param template_document_path: Path where template document is located

    :return: a dictionary, which can be considered the raw blueprint
    """
    if index_mappings_path:
        processed_input_file = process_index_mappings(index_mappings_path)
    elif template_document_path:
        processed_input_file = process_template_document(template_document_path)

    return processed_input_file

def process_index_mappings(index_mappings) -> dict:
    return MappingParser.parse_from_file(index_mappings)

def process_template_document(template_document) -> dict:
    return TemplateDocumentParser.parse_from_file(template_document)
