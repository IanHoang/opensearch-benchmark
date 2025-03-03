# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import re
import json

from osbenchmark.synthetic_data_generator.types import TYPE_MAPPING

class TemplateDocumentParser:
    """
    TemplateDocuments come with template data generators, which are custom or built-in OSB functions that generate data.
    In the template document, they are usually denoted by {{}} brackets
    The methods in this class work together to parse the template document and parse template data generators

    Update: This must be in the format {{<DATA_GENERATOR_NAME>()}}
    For example, to use the integer data generator in OSB, specify {{INTEGER()}} in the template document.
    Notice how there are no spaces in between the brackets and INTEGER. Also note how there are parenthesis,
    which usually houses params.
    """
    @staticmethod
    def parse_from_file(file_path):
        with open(file_path, 'r') as file:
            template_data = json.load(file)

        return TemplateDocumentParser.parse(template_data)

    @staticmethod
    def parse(template_document):
        """
        Parse template document containing template data generators. Some users might not populate values in all fields.
        Therefore, we note that fields without template data generators should keep the static value from the template document.
        """
        parsed_template = {}
        for field, value in template_document.items():
            if isinstance(value, str) and value.startswith('{{') and value.endswith('}}'):
                parsed_template[field] = TemplateDocumentParser._parse_template_data_generator(value)
            else:
                parsed_template[field] = {"type": "STATIC", "value": value}
        return parsed_template

    @staticmethod
    def _parse_template_data_generator(template_data_generator):
        match = re.match(r'{{(\w+)\((.*?)\)}}', template_data_generator)
        if match:
            data_generator_type = match.group(1)
            params_str = match.group(2)
            params = TemplateDocumentParser._parse_params(params_str)
            return {"data_generator_type": data_generator_type, "params": params}
        else:
            raise ValueError(f"Invalid template_data_generator: {template_data_generator}")

    @staticmethod
    def _parse_params(params_str):
        params = {}
        if params_str:
            for param in params_str.split(','):
                key, value = param.split('=')
                params[key.strip()] = TemplateDocumentParser._parse_value(value.strip())
        return params

    @staticmethod
    def _parse_value(value):
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                if value.lower() == 'true':
                    return True
                elif value.lower() == 'false':
                    return False
                else:
                    return value

class MappingParser:
    @staticmethod
    def parse_from_file(file_path):
        """
        Used to parse index mappings

        :param file_path: Path to index mappings

        :return: Parsed dictionary
        """
        with open(file_path, 'r') as file:
            mapping_data = json.load(file)

        return MappingParser.parse(mapping_data)

    @staticmethod
    def parse(input_data):
        if 'properties' in input_data:
            return MappingParser._parse_mapping(input_data['properties'])
        elif 'index_patterns' in input_data:
            return MappingParser._parse_template(input_data)
        else:
            raise ValueError("Invalid input: neither mapping nor template structure recognized")

    @staticmethod
    def _parse_mapping(properties):
        parsed_mapping = {}
        for field_name, field_info in properties.items():
            if 'properties' in field_info:
                parsed_mapping[field_name] = MappingParser._parse_mapping(field_info["properties"])
            else:
                field_type = MappingParser._get_field_type(field_info["type"])

                parsed_mapping[field_name] = {
                    "data_generator_type": field_type,
                    "params": {}
                }
        return parsed_mapping

    @staticmethod
    def _parse_template(template):
        if "mappings" in template:
            return MappingParser._parse_mapping(template["mappings"]["properties"])

        else:
            raise ValueError("Template does not contain mappings")

    @staticmethod
    def _get_field_type(opensearch_type):
        try:
            return TYPE_MAPPING.get(opensearch_type)

        except KeyError as e:
            raise KeyError(f"Unable to identify mapping field type {opensearch_type}. "
                           f"Ensure that this type is a supported OpenSearch mapping field type: "
                           f"https://opensearch.org/docs/latest/field-types/supported-field-types/index/")

