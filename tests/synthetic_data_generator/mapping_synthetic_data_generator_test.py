from unittest.mock import patch, MagicMock, mock_open
import pytest

from osbenchmark.synthetic_data_generator.mapping_synthetic_data_generator import MappingSyntheticDataGenerator

class TestMappingSyntheticDataGenerator:

    @pytest.fixture
    def sample_basic_opensearch_mapping(self):
        return {
            "mappings": {
                "properties": {
                    "title": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "description": {
                        "type": "text"
                    },
                    "price": {
                        "type": "float"
                    },
                    "created_at": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis"
                    },
                    "is_available": {
                        "type": "boolean"
                    },
                    "category_id": {
                        "type": "integer"
                    },
                    "tags": {
                        "type": "keyword"
                    }
                }
            }
        }
    
    @pytest.fixture
    def sample_mapping_synthetic_data_generator_config(self):
        return {
            "MappingSyntheticDataGenerator": {
                "generator_overrides": {
                    "integer": {"min": 1, "max": 100},
                    "float": {"min": 0, "max": 10, "round": 1}
                }
            }
        }

    @pytest.fixture
    def mapping_synthetic_data_generator(self):
        return MappingSyntheticDataGenerator()
    
    def test_that_mapping_synthetic_data_generator_intializes_correctly(self, mapping_synthetic_data_generator):
        assert mapping_synthetic_data_generator is not None
        assert hasattr(mapping_synthetic_data_generator, 'type_generators')
        assert isinstance(mapping_synthetic_data_generator.type_generators, dict)
    
