# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import bz2
import json
import logging
import os
from abc import ABC, abstractmethod

import opensearchpy.exceptions

from osbenchmark import exceptions
from osbenchmark.utils import console
from osbenchmark.workload_generator.config import CustomWorkload

DOCS_COMPRESSOR = bz2.BZ2Compressor
COMP_EXT = ".bz2"

class IndexExtractor:
    def __init__(self, custom_workload, client):
        self.custom_workload: CustomWorkload = custom_workload
        self.client = client

        self.INDEX_SETTINGS_EPHEMERAL_KEYS = ["uuid",
                                        "creation_date",
                                        "version",
                                        "provided_name",
                                        "store"]
        self.INDEX_SETTINGS_PARAMETERS = {
            "number_of_replicas": "{{{{number_of_replicas | default({orig})}}}}",
            "number_of_shards": "{{{{number_of_shards | default({orig})}}}}"
        }
        self.logger = logging.getLogger(__name__)

    def extract_indices(self, workload_path):
        extracted_indices, failed_indices = [], []
        try:
            for index in self.custom_workload.indices:
                extracted_indices += self.extract(workload_path, index.name)
        except opensearchpy.exceptions.NotFoundError as e:
            raise exceptions.SystemSetupError(f"Index {index.name} does not exist.")
        except opensearchpy.OpenSearchException:
            self.logger.error("Failed at extracting index [%s]", index)
            failed_indices += index

        return extracted_indices, failed_indices

    def extract(self, outdir, index_pattern):
        """
        Extracts and writes index settings and
        mappings to "<index_name>.json" in a workload
        :param outdir: destination directory
        :param index_pattern: name of index or index pattern
        :return: Dictionary of template variables corresponding to the
        specified index / indices
        """
        results = []

        index_obj = self.extract_index_mapping_and_settings(index_pattern)
        for index, details in index_obj.items():
            filename = f"{index}.json"
            outpath = os.path.join(outdir, filename)
            with open(outpath, "w") as outfile:
                json.dump(details, outfile, indent=4, sort_keys=True)
                outfile.write("\n")
            results.append({
                "name": index,
                "path": outpath,
                "filename": filename,
            })
        return results

    def extract_index_mapping_and_settings(self, index_pattern):
        """
        Uses client to retrieve mapping + settings, filtering settings
        related to index / indices. They will be used to re-create
        index / indices
        :param index_pattern: name of index or index pattern
        :return: dictionary of index / indices mappings and settings
        """
        results = {}
        logger = logging.getLogger(__name__)
        # the response might contain multiple indices if a wildcard was provided
        response = self.client.indices.get(index_pattern)
        for index, details in response.items():
            valid, reason = self.is_valid_index(index)
            if valid:
                mappings = details["mappings"]
                index_settings = self.filter_ephemeral_index_settings(details["settings"]["index"])
                self.update_index_setting_parameters(index_settings)
                results[index] = {
                    "mappings": mappings,
                    "settings": {
                        "index": index_settings
                    }
                }
            else:
                logger.info("Skipping index [%s] (reason: %s).", index, reason)

        return results

    def filter_ephemeral_index_settings(self, settings):
        """
        Some of the 'settings' (like uuid, creation-date, etc.)
        published by OpenSearch for an index are
        ephemeral values, not useful for re-creating the index.
        :param settings: Index settings published by index.get()
        :return: settings with ephemeral keys removed
        """
        filtered = dict(settings)
        for s in self.INDEX_SETTINGS_EPHEMERAL_KEYS:
            filtered.pop(s, None)
        return filtered


    def update_index_setting_parameters(self, settings):
        for s, param in self.INDEX_SETTINGS_PARAMETERS.items():
            if s in settings:
                orig_value = settings[s]
                settings[s] = param.format(orig=orig_value)


    def is_valid_index(self, index_name):
        if len(index_name) == 0:
            return False, "Index name is empty"
        if index_name.startswith("."):
            return False, f"Index [{index_name}] is hidden"
        return True, None


class CorpusExtractor(ABC):

    @abstractmethod
    def extract_documents(self, index, documents_limit=None):
        pass


class SequentialCorpusExtractor(CorpusExtractor):
    DEFAULT_TEST_MODE_DOC_COUNT = 1000
    DEFAULT_TEST_MODE_SUFFIX = "-1k"

    def __init__(self, custom_workload, client):
        self.custom_workload: CustomWorkload = custom_workload
        self.client = client

    def template_vars(self,index_name, out_path, doc_count):
        comp_outpath = out_path + COMP_EXT
        return {
            "index_name": index_name,
            "filename": os.path.basename(comp_outpath),
            "path": comp_outpath,
            "doc_count": doc_count,
            "uncompressed_bytes": os.path.getsize(out_path),
            "compressed_bytes": os.path.getsize(comp_outpath)
        }

    def _get_doc_outpath(self, outdir, name, suffix=""):
        return os.path.join(outdir, f"{name}-documents{suffix}.json")


    def extract_documents(self, index, documents_limit=None):
        """
        Scroll an index with a match-all query, dumping document source to ``outdir/documents.json``.

        :param index: Name of index to dump
        :param documents_limit: The number of documents to extract. Must be equal
        to or less than the total number of documents that exists in the index
        :return: dict of properties describing the corpus for templates
        """

        logger = logging.getLogger(__name__)

        total_documents = self.client.count(index=index)["count"]

        logger.info("total documents: %s, documents limit: %s", total_documents, documents_limit)

        documents_to_extract = total_documents if not documents_limit else min(total_documents, documents_limit)

        if documents_limit:
            # Only time when documents-1k.json will be less than 1K documents is
            # when the documents_limit is < 1k documents or source index has less than 1k documents
            if documents_limit < self.DEFAULT_TEST_MODE_DOC_COUNT:
                test_mode_warning_msg = f"Due to --number-of-docs set by user, " + \
                    f"test-mode docs will be less than the default {self.DEFAULT_TEST_MODE_DOC_COUNT} documents."
                console.warn(test_mode_warning_msg)

            # Notify users when they specified more documents than available in index
            if documents_limit > total_documents:
                documents_to_extract_msg = f"User requested to extract {documents_limit} documents " + \
                    f"but there are only {total_documents} documents in {index}. " + \
                    f"Will only extract {total_documents} documents from {index}."
                console.warn(documents_to_extract_msg)

        if documents_to_extract > 0:
            logger.info("[%d] total docs in index [%s]. Extracting [%s] docs.", total_documents, index, documents_to_extract)
            docs_path = self._get_doc_outpath(self.custom_workload.workload_path, index)
            # Create test mode corpora
            self.dump_documents(
                self.client,
                index,
                self._get_doc_outpath(self.custom_workload.workload_path, index, self.DEFAULT_TEST_MODE_SUFFIX),
                min(documents_to_extract, self.DEFAULT_TEST_MODE_DOC_COUNT),
                " for test mode")
            # Create full corpora
            self.dump_documents(self.client, index, docs_path, documents_to_extract)

            return self.template_vars(index, docs_path, documents_to_extract)
        else:
            logger.info("Skipping corpus extraction fo index [%s] as it contains no documents.", index)
            return None


    def dump_documents(self, client, index, out_path, number_of_docs, progress_message_suffix=""):
        # pylint: disable=import-outside-toplevel
        from opensearchpy import helpers

        logger = logging.getLogger(__name__)
        freq = max(1, number_of_docs // 1000)

        progress = console.progress()
        compressor = DOCS_COMPRESSOR()
        comp_outpath = out_path + COMP_EXT
        with open(out_path, "wb") as outfile:
            with open(comp_outpath, "wb") as comp_outfile:
                logger.info("Dumping corpus for index [%s] to [%s].", index, out_path)
                query = {"query": {"match_all": {}}}
                for i, doc in enumerate(helpers.scan(client, query=query, index=index)):
                    if i >= number_of_docs:
                        break
                    data = (json.dumps(doc["_source"], separators=(",", ":")) + "\n").encode("utf-8")

                    outfile.write(data)
                    comp_outfile.write(compressor.compress(data))

                    self.render_progress(progress, progress_message_suffix, index, i + 1, number_of_docs, freq)

                comp_outfile.write(compressor.flush())
        progress.finish()


    def render_progress(self, progress, progress_message_suffix, index, cur, total, freq):
        if cur % freq == 0 or total - cur < freq:
            msg = f"Extracting documents for index [{index}]{progress_message_suffix}..."
            percent = (cur * 100) / total
            progress.print(msg, f"{cur}/{total} docs [{percent:.1f}% done]")
