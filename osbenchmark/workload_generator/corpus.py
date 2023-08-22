# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import bz2
import json
import logging
import os

from osbenchmark.utils import console

from time import sleep
from tqdm import tqdm


DOCS_COMPRESSOR = bz2.BZ2Compressor
COMP_EXT = ".bz2"


def template_vars(index_name, out_path, doc_count):
    comp_outpath = out_path + COMP_EXT
    return {
        "index_name": index_name,
        "filename": os.path.basename(comp_outpath),
        "path": comp_outpath,
        "doc_count": doc_count,
        "uncompressed_bytes": os.path.getsize(out_path),
        "compressed_bytes": os.path.getsize(comp_outpath)
    }


def get_doc_outpath(outdir, name, suffix=""):
    return os.path.join(outdir, f"{name}-documents{suffix}.json")


def extract(client, output_path, index, number_of_docs_requested=None, multiple=None):
    """
    Scroll an index with a match-all query, dumping document source to ``outdir/documents.json``.

    :param client: OpenSearch client used to extract data
    :param output_path: Destination directory for corpus dump
    :param index: Name of index to dump
    :return: dict of properties describing the corpus for templates
    """

    logger = logging.getLogger(__name__)

    number_of_docs_in_index = client.count(index=index)["count"]

    # Total number of docs to extract should be the smaller value when comparing number_of_docs_in_index and number_of_docs_requested

    if multiple and multiple > 1:
        return multiples_extraction(number_of_docs_in_index, output_path, index, client, multiple)
    else:
        total_docs = number_of_docs_in_index if not number_of_docs_requested else min(number_of_docs_in_index, number_of_docs_requested)
        return standard_extraction(number_of_docs_in_index, total_docs, output_path, index, client)

def multiples_extraction(number_of_docs_in_index, output_path, index, client, multiple):
    logger = logging.getLogger(__name__)

    if number_of_docs_in_index > 0:
        logger.info("[%d] total docs in index [%s]. Extracting [%s] docs with multiple [%s]", number_of_docs_in_index, index, number_of_docs_in_index, multiple)

        docs_path = get_doc_outpath(output_path, index)
        dump_documents_for_test_mode(client, index, get_doc_outpath(output_path, index, "-1k"), number_of_docs_in_index)

        num_of_extracted_docs = dump_documents_with_multiple(client, index, docs_path, number_of_docs_in_index, multiple)
        amount_of_docs_to_extract = (number_of_docs_in_index // multiple)

        return template_vars(index, docs_path, amount_of_docs_to_extract)

    else:
        logger.info("Skipping corpus extraction for index [%s] as it contains no documents.", index)
        return None

def standard_extraction(number_of_docs_in_index, total_docs, output_path, index, client):
    """
    Scroll an index with a match-all query, dumping document source to ``outdir/documents.json``.

    :param client: OpenSearch client used to extract data
    :param output_path: Destination directory for corpus dump
    :param index: Name of index to dump
    :return: dict of properties describing the corpus for templates
    """

    logger = logging.getLogger(__name__)

    if total_docs > 0:
        logger.info("[%d] total docs in index [%s]. Extracting [%s] docs.", number_of_docs_in_index, index, total_docs)
        docs_path = get_doc_outpath(output_path, index)
        dump_documents(client, index, get_doc_outpath(output_path, index, "-1k"), min(total_docs, 1000), " for test mode")
        dump_documents(client, index, docs_path, total_docs)
        return template_vars(index, docs_path, total_docs)
    else:
        logger.info("Skipping corpus extraction for index [%s] as it contains no documents.", index)
        return None

def dump_documents_for_test_mode(client, index, out_path, total_docs):
    # Gets the first 1000 docs in index
    progress_message_suffix = " for test mode"
    number_of_docs = min(total_docs, 1000)

    # pylint: disable=import-outside-toplevel
    from opensearchpy import helpers

    logger = logging.getLogger(__name__)
    freq = max(1, number_of_docs // 1000)
    logger.info("Number of docs: [%s], freq: [%s]", number_of_docs, freq)

    progress = console.progress()
    compressor = DOCS_COMPRESSOR()
    comp_outpath = out_path + COMP_EXT

    with open(out_path, "wb") as outfile:
        with open(comp_outpath, "wb") as comp_outfile:

            logger.info("Dumping corpus for index [%s] to [%s].", index, out_path)
            query = {"query": {"match_all": {}}}
            for n, doc in enumerate(helpers.scan(client, query=query, index=index)):
                if n >= number_of_docs:
                    break

                logger.info("Nth doc [%s], doc contents: [%s]", n, doc)
                data = (json.dumps(doc["_source"], separators=(",", ":")) + "\n").encode("utf-8")

                outfile.write(data)
                comp_outfile.write(compressor.compress(data))

                render_progress(progress, progress_message_suffix, index, n + 1, number_of_docs, freq)

            comp_outfile.write(compressor.flush())
    progress.finish()

def dump_documents(client, index, out_path, number_of_docs, progress_message_suffix=""):
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

            for n, doc in enumerate(helpers.scan(client, query=query, index=index)):
                if n >= number_of_docs:
                    break
                data = (json.dumps(doc["_source"], separators=(",", ":")) + "\n").encode("utf-8")

                outfile.write(data)
                comp_outfile.write(compressor.compress(data))

                render_progress(progress, progress_message_suffix, index, n + 1, number_of_docs, freq)

            comp_outfile.write(compressor.flush())
    progress.finish()

def dump_documents_with_multiple(client, index, out_path, number_of_docs_in_index, multiple):
    number_of_docs_to_fetch = number_of_docs_in_index // multiple
    number_of_docs_left = number_of_docs_to_fetch

    progress_message = f"Extracting documents for index [{index}] with multiple of {multiple}"

    # pylint: disable=import-outside-toplevel
    from opensearchpy import helpers

    logger = logging.getLogger(__name__)
    logger.info("Number of docs in index: [%s], number of docs to fetch: [%s]", number_of_docs_in_index, number_of_docs_to_fetch)

    freq = 100 / number_of_docs_to_fetch
    logger.info("Multiple: [%s]", multiple)

    compressor = DOCS_COMPRESSOR()
    comp_outpath = out_path + COMP_EXT

    with open(out_path, "wb") as outfile:
        with open(comp_outpath, "wb") as comp_outfile:
            logger.info("Dumping corpus for index [%s] to [%s].", index, out_path)
            query = {"query": {"match_all": {}}}

            progress_bar = tqdm(range(number_of_docs_to_fetch), desc=progress_message, ascii=' >=', bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}')

            # bytes_bar = tqdm(unit = 'B', ascii = True, unit_scale = True)
            for n, doc in enumerate(helpers.scan(client, query=query, index=index), start=1):
                if (n % multiple) != 0:
                    continue

                if number_of_docs_left == 0:
                    break

                number_of_docs_left -= 1

                logger.info("Nth doc [%s], doc contents: [%s]", n, doc)
                data = (json.dumps(doc["_source"], separators=(",", ":")) + "\n").encode("utf-8")

                outfile.write(data)
                comp_outfile.write(compressor.compress(data))
                sleep(0.1)
                progress_bar.update(1)


            comp_outfile.write(compressor.flush())




def render_progress(progress, progress_message_suffix, index, cur, total, freq):
    if cur % freq == 0 or total - cur < freq:
        msg = f"Extracting documents for index [{index}]{progress_message_suffix}..."
        percent = (cur * 100) / total
        progress.print(msg, f"{cur}/{total} docs [{percent:.1f}% done]")