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

from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


DOCS_COMPRESSOR = bz2.BZ2Compressor
COMP_EXT = ".bz2"
OUTPUT_EXT = ".json"

DEFAULT_CONCURRENT_THREADS=8
DEFAULT_DUMP_QUERY='{"match_all": {}}'


def template_vars(index_name, output_path, doc_count):
    comp_outpath = output_path + COMP_EXT
    return {
        "index_name": index_name,
        "filename": os.path.basename(comp_outpath),
        "path": comp_outpath,
        "doc_count": doc_count,
        "uncompressed_bytes": os.path.getsize(output_path),
        "compressed_bytes": os.path.getsize(comp_outpath)
    }


def get_doc_outpath(outdir, name, suffix=""):
    return os.path.join(outdir, f"{name}-documents{suffix}.json")


def extract(
        client,
        output_path,
        index,
        number_of_docs_requested=None,
        concurrent=False,
        threads=DEFAULT_CONCURRENT_THREADS,
        batch_size=0,
        custom_dump_query=DEFAULT_DUMP_QUERY
    ):
    """
    Scroll an index with a match-all query, dumping document source to ``outdir/documents.json``.

    :param client: OpenSearch client used to extract data
    :param output_path: Destination directory for corpus dump
    :param index: Name of index to dump
    :param numer_of_docs_requested: Specific number of docs requested instead of all (default: None)
    :param concurrent: Use threads to concurrently dump documents (default: false)
    :param threads: Number of threads to use if concurrent is set to true (default: 8)
    :param batch_size: Number of hits that should be returned in response (default: 0)
    :param custom_dump_query: Custom dump query used to collect and dump documents (default: match_all query)
    :return: dict of properties describing the corpus for templates
    """

    logger = logging.getLogger(__name__)

    number_of_docs = client.count(index=index)["count"]

    total_docs = number_of_docs if not number_of_docs_requested else min(number_of_docs, number_of_docs_requested)

    if total_docs > 0:
        logger.info("[%d] total docs in index [%s]. Extracting [%s] docs.", number_of_docs, index, total_docs)
        docs_path = get_doc_outpath(output_path, index)

        # Dump documents that are used for --test-mode flag
        dump_documents(
            concurrent,
            client,
            index,
            get_doc_outpath(output_path, index, "-1k"),
            min(total_docs, 1000),
            progress_message_suffix="for test mode",
            threads=threads,
            batch_size=batch_size,
            custom_dump_query=custom_dump_query,
        )

        # Dump documents that are used for full test
        dump_documents(
            concurrent,
            client,
            index,
            docs_path,
            total_docs,
            threads=threads,
            batch_size=batch_size,
            custom_dump_query=custom_dump_query,
        )
        return template_vars(index, docs_path, total_docs)
    else:
        logger.info("Skipping corpus extraction fo index [%s] as it contains no documents.", index)
        return None


def dump_documents_range(
    pbar,
    client,
    index,
    output_path,
    start_doc,
    end_doc,
    total_docs,
    batch_size=0,
    custom_dump_query=DEFAULT_DUMP_QUERY
    ):
    """
    Extract documents in the range of start_doc and end_doc and write to individual files

    :param client: OpenSearch client used to extract data
    :param index: Name of OpenSearch index to extract documents from
    :param output_path: Destination directory for corpus dump
    :param start_doc: Start index of the document chunk
    :param end_doc: End index of the document chunk
    :param total_docs: Total number of documents (default: All)
    :param batch_size: Number of hits that should be returned in response (default: 0)
    :param custom_dump_query: Custom dump query used to retrieve and dump documents. (default: match_all query)
    :return: dict of properties describing the corpus for templates
    """

    logger = logging.getLogger(__name__)

    compressor = DOCS_COMPRESSOR()
    output_path = f"{output_path}_{start_doc}_{end_doc}" + OUTPUT_EXT
    comp_outpath = output_path + COMP_EXT

    with open(output_path, "wb") as outfile:
        with open(comp_outpath, "wb") as comp_outfile:
            max_doc = total_docs if end_doc > total_docs else end_doc

            batch_size = batch_size if batch_size > 0 else (max_doc - start_doc) // 5
            if batch_size < 1:
                batch_size = 1
            search_after = None
            n = 0

            while n < max_doc - start_doc:
                if search_after:
                    query = {
                        "query": custom_dump_query,
                        "size": batch_size,
                        "sort": [{"_id": "asc"}],
                        "search_after": search_after,
                    }
                else:
                    query = {
                        "query": custom_dump_query,
                        "size": batch_size,
                        "sort": [{"_id": "asc"}],
                        "from": start_doc,
                    }

                response = client.search(index=index, body=query)
                hits = response["hits"]["hits"]

                if not hits:
                    break

                for doc in hits:
                    try:
                        search_after = doc["sort"]
                    except KeyError:
                        logger.info("Error in response format: %s", doc)
                    data = (
                        json.dumps(doc["_source"], separators=(",", ":")) + "\n"
                    ).encode("utf-8")

                    outfile.write(data)
                    comp_outfile.write(compressor.compress(data))

                    n += 1
                    pbar.update(1)
                    if n >= (max_doc - start_doc):
                        break

            comp_outfile.write(compressor.flush())

    logger.info("Finished dumping corpus for index [%s] to [%s].", index, output_path)


def dump_documents(
    concurrent,
    client,
    index,
    output_path,
    number_of_docs,
    progress_message_suffix="",
    threads=DEFAULT_CONCURRENT_THREADS,
    batch_size=0,
    custom_dump_query=DEFAULT_DUMP_QUERY,
    ):
    """
    Splits the dumping process into 8 threads by default (see benchmark.py argparse for default value)
    First, they split the documents into chunks to be dumped. Then, they are dumped as "{index}-documents{suffix}_{start}_{end}.json(.bz2)"
    Finally, they are all collated into their file "{output_path}-documents{suffix}.json(.bz2)" format.

    :param concurrent: Use threads to concurrently dump documents (default: false)
    :param client: OpenSearch client used to extract data
    :param index: Name of OpenSearch index to extract documents from
    :param output_path: Destination directory for corpus dump
    :param number_of_docs: Total number of documents
    :param threads: Number of threads for dumping documents from indices (default: 8)
    :param batch_size: Number of hits that should be returned in response (default: 0)
    :param custom_dump_query: Custom query used to retrieve and dump documents. (default: match_all query)
    """
    if concurrent:
        threads = int(threads)
        batch_size = int(batch_size)
        with tqdm(
            total=number_of_docs,
            desc=f"Extracting documents from {index}"
            + (f" [{progress_message_suffix}]" if progress_message_suffix else ""),
            unit="doc",
        ) as pbar:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                step = number_of_docs // threads
                ranges = [(i, i + step) for i in range(0, number_of_docs, step)]
                executor.map(
                    lambda args: dump_documents_range(
                        pbar,
                        client,
                        index,
                        output_path,
                        *args,
                        number_of_docs,
                        batch_size,
                        custom_dump_query,
                    ),
                    ranges,
                )
        merge_json_files(output_path, ranges)
    else:
        with tqdm(
            total=number_of_docs,
            desc=f"Extracting documents from {index}"
            + (f" [{progress_message_suffix}]" if progress_message_suffix else ""),
            unit="doc",
        ) as pbar:
            dump_documents_range(
                pbar,
                client,
                index,
                output_path,
                0,
                number_of_docs,
                number_of_docs,
            )
        merge_json_files(output_path, [(0, number_of_docs)])


def merge_json_files(output_path, ranges):
    logger = logging.getLogger(__name__)

    for extension in [OUTPUT_EXT, OUTPUT_EXT + COMP_EXT]:
        merged_file_path = f"{output_path}" + extension
        with open(merged_file_path, "wb") as merged_file:
            for start, end in ranges:
                file_path = f"{output_path}_{start}_{end}" + extension

                with open(file_path, "rb") as f:
                    for line in f:
                        merged_file.write(line)
                try:
                    os.remove(file_path)
                except:
                    logger.error(
                        "Create-Workload Feature: Merge Json Files Error encountered when removing file path [%s]",
                        file_path
                    )
                    pass

        logger.info("Finished merging shards into [%s].", merged_file_path)