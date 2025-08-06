# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
import logging
import pandas as pd
import numpy as np
import math
from collections import deque
from typing import Generator
import time

import osbenchmark.exceptions as exceptions

class TimeSeriesPartitioner:

    # Change this into a dictionary that points to which frequencies can have which formats
    # TODO: Add epoch timestamp in ms and s
    VALID_DATETIMESTAMPS_FORMATS = [
        "%Y-%m-%d",                 # 2023-05-20
        "%Y-%m-%dT%H:%M:%S",        # 2023-05-20T15:30:45
        "%Y-%m-%dT%H:%M:%S.%f",     # 2023-05-20T15:30:45.123456
        "%Y-%m-%d %H:%M:%S",        # 2023-05-20 15:30:45
        "%Y-%m-%d %H:%M:%S.%f",     # 2023-05-20 15:30:45.123456
        "%d/%m/%Y",                 # 20/05/2023
        "%m/%d/%Y",                 # 05/20/2023
        "%d-%m-%Y",                 # 20-05-2023
        "%m-%d-%Y",                 # 05-20-2023
        "%d.%m.%Y",                 # 20.05.2023
        "%Y%m%d",                   # 20230520
        "%B %d, %Y",               # May 20, 2023
        "%b %d, %Y",              # May 20, 2023
        "%d %B %Y",               # 20 May 2023
        "%d %b %Y",               # 20 May 2023
        "%Y %B %d",               # 2023 May 20
        "%d/%m/%Y %H:%M",         # 20/05/2023 15:30
        "%d/%m/%Y %H:%M:%S",      # 20/05/2023 15:30:45
        "%Y-%m-%d %I:%M %p",      # 2023-05-20 03:30 PM
        "%d.%m.%Y %H:%M",         # 20.05.2023 15:30
        "%H:%M",                  # 15:30
        "%H:%M:%S",               # 15:30:45
        "%I:%M %p",              # 03:30 PM
        "%I:%M:%S %p",           # 03:30:45 PM
        "%a, %d %b %Y %H:%M:%S", # Sat, 20 May 2023 15:30:45
        "%Y/%m/%d",              # 2023/05/20
        "%Y/%m/%d %H:%M:%S",     # 2023/05/20 15:30:45
        "%Y%m%d%H%M%S",          # 20230520153045
        "epoch_s",
        "epoch_ms"
    ]

    AVAILABLE_FREQUENCIES = ['B', 'C', 'D', 'h', 'bh', 'cbh', 'min', 's', 'ms']

    def __init__(self, timeseries_enabled: dict, workers: int, docs_per_chunk: int, avg_document_size: int, total_size_bytes: int):
        self.timeseries_enabled = timeseries_enabled
        self.workers = workers
        self.docs_per_chunk = docs_per_chunk
        self.avg_document_size = avg_document_size
        self.total_size_bytes = total_size_bytes

        # self.timestamp_field = self.timeseries_enabled['timeseries_field']
        self.start_date = self.timeseries_enabled.get('timeseries_start_date', "1/1/2019")
        self.end_date = self.timeseries_enabled.get('timeseries_end_date', "12/31/2019")
        self.frequency = self.timeseries_enabled.get('timeseries_frequency', 'min')
        self.format = self.timeseries_enabled.get('timeseries_format', "%Y-%m-%dT%H:%M:%S") # Default to ISO 8601 Format
        self.logger = logging.getLogger(__name__)

        if self.frequency not in TimeSeriesPartitioner.AVAILABLE_FREQUENCIES:
            msg = f"Frequency {self.frequency} not found in available frequencies {TimeSeriesPartitioner.AVAILABLE_FREQUENCIES}"
            raise exceptions.ConfigError(msg)

        if self.format not in TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS:
            msg = f"Format {self.format} not found in available format {TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS}"
            raise exceptions.ConfigError(msg)

    def generate_windows(self) -> Generator:
        '''
        returns: a list of timestamp pairs where each timestamp pair is a set containing start datetime and end datetime
        '''
        # TODO: Give option to users to use a smaller frequency if it's better.
        # TODO: Get enough frequencies to where it's
        # Determine optimal time settings
        # Check if number of docs generated will fit in the timestamp. Adjust frequency as needed
        expected_number_of_docs = self.total_size_bytes // self.avg_document_size
        expected_number_of_docs_with_buffer = math.ceil((expected_number_of_docs * 0.1) + expected_number_of_docs)

        # Get number of timestamps with dates and frequencies
        datetimeindex = pd.date_range(self.start_date, self.end_date, freq=self.frequency)
        number_of_timestamps = len(datetimeindex)

        if number_of_timestamps < expected_number_of_docs_with_buffer:
            self.logger.info("Number of timestamps generated is less than expected docs generated. Trying to find the optimal frequency")
            if self.frequency == 'ms':
                self.logger.error("No other time frequencies to try. Not enough timestamps to generate for docs. Please expand dates and frequency accordingly.")
                raise exceptions.ConfigError("No other time frequencies to try. Not enough timestamps to generate for docs. Please expand dates and frequency accordingly.")

            frequencies_to_try = deque(TimeSeriesPartitioner.AVAILABLE_FREQUENCIES[TimeSeriesPartitioner.AVAILABLE_FREQUENCIES.index(self.frequency)+1:])
            print(frequencies_to_try)
            frequency = ""
            while frequencies_to_try:
                frequency = frequencies_to_try.popleft()
                # TODO: Add opportunity to split this up if too large for memory
                datetimeindex = pd.date_range(self.start_date, self.end_date, freq=frequency)
                number_of_timestamps = len(datetimeindex)
                if number_of_timestamps > expected_number_of_docs_with_buffer:
                    self.logger.info("Using [%s] frequency as this resulted in more timestamps", frequency)
                    break
                else:
                    self.logger.info("Using [%s] frequency did not result in more timestamps", frequency)

            #TODO: Update the timeseries enabled settings too so downstream isn't confused
            self.frequency = frequency
            self.logger.info("Updated frequency to use [%s]", self.frequency)
            # print(f"Using frequeny: {self.frequency}")
            # print(f"Length of timestamps: {number_of_timestamps}")
            # print(f"Expected documents: {expected_number_of_docs}")
            # print(f"Expected documents with buffer: {expected_number_of_docs_with_buffer}")

        # Get total number of docs each worker is expected to make and expected rounds per worker
        total_docs_made_by_each_worker = math.ceil(expected_number_of_docs / self.workers)
        expected_rounds_per_worker = math.ceil(total_docs_made_by_each_worker / self.docs_per_chunk)
        total_time_windows_needed = expected_rounds_per_worker * self.workers
        time_window_length = self.docs_per_chunk
        # print(f"Total docs made by each worker: {total_docs_made_by_each_worker}")
        # print(f"Expected rounds per worker: {expected_rounds_per_worker}")
        # print(f"Total time windows needed: {total_time_windows_needed}")
        # print(f"Time window length: {time_window_length}")
        # print("")

        start_time = time.time()
        datetimestamps = datetimeindex.values # get np array
        start_indices = np.arange(0, len(datetimestamps), time_window_length)
        end_indices = np.minimum(start_indices + time_window_length -1, len(datetimestamps) - 1)

        start_times = pd.to_datetime(datetimestamps[start_indices])
        end_times = pd.to_datetime(datetimestamps[end_indices])
        datetime_windows = list(zip(start_times, end_times))
        end_time = time.time()

        # TODO: This is correct. It generates the number of timestamp windows needed for the available timestamps, not the number that are needed
        # Find a way to adjust this list to be in order and slim it down to ones needed
        print(f"Total time it took to generate timestamp window pairs {len(datetime_windows)}: {end_time - start_time} secs")
        # Check the pair and see if they generated 10,000 timestamps
        first_pair = datetime_windows[0]
        first_pair_start_time = first_pair[0]
        first_pair_end_time = first_pair[1]
        length_of_first_pair = len(pd.date_range(first_pair_start_time, first_pair_end_time, freq=self.frequency))
        if length_of_first_pair != self.docs_per_chunk:
            msg = f"Length of first datetimestamps pair [{length_of_first_pair}] generated did not match the chunk size {self.docs_per_chunk}"
            self.logger.error(msg)
            raise exceptions.SystemSetupError(msg)
        else:
            self.logger.info("First datetimestamps pair: [%s]", first_pair)
            self.logger.info("Length of first pair: [%s]", length_of_first_pair)

            return iter(datetime_windows)

    @staticmethod
    def generate_datetimestamps_from_window(window: set, frequency: str = "min", format: str = "%Y-%m-%dT%H:%M:%S") -> Generator:
        logger = logging.getLogger(__name__)
        if frequency not in TimeSeriesPartitioner.AVAILABLE_FREQUENCIES:
            msg = f"Frequency {frequency} not found in available frequencies {TimeSeriesPartitioner.AVAILABLE_FREQUENCIES}"
            raise exceptions.ConfigError(msg)

        if format not in TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS:
            msg = f"Format {format} not found in available format {TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS}"
            raise exceptions.ConfigError(msg)

        try:
            start_datetimestamp = window[0]
            end_datetimestamp = window[1]
            generated_datetimestamps: pd.DatetimeIndex = pd.date_range(start_datetimestamp, end_datetimestamp, freq=frequency)

            #TODO: Handle formatting after generating iterator?
            if format and format in TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS:
                if format == "epoch_s":
                    generated_datetimestamps = generated_datetimestamps.map(lambda x: int(x.timestamp()))
                elif format == "epoch_ms":
                    generated_datetimestamps = generated_datetimestamps.map(lambda x: int(x.timestamp() * 1000))
                else:
                    generated_datetimestamps = generated_datetimestamps.strftime(date_format=format)

            return iter(generated_datetimestamps)

        except IndexError:
            raise exceptions.SystemSetupError("IndexError encountered with accessing datetimestamp from window.")
        except Exception:
            raise exceptions.SystemSetupError("Unknown error encountered with generating datetimestamps from window.")

    @staticmethod
    def sort_results_by_datetimestamps(results: list, timeseries_field: str) -> list:
        logger = logging.getLogger(__name__)
        logger.info("Length of results: %s", len(results))
        logger.info("Docs in each result: %s ", [len(result) for result in results])


        start_time = time.time()
        sorted_results = sorted(results, key=lambda chunk: chunk[0][timeseries_field])
        end_time = time.time()
        logger.info("Time it took to sort: %s secs", end_time-start_time)
        logger.info("First timestamp from all chunks: %s ", [result[0][timeseries_field] for result in sorted_results])

        return sorted_results



if __name__ == "__main__":
    # timeseries_partitioner = TimeSeriesPartitioner({}, 10, 10000, 921, 21474836480)
    # timeseries_windows = timeseries_partitioner.generate_windows()
    # for i in range(10):
    #     print(next(timeseries_windows))

    window = [(pd.Timestamp('2019-01-01 00:00:00'), pd.Timestamp('2019-01-01 02:46:39'))]
    window_generator = TimeSeriesPartitioner.generate_datetimestamps_from_window(window)
    print(next(window_generator))
    print(next(window_generator))
    print(next(window_generator))
    print(next(window_generator))
    print(next(window_generator))
    print(next(window_generator))
