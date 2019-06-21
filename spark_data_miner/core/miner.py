#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Data Miner

Combines large datasets into one
The dataset requires a data mining phase powered by spark and using s3 resources.

Usage:
>>> from spark_data_miner.cluster.manager.context_managers import spark_data_mining_session
>>> from spark_data_miner.cluster.manager.access import ClusterPlan
>>> from spark_data_miner.core.config import MinerField, MinerConfig
>>> config = MinerConfig(...)
>>> cluster_plan = ClusterPlan(...)
>>> miner = SparkDatasetMiner(config, 'output-bucket', 1)
>>> with spark_data_mining_session(cluster_plan) as session:
...:    miner.create_dataset(session)
"""
from __future__ import unicode_literals

import base64
import csv
import datetime
import logging
import os
import posixpath
import ujson
from operator import itemgetter

from spark_data_miner.core.utils import get_spark_s3_files, get_s3_connection


logger = logging.getLogger('spark_data_miner.core.miner')


class SparkDatasetMiner(object):

    MAX_COMBINED_RECORDS = 10000
    MIN_COMBINED_RECORDS = 5

    def __init__(self, config, output_s3_bucket, data_max_age=7):
        self.run_date = None

        self.config = config
        self.data_max_age = data_max_age
        self.output_s3_bucket = output_s3_bucket
        self.storage_delimiter = '\t'

    @property
    def _dates(self):
        """Get the dates of the job. Not available until the run_date is set"""
        return sorted(self.run_date - datetime.timedelta(days=i+1) for i in range(self.data_max_age))

    @property
    def _input_prefixes(self):
        """Get the input locations of the job. Not available until the run_date is set"""
        return {date: date.strftime(os.path.join(self.config.s3_prefix)) for date in self._dates}

    @property
    def _output_prefixes(self):
        """Get the output locations of the job. Not available until the run_date is set"""
        bucket = base64.b64encode(self.config.s3_bucket.encode()).decode("utf-8")
        prefix = base64.b64encode(self.config.s3_prefix.encode()).decode("utf-8")
        date_prefix = posixpath.join('spark_data_miner', self.config.name, bucket, prefix, '%Y-%m-%d/')
        return {date: date.strftime(date_prefix) for date in self._dates + [self.run_date]}

    @property
    def create_record(self):
        """
        This function returns a function (that can be serialized) for the spark job to create a dataset
        they do not contain references to self, and so can be easily serialised by spark.
        :returns: types.FuncType
        """

        field_functions = ((f.name, f.stype, eval(f.rtype), itemgetter(*f.index)) for f in self.config.fields)
        id_field = self.config.id_field

        def get_stored_value(store_as, value):
            if store_as == 'dict':
                return {value: 1}
            if store_as == 'set':
                return {value}
            if store_as is None:
                return value

        def create_record(raw):
            record = {}
            for field_name, field_type, true_val, getter in field_functions:
                record[field_name] = get_stored_value(field_type, true_val(getter(raw)))
            record['c'] = 1
            return raw[id_field], record

        return create_record

    @property
    def combine_records(self):

        max_records = self.MAX_COMBINED_RECORDS

        def combine_records(record_1, record_2):
            if (not record_1) or (not record_2) or record_1['c'] + record_2['c'] > max_records:
                return
            for feature, val in record_2.items():
                if feature in record_1:
                    if isinstance(val, (bool, set)):
                        record_1[feature] |= val
                    elif isinstance(val, int):
                        record_1[feature] += val
                    elif isinstance(val, dict):
                        for i in val:
                            if i in record_1[feature]:
                                record_1[feature][i] += val[i]
                            else:
                                record_1[feature][i] = val[i]
                else:
                    record_1[feature] = val

            return record_1

        return combine_records

    @property
    def filter_records(self):

        min_records = self.MIN_COMBINED_RECORDS
        max_records = self.MAX_COMBINED_RECORDS

        def filter_records(record):
            id_field, record = record
            return record and min_records <= record['c'] <= max_records and id_field

        return filter_records

    @property
    def store_record(self):
        """
        This function returns a function (that can be serialized) for the spark job to create get_dataset_for_day
        the functions do not reference self.
        :return:
        """
        storage_delimiter = self.storage_delimiter

        def store_record(mined_data):
            id_field, record = mined_data
            return storage_delimiter.join([id_field, ujson.dumps(record)])

        return store_record

    @property
    def load_record(self):
        """
        This function returns a function (that can be serialized) for the spark job to create a dataset
        the functions do not reference self.
        :returns:
        """
        storage_delimiter = self.storage_delimiter

        def deserialize_record(serialized_record):
            record = ujson.loads(serialized_record)
            for k, v in record.items():
                if isinstance(v, list):
                    record[k] = set(v)
            return record

        def load_record(stored_data):
            id_field, serialized_record = stored_data.strip().split(storage_delimiter)
            return id_field, deserialize_record(serialized_record)

        return load_record

    def get_dataset_input_location(self, date):
        """
        Get the input location for the job, for a given date
        :type date: datetime|date|None
        :rtype: str
        """
        return get_spark_s3_files(self.config.s3_bucket, self._input_prefixes[date])

    def get_dataset_output_location(self, date):
        """
        Get the output location for the job, for a given date
        :type date: datetime|date|None
        :rtype: str
        """
        return get_spark_s3_files(self.output_s3_bucket, self._output_prefixes[date])

    def create_dataset_for_day(self, session, date):
        """
        Builds datasets for a specific right_person configuration
        :type session: pyspark.SparkSession
        :type date: datetime|date
        """

        record_location = self.get_dataset_input_location(date)
        dataset_output_location = self.get_dataset_output_location(date)

        delimiter = str(self.config.delimiter)

        raw_files = session.sparkContext.textFile(record_location)

        if self.config.headers:  # todo: abstract to a method
            header = raw_files.first().strip()
            map_fn = (lambda x: self.create_record(
                csv.reader([x], delimiter=delimiter).next()) if header not in x else (None, None))
        else:
            map_fn = (lambda x: self.create_record(csv.reader([x], delimiter=delimiter).next()))

        partial_dataset = raw_files.map(map_fn).filter(lambda x: x != (None, None))
        dataset = partial_dataset.reduceByKey(self.combine_records, 1000).filter(self.filter_records)
        dataset.map(self.store_record).saveAsTextFile(
            dataset_output_location, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

    def get_dataset_for_day(self, session):
        """
        :type session: pyspark.SparkSession
        """
        for date in self._dates:
            try:
                rdd = session.sparkContext.textFile(self.get_dataset_output_location(date)).map(self.load_record).cache()
                yield rdd
                rdd.unpersist()
            except (Exception, ):
                logger.warning('Could not retrieve dataset {} for date {}'.format(self.config.name, date))

    def dataset_exists(self, date):
        prefix = self._output_prefixes[date]
        return bool(list(get_s3_connection().Bucket(self.output_s3_bucket).objects.filter(Prefix=prefix)))

    def create_dataset(self, session):
        self.run_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        for date in self._dates:
            if not self.dataset_exists(date):
                try:
                    self.create_dataset_for_day(session, date)
                except (Exception, ):
                    logger.warning('Could not build dataset {} for date {}'.format(self.config.name, date))
