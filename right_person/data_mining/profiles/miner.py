#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Right Person Profile Miner

The right person model relies on a complex profile based dataset.
The dataset requires a data mining phase.
This file contains data mining classes to build profiles.

Usage:
>>> from right_person.data_mining.cluster.context_managers import right_person_cluster_session
>>> miner = RightPersonProfileMiner({}, '')
>>> with right_person_cluster_session('cluster-id') as session:
...:    miner.run(session)
"""
from __future__ import unicode_literals

import csv
import datetime
import os
import ujson
from operator import itemgetter

from right_person.data_mining.cluster.utils import get_spark_s3_files
from right_person.data_mining.profiles.config import ProfileDocumentConfig
from right_person.data_mining.profiles.transformations import combine_profiles, global_filter_profile
from right_person.utilities.connections import get_s3_connection


class RightPersonProfileMiner(object):
    """A RightPerson data miner that builds profiles from openrtb data using spark"""

    def __init__(self, config, output_s3_bucket, data_max_age=7):
        """
        create a right_person profile creating job
        :type config: right_person.config.profile_mining_job.ProfileDocumentConfig
        :type output_s3_bucket: str
        :type data_max_age: int
        """
        # these properties are set at runtime and/or by the store that manages the machine_learning
        self.job_id = None
        self.run_date = None

        self.config = config
        self.data_max_age = data_max_age
        self.output_s3_bucket = output_s3_bucket
        self.storage_delimiter = '\t'

    @property
    def _dates(self):
        """Get the dates of the job. Not available until the run_date is set"""
        return sorted(self.run_date - datetime.timedelta(days=i) for i in range(1, self.data_max_age + 1))

    @property
    def _input_prefixes(self):
        """Get the input locations of the job. Not available until the run_date is set"""
        return {date: date.strftime(os.path.join(self.config.s3_prefix)) for date in self._dates}

    @property
    def _output_prefixes(self):
        """Get the output locations of the job. Not available until the run_date is set"""
        date_prefix = os.path.join('right_person', self.config.doc_name, 'profiles', '%Y-%m-%d/')
        return {date: date.strftime(date_prefix) for date in self._dates}

    @property
    def create_profile(self):  # todo: confirm this is necessary
        """
        This function returns a function (that can be serialized) for the spark job to create profiles
        they do not contain references to self, and so can be easily serialised by spark.
        :returns:
        """

        fields = self.config.fields

        for f in fields:
            true_type = eval(f.field_type)
            f.true_val = true_type if len(f.field_position) == 1 else (lambda x: true_type(*x))
            f.getter = itemgetter(*f.field_position)

        profile_field_id = self.config.profile_id_field

        def get_value_from_record(field, split_record):
            value = field.true_val(field.getter(split_record))
            return get_stored_value(field.store_as, value)

        def get_stored_value(store_as, value):
            if store_as == 'counter':
                return {value: 1}
            if store_as == 'set':
                return {value}
            elif store_as is None:
                return value

        def create_profile(record):
            profile = {}
            for field in fields:
                profile[field.field_name] = get_value_from_record(field, record)
            profile['c'] = 1
            return record[profile_field_id], profile

        return create_profile

    @property
    def store_profile(self):  # todo: confirm this is necessary
        """
        This function returns a function (that can be serialized) for the spark job to create profiles_by_day
        the functions do not reference self.
        :return:
        """
        storage_delimiter = self.storage_delimiter

        def store_profile(user_profile):
            user_id, profile = user_profile
            return storage_delimiter.join([user_id, ujson.dumps(profile)])

        return store_profile

    @property
    def load_profile(self):  # todo: confirm this is necessary
        """
        This function returns a function (that can be serialized) for the spark job to create profiles_by_day
        the functions do not reference self.
        :returns:
        """
        storage_delimiter = self.storage_delimiter

        def deserialize_profile(profile_str):
            profile = ujson.loads(profile_str)
            for k, v in profile.items():
                if isinstance(v, list):
                    profile[k] = set(v)
            return profile

        def load_profile(record):
            user_id, profile_str = record.strip().split(storage_delimiter)
            return user_id, deserialize_profile(profile_str)

        return load_profile

    def serialize(self):
        """
        Serializes the job so that we can store it
        :rtype: dict
        """
        return {
            'configuration': self.config,
            'run_date': self.run_date,
            'data_max_age': self.data_max_age,
            's3_bucket': self.output_s3_bucket
        }

    def deserialize(self, configuration, output_s3_bucket, data_max_age, run_date):
        """
        take a deserialized job and load it as an object
        :type configuration: dict
        :type output_s3_bucket: str
        :type data_max_age: int
        :type run_date: datetime|date|None
        """
        self.config = ProfileDocumentConfig(**configuration)
        self.output_s3_bucket = output_s3_bucket
        self.data_max_age = data_max_age
        self.run_date = run_date  # allowing the setting of a run date means we can repeat right_person runs

    def profile_input_location(self, date):
        """
        Get the input location for the job, for a given date
        :type date: datetime|date|None
        :rtype: str
        """
        return get_spark_s3_files(self.config.s3_bucket, self._input_prefixes[date])

    def profile_output_location(self, date):
        """
        Get the output location for the job, for a given date
        :type date: datetime|date|None
        :rtype: str
        """
        return get_spark_s3_files(self.output_s3_bucket, self._output_prefixes[date])

    def build_profiles(self, session, date):
        """
        Builds profiles for a specific right_person configuration
        :type session: pyspark.SparkSession
        :type date: datetime|date
        """

        record_location = self.profile_input_location(date)
        profile_save_location = self.profile_output_location(date)

        profile_delimiter = str(self.config.delimiter)

        raw_files = session.sparkContext.textFile(record_location)

        if self.config.files_contain_headers:
            header = raw_files.first()
            map_fn = (lambda x: self.create_profile(
                csv.reader([x], delimiter=profile_delimiter).next()) if x != header else (None, None))
        else:
            map_fn = (lambda x: self.create_profile(csv.reader([x], delimiter=profile_delimiter).next()))

        partial_profiles = raw_files.map(map_fn).filter(lambda x: x != (None, None))
        profiles = partial_profiles.reduceByKey(combine_profiles, partitionFunc=hash).filter(global_filter_profile)
        profiles.map(self.store_profile).saveAsTextFile(
            profile_save_location, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

    def profiles_by_day(self, session):
        """
        Get the profiles as defined by the job. Only works when the job is complete (self.run_date is set)
        :type session: pyspark.SparkSession
        :rtype: pyspark.rdd.RDD
        """
        profiles = ','.join([self.profile_output_location(date) for date in self._dates])

        return session.sparkContext.textFile(profiles).map(self.load_profile)

    def profiles_exist_for_day(self, date):
        """Checks if profiles exist for a given date"""

        prefix = self._output_prefixes[date]
        return bool(list(get_s3_connection().Bucket(self.output_s3_bucket).objects.filter(Prefix=prefix)))

    def run(self, session):
        """runs the mining job to produce profiles"""

        self.run_date = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        for date in self._dates:
            if not self.profiles_exist_for_day(date):
                self.build_profiles(session, date)
