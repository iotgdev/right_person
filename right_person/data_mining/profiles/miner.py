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
import datetime
import os
import ujson
from collections import Counter

from right_person.data_mining.cluster.utils import get_spark_s3_files
from right_person.data_mining.profiles.config import ProfileDocumentConfig
from right_person.data_mining.profiles.transformations import combine_profiles, global_filter_profile
from right_person.utilities.connections import get_s3_connection


PROFILE_DAYS = 7


class RightPersonProfileMiner(object):
    """A RightPerson data miner that builds profiles from openrtb data using spark"""

    def __init__(self, config, output_s3_bucket, data_max_age=PROFILE_DAYS):
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
    def dates(self):
        """Get the dates of the job. Not available until the run_date is set"""
        return sorted(self.run_date - datetime.timedelta(days=i) for i in range(self.data_max_age))

    @property
    def input_prefixes(self):
        """Get the input locations of the job. Not available until the run_date is set"""
        return {date: date.strftime(os.path.join(self.config.s3_prefix)) for date in self.dates}

    @property
    def output_prefixes(self):
        """Get the output locations of the job. Not available until the run_date is set"""
        date_prefix = os.path.join('right_person', 'profiles', self.config.doc_name, '%Y/%m/%d/')
        return {date: date.strftime(date_prefix) for date in self.dates}

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
        input_prefix = self.input_prefixes[date]
        return get_spark_s3_files(self.config.s3_bucket, input_prefix)

    def profile_output_location(self, date):
        """
        Get the output location for the job, for a given date
        :type date: datetime|date|None
        :rtype: str
        """
        output_prefix = self.output_prefixes[date]
        return get_spark_s3_files(self.output_s3_bucket, output_prefix)

    def profile_building_functions(self):
        """
        This function returns functions (that can be serialized) for the spark job to create profiles
        :returns: a function to create profiles and a function to store them
        """

        fields = self.config.fields
        profile_field_id = self.config.profile_id_field
        storage_delimiter = self.storage_delimiter

        def create_profile(record):
            profile = dict([field.get_value_from_record(record) for field in fields] + [('c', 1)])
            return record[profile_field_id], profile

        def store_profile(user_profile):
            user_id, profile = user_profile
            return storage_delimiter.join([user_id, ujson.dumps(profile)])

        return create_profile, store_profile

    def profile_loading_functions(self):
        """
        This functions returns functions (that can be serialized) for the spark job to create profiles
        :returns:
        """
        storage_delimiter = self.storage_delimiter

        def deserialize_profile(profile_str):

            profile = ujson.loads(profile_str)

            for k, v in profile.items():
                if isinstance(v, dict):
                    profile[k] = Counter(v)
                elif isinstance(v, list):
                    profile[k] = set(v)

            return profile

        def load_profile(record):
            user_id, profile_str = record.strip().split(storage_delimiter)
            return user_id, deserialize_profile(profile_str)

        return load_profile

    def build_profiles(self, session, date):
        """
        Builds profiles for a specific right_person configuration
        :type session: pyspark.SparkSession
        :type date: datetime|date
        """

        record_location = self.profile_input_location(date)
        profile_save_location = self.profile_output_location(date)

        create_profile, serialise_profile = self.profile_building_functions()

        files_rdd = session.read.format("csv").option("header", self.config.files_contain_headers).option(
            "delimiter", self.config.delimiter).load(record_location).rdd

        files_rdd.map(create_profile).reduceByKey(combine_profiles).filter(
            global_filter_profile).map(serialise_profile).saveAsTextFile(
            profile_save_location, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

    def profiles(self, session):
        """
        Get the profiles as defined by the job. Only works when the job is complete (self.run_date is set)
        :type session: pyspark.SparkSession
        :rtype: pyspark.rdd.RDD
        """
        profiles = ','.join([self.profile_output_location(date) for date in self.dates])
        load_profile = self.profile_loading_functions()

        return session.sparkContext.textFile(profiles).map(load_profile).reduceByKey(combine_profiles)

    def profiles_exist_for_date(self, date):

        prefix = self.output_prefixes[date]
        return len(list(get_s3_connection().Bucket(self.output_s3_bucket).objects.filter(Prefix=prefix))) > 1

    def run(self, session):

        self.run_date = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        for date in self.dates:
            if not self.profiles_exist_for_date(date):
                self.build_profiles(session, date)
