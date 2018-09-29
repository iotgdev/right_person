#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Classes managing the storage of right person profile miners
"""
from __future__ import unicode_literals

import ujson
from datetime import datetime

import ulid

from right_person.utilities.connections import get_s3_connection


def get_job_directory(prefix, job_id):
    """Get the location of the job code"""
    return '/'.join((prefix, 'profile_miners', job_id))


class S3ProfileMinerStore(object):

    MINER_PARAMS = 'MINER_PARAMS'
    MINER_CONFIG = 'MINER_CONFIG'

    def __init__(self, s3_bucket, s3_prefix, miner_class):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.miner_class = miner_class

    def retrieve(self, miner_id):
        """
        retrieve a swarm miner
        :type miner_id: str
        :rtype: right_person.core.profiles.miners.RightPersonProfileMiner
        """
        miner_params = self._get_json(miner_id, self.MINER_PARAMS)
        miner_config = self._get_json(miner_id, self.MINER_CONFIG)
        run_date = datetime.strptime(miner_params.pop('run_date'), '%Y-%m-%d %H:%M:%S.%f')
        miner = self.miner_class(**miner_params)
        miner.deserialize(configuration=miner_config, run_date=run_date, **miner_params)
        return miner

    @property
    def metadata_prefix(self):
        """get the s3 prefix corresponding to the metadata location of the mining jobs"""
        return '/'.join((self.s3_prefix, 'metadata', ''))

    def list(self):
        """List a history of the recent profile mining jobs"""
        s3_objects = get_s3_connection().Bucket(self.s3_bucket).objects.filter(Prefix=self.metadata_prefix)
        jobs = [obj.key[len(self.metadata_prefix):] for obj in s3_objects]

        return sorted(jobs, reverse=True)

    def create(self, miner):
        """
        Create/register a new profile mining job
        :type miner: right_person.core.profiles.miners.RightPersonProfileMiner
        :rtype: str
        """
        miner.job_id = ulid.ULID.new().str
        miner_id = self.update(miner)
        self._create_key(self.metadata_prefix + miner_id)

        return miner_id

    def update(self, miner):
        """
        update a mining job reference
        :type miner:
        :rtype: str
        """
        if not miner.job_id:
            raise ValueError('Profile Mining Job has no id!')
        serialised_miner = miner.serialize()
        config = serialised_miner['configuration']
        miner_id = miner.job_id
        self._set_json(miner_id, self.MINER_PARAMS, serialised_miner)
        self._set_json(miner_id, self.MINER_CONFIG, config)
        return miner_id

    def delete(self, miner_id):
        """
        Delete a mining job reference (does not delete mined resources)
        :type miner_id: str
        """
        location = get_job_directory(self.s3_prefix, miner_id)
        for obj in get_s3_connection().Bucket(self.s3_bucket).objects.filter(Prefix=location):
            self._delete_key(obj.key)

        self._delete_key(self.metadata_prefix + miner_id)

    def _set_json(self, job_id, location, data):
        """saves a json document to s3"""
        file_path = get_job_directory(self.s3_prefix, job_id) + '/{}.json'.format(location)
        self._create_key(file_path, body=ujson.dumps(data))

    def _get_json(self, job_id, location):
        """gets a json document from s3"""
        file_path = get_job_directory(self.s3_prefix, job_id) + '/{}.json'.format(location)
        obj = get_s3_connection().Object(self.s3_bucket, file_path)

        return ujson.loads(obj.get()['Body'].read())

    def _create_key(self, key_name, body=""):
        """creates a key on s3"""
        get_s3_connection().Object(self.s3_bucket, key_name).put(Body=body)

    def _delete_key(self, key_name):
        """deletes a key on s3"""
        get_s3_connection().Bucket(self.s3_bucket).delete_objects(
            Delete={'Objects': [{'Key': key_name}], 'Quiet': True})
