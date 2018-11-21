#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
classes for storing right person models
"""
from __future__ import unicode_literals

import ujson

import datetime

import logging

from right_person.machine_learning.models.profile_model import ID_DELIMITER
from right_person.utilities.connections import get_s3_connection

_VERSION_DELIMITER = '-'


logger = logging.getLogger('right_person.stores.model_stores')


def get_model_directory(prefix, model_id):
    """Get the location of the machine_learning prefixes"""
    return '/'.join((prefix, 'models', model_id))


def get_next_version(version=None):
    """
    Given a version identifier, increment by a major version and return.
    NB that major versions are also bumped to today, where minor versions are not

    :param version: string version id
    :return: string version id
    """
    today = datetime.date.today()

    try:
        ver_parts = [int(part) for part in version.split(_VERSION_DELIMITER)]
    except AttributeError:
        logger.warning('error when creating new version from {}'.format(version))
        ver_parts = [0, 0, 0, 0]

    if ver_parts[0:3] == [today.year, today.month, today.day]:
        ver_parts[3] += 1
    else:
        ver_parts = [today.year, today.month, today.day, 0]

    return "{0:04} {1:02} {2:02} {3:02}".format(*ver_parts).replace(' ', _VERSION_DELIMITER)


class S3RightPersonModelStore(object):

    MODEL_PARAMS = 'MODEL_PARAMS'
    CLASSIFIER_PARAMS = 'CLASSIFIER_PARAMS'
    USER_IDS = 'GOOD_USER_IDS'
    TRAINING_WEIGHTS = 'TRAINING_WEIGHTS'

    def __init__(self, s3_bucket, s3_prefix, model_class):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.model_class = model_class

    @property
    def metadata_prefix(self):
        """get the s3 prefix corresponding to the metadata location of the machine_learning"""
        return '/'.join((self.s3_prefix, 'metadata', ''))

    def retrieve(self, model_id):
        """
        retrieve a machine_learning by its metdata id
        :param str model_id: model_index:version
        """
        index, version = model_id.split(ID_DELIMITER)
        model_prefix = get_model_directory(self.s3_prefix, '{}/{}'.format(index, version))

        model_params = self._get_json(model_prefix, self.MODEL_PARAMS)
        classifier_params = self._get_json(model_prefix, self.CLASSIFIER_PARAMS)
        good_user_ids = self._get_json(model_prefix, self.USER_IDS)
        training_weights = self._get_json(model_prefix, self.TRAINING_WEIGHTS)

        serialized_params = {
            'name': model_params['name'],
            'config': model_params['config'],
            'good_users': good_user_ids,
            'coef': training_weights,
            'l2reg': classifier_params['l2regularisation'],
            'warm_start': classifier_params['enable_incremental_building'],
            'audience_good_size': classifier_params['audience_good_size'],
            'audience_size': classifier_params['audience_size'],
            'num_features': classifier_params['num_features']
        }

        model = self.model_class(serialized_params.pop('name'))
        model.index = index
        model.version = version
        model.deserialize(**serialized_params)

        return model

    def list(self, model_index=None, model_version=None):
        """
        List the machine_learning available in the store, searchable by name
        :param str model_index: the machine_learning id to search for
        :param str model_version: the machine_learning version to search for
        :rtype: list[str]
        """
        model_ids = []
        for obj in get_s3_connection().Bucket(self.s3_bucket).objects.filter(Prefix=self.metadata_prefix):
            model_id = obj.key[len(self.metadata_prefix):]
            model_index_search, model_version_search = model_id.split(ID_DELIMITER)
            if not model_version or model_version in model_version_search:
                if model_index_search.isdigit() and (not model_index or model_index == model_index_search):
                    model_ids.append(ID_DELIMITER.join((model_index_search, model_version_search)))

        return sorted(model_ids, key=lambda x: x.split(ID_DELIMITER)[1], reverse=True)

    def create(self, model):
        """
        register a machine_learning with the store
        :rtype: str
        """
        model_indexes = [int(model_index.split(ID_DELIMITER)[0]) for model_index in self.list()]
        model.index = str(max(model_indexes + [0]) + 1).zfill(4)

        return self.update(model)

    def update(self, model):
        """
        persist changes in the machine_learning to the store.
        Each save makes a new version
        :rtype: str
        """
        if not model.model_id:
            raise ValueError('Model has no id!')

        model.version = get_next_version(model.version)
        model_prefix = '{}/{}'.format(model.index, model.version)
        serialized_model = model.serialize()

        model_params = {
            'config': serialized_model['config'],
            'name': serialized_model['name']
        }
        profile_data = serialized_model['good_users']
        training_weights = serialized_model['coef']
        classifier_data = {
            'l2regularisation': serialized_model['l2reg'],
            'enable_incremental_building': serialized_model['warm_start'],
            'audience_good_size': serialized_model['audience_good_size'],
            'audience_size': serialized_model['audience_size'],
            'num_features': serialized_model['num_features'],
        }

        self._set_json(model_prefix, self.MODEL_PARAMS, model_params)
        self._set_json(model_prefix, self.USER_IDS, profile_data)
        self._set_json(model_prefix, self.CLASSIFIER_PARAMS, classifier_data)
        self._set_json(model_prefix, self.TRAINING_WEIGHTS, training_weights)

        self._create_key(self.metadata_prefix + ID_DELIMITER.join((model.index, model.version)))

        return model.model_id

    def delete(self, model_id):
        """
        Delete a machine_learning from the store
        :param str model_id: model_index:model_version
        """
        location = get_model_directory(self.s3_prefix, model_id)
        for obj in get_s3_connection().Bucket(self.s3_bucket).objects.filter(Prefix=location):
            self._delete_key(obj.key)

        self._delete_key(self.metadata_prefix + model_id)

    def _set_json(self, model_id, location, data):
        """
        Set the contents of a json file on s3
        :param str|unicode model_id: model_index:model_version
        :param str|unicode location: path of the file on s3
        :param dict|list data: the data body
        """
        file_path = get_model_directory(self.s3_prefix, model_id) + '/{}.json'.format(location)
        self._create_key(file_path, body=ujson.dumps(data))

    def _get_json(self, model_id, location):
        """
        Get the content of a json file on s3
        :param str|unicode model_id: model_index:model_version
        :param str|unicode location: path of the file on s3
        :rtype: dict|list
        """
        file_path = get_model_directory(self.s3_prefix, model_id) + '/{}.json'.format(location)
        obj = get_s3_connection().Object(self.s3_bucket, file_path)

        return ujson.loads(obj.get()['Body'].read())

    def _create_key(self, key_name, body=""):
        """create a key on s3"""
        get_s3_connection().Object(self.s3_bucket, key_name).put(Body=body)

    def _delete_key(self, key_name):
        """Delete a key on s3"""
        get_s3_connection().Bucket(self.s3_bucket).delete_objects(
            Delete={'Objects': [{'Key': key_name}], 'Quiet': True})
