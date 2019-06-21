#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
classes for storing right person models
"""
from __future__ import unicode_literals

import logging
import struct
import ujson
from builtins import bytes

import numpy
from retrying import retry

from ioteclabs_wrapper.core.access import get_labs_dal
from ioteclabs_wrapper.modules.right_person import RightPerson as LabsRightPersonAPI
from right_person.models.core import RightPersonModel

try:
    # noinspection PyCompatibility
    from urllib.parse import urlparse
except ImportError:
    # noinspection PyUnresolvedReferences,PyCompatibility
    from urlparse import urlparse


logger = logging.getLogger('right_person.models.store')


class RightPersonStore(object):

    _api_to_model = {
        'id': 'model_id',
        'account': 'account',
        'name': 'name',
        'audience_size': 'audience_size',
        'good_users_in_audience': 'audience_good_size',
        'hash_size': 'hash_size',
        'penalty': 'l2reg',
        'features': 'features',
        'created_at': 'created_at',
        'updated_at': 'updated_at',
    }

    _byte_fields = {
        'weights': 'weights'
    }

    _json_fields = {
        'good_users': 'good_users'
    }

    _file_fields = {
        'good_users': 'good_users'
    }

    def __init__(self):
        """create API session"""
        self.api = LabsRightPersonAPI(dal=get_labs_dal())

    @property
    def model_fields(self):
        """
        :rtype: list[tuple[str, str]]
        """
        return list(self._api_to_model.items()) + list(self._byte_fields.items()) + list(self._json_fields.items())

    @property
    def params(self):
        """
        :rtype: dict
        """
        return {'complete': True, 'verbose': True}

    def _to_model(self, response):
        """converts an api response to a model object"""
        self._format_model_bytes(response)
        try:
            self._format_model_json(response)
        except ValueError:
            self._format_model_file(response)
        return RightPersonModel(**{v: response.get(k) for k, v in self.model_fields})

    def _format_model_bytes(self, response):
        """
        gets a bytes representation from the response
        :type response: dict
        """
        model_id = response['id']
        for i, j in self._byte_fields.items():
            if self._is_internal_resource(response[i]):
                response[i] = numpy.frombuffer(self.api.resources.retrieve(model_id, j, **self.params), dtype='<f4')

    def _format_model_file(self, response):
        """
        loads a file of lines as a list from response
        :type response: dict
        """
        model_id = response['id']
        for i, j in self._file_fields.items():
            if self._is_internal_resource(response[i]):
                file_lines = self.api.resources.retrieve(model_id, j, **self.params).split(bytes('\n'.encode('utf-8')))
                response[i] = {line.strip() for line in file_lines}

    def _format_model_json(self, response):
        """
        gets model json from response
        :type response: dict
        """
        model_id = response['id']
        for i, j in self._json_fields.items():
            if self._is_internal_resource(response[i]):
                response[i] = ujson.loads(self.api.resources.retrieve(model_id, j, **self.params))

    def _to_response(self, model):
        """
        converts a model object to an api response
        :type model: RightPersonModel
        :rtype: dict
        """
        response = {k: getattr(model, v, None) for k, v in self.model_fields}
        self._format_response_bytes(response)
        self._format_response_files(response)
        return response

    def _format_response_bytes(self, response):
        """
        formats the byte fields in a response
        :type response: dict
        """
        hash_size = response['hash_size']
        for i in self._byte_fields:
            response[i] = struct.pack('<%sf' % hash_size, *response[i].ravel())
            if isinstance(response[i], str):  # python 2 to 3
                response[i] = bytes(response[i])

    def _format_response_json(self, response):
        """
        formats the json fields in a response
        :type response: dict
        """
        for i in self._json_fields:
            response[i] = ujson.dumps(response[i]).encode()
            if isinstance(response[i], str):  # python 2 to 3
                response[i] = bytes(response[i])

    def _format_response_files(self, response):
        """
        formats the file fields for a response
        :type response: dict
        """
        for i in self._file_fields:
            response[i] = '\n'.join(response[i]).encode()
            if isinstance(response[i], str):
                response[i] = bytes(response[i])

    def _is_internal_resource(self, value):
        """checks if a model value directs to another API endpoint"""
        try:
            url_value = urlparse(value)
        except (AttributeError, TypeError):
            return False

        # noinspection PyProtectedMember
        current_host_value = urlparse(self.api._dal.url)
        return url_value.netloc == current_host_value.netloc and url_value.scheme == current_host_value.scheme

    # noinspection PyShadowingBuiltins
    @retry(stop_max_attempt_number=3)
    def retrieve(self, id):
        """retrieve a right person model from the labs API by id"""

        return self._to_model(self.api.retrieve(id, **self.params))

    @retry(stop_max_attempt_number=3)
    def create(self, model):
        """register a model from an object on the labs API"""
        if model.model_id:
            raise ValueError('This model has an id!')

        return self._to_model(self.api.create(params=self.params, **self._to_response(model)))

    @retry(stop_max_attempt_number=3)
    def update(self, model):
        """update a model from an object on the labs API"""
        if not model.model_id:
            raise ValueError('This model has no id!')

        return self._to_model(self.api.update(params=self.params, **self._to_response(model)))

    @retry(stop_max_attempt_number=3)
    def _list_iter(self, **kwargs):
        """
        iterate through models one at a time, yielding
        :type kwargs: dict[str, str]
        :rtype: list[RightPersonModel]
        """
        params = dict(self.params, offset=0, limit=100)
        # noinspection PyTypeChecker
        params.update(kwargs, fields='id')
        continuing = True

        while continuing:
            response = self.api.list(**params)
            for i in response['results']:
                yield self.retrieve(i['id'])
            params['offset'] += params['limit']
            continuing = response['next']

    def list(self, as_list=False, **kwargs):
        """
        List right person models, choosing an iterator or list output
        :type as_list: bool
        :type kwargs: dict
        :rtype: list[RightPersonModel]
        """
        if as_list:
            return list(self._list_iter(**kwargs))
        else:
            return self._list_iter(**kwargs)
