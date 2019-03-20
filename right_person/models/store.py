#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
classes for storing right person models
"""
from __future__ import unicode_literals
from builtins import bytes

import struct
import ujson

import logging

import numpy

from ioteclabs_wrapper.core.access import get_labs_dal
from ioteclabs_wrapper.modules.right_person import RightPerson as LabsRightPersonAPI
from right_person.models.core import RightPersonModel

try:
    # noinspection PyCompatibility
    from urllib.parse import urlparse
except ImportError:
    # noinspection PyUnresolvedReferences,PyCompatibility
    from urlparse import urlparse


logger = logging.getLogger('right_person.stores.model_stores')


class RightPersonStore(object):

    api_to_model = {
        'id': 'model_id',
        'account': 'account',
        'name': 'name',
        'audienceSize': 'audience_size',
        'goodUsersInAudience': 'audience_good_size',
        'hashSize': 'hash_size',
        'penalty': 'l2reg',
        'features': 'features',
        'createdAt': 'created_at',
        'updatedAt': 'updated_at',
    }

    byte_fields = {
        'weights': 'weights'
    }

    json_fields = {
        'goodUsers': 'good_users'
    }

    def _to_model(self, response):
        model_signature = {v: response.get(k) for k, v in self.api_to_model.items()}
        return RightPersonModel(**model_signature)

    def _to_response(self, model):
        model_fields = {k: getattr(model, v, None) for k, v in self.api_to_model.items()}
        hash_size = model_fields['hashSize']
        for i in self.byte_fields:
            model_fields[i] = struct.pack('<%sf' % hash_size, *model_fields[i].ravel())
            if isinstance(model_fields[i], str):  # python 2 to 3
                model_fields[i] = bytes(model_fields[i])
        for i in self.json_fields:
            model_fields[i] = ujson.dumps(model_fields[i]).encode()
            if isinstance(model_fields[i], str):  # python 2 to 3
                model_fields[i] = bytes(model_fields[i])
        return model_fields

    def __init__(self):
        self.api = LabsRightPersonAPI(dal=get_labs_dal())

    def _is_internal_resource(self, value):
        try:
            url_value = urlparse(value)
        except (AttributeError, TypeError):
            return False

        # noinspection PyProtectedMember
        current_host_value = urlparse(self.api._dal.url)
        return url_value.netloc == current_host_value.netloc and url_value.scheme == current_host_value.scheme

    # noinspection PyShadowingBuiltins
    def retrieve(self, id):
        params = {'complete': True, 'verbose': True}
        resp = self.api.retrieve(id, **params)

        for key, value in resp.items():
            print(value)
            if self._is_internal_resource(value):
                model_id, resource = value.split('/')[-2:]
                resp_resource = self.api.resources.retrieve(model_id, resource, **params)
                if key in self.byte_fields:
                    resp[key] = numpy.fromstring(resp_resource, dtype='<f4')
                elif key in self.json_fields:
                    resp[key] = ujson.loads(resp_resource.decode())
        return self._to_model(resp)

    def create(self, model):
        if model.model_id:
            raise ValueError('This model has an id!')
        params = {'complete': True, 'verbose': True}
        created_model = self._to_model(self.api.create(params=params, **self._to_response(model)))

        model.model_id = created_model.model_id
        model.created_at = created_model.created_at
        model.updated_at = created_model.updated_at
        return model

    def update(self, model):
        if not model.model_id:
            raise ValueError('This model has no id!')
        params = {'complete': True, 'verbose': True}
        updated_model = self._to_model(self.api.update(params=params, **self._to_response(model)))
        model.updated_at = updated_model.updated_at
        return model

    def _list_iter(self, **kwargs):
        params = {'complete': True, 'verbose': True, 'offset': 0, 'limit': 100}
        params.update(kwargs, fields='id')
        continuing = True

        while continuing:
            response = self.api.list(**params)
            for i in response['results']:
                yield self.retrieve(i['id'])
            params['offset'] += params['limit']
            continuing = response['next']

    def list(self, as_list=False, **kwargs):
        if as_list:
            return list(self._list_iter(**kwargs))
        else:
            return self._list_iter(**kwargs)
