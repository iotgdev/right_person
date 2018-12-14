#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
utility functions to manage right person stores
"""
from __future__ import unicode_literals


from right_person.machine_learning.models import RightPersonModel
from right_person.data_mining.profiles.miner import RightPersonProfileMiner
from right_person.stores.model_stores import S3RightPersonModelStore
from right_person.stores.miner_stores import S3ProfileMinerStore

_MODEL_STORE = None
_MINER_STORE = None


def get_model_store():
    """Gets a model store"""
    global _MODEL_STORE
    if _MODEL_STORE is None:
        _MODEL_STORE = S3RightPersonModelStore('', '', RightPersonModel)
    return _MODEL_STORE


def get_miner_store():
    """Gets a profile miner store"""
    global _MINER_STORE
    if _MINER_STORE is None:
        _MINER_STORE = S3ProfileMinerStore('', '', RightPersonProfileMiner)
    return _MINER_STORE
