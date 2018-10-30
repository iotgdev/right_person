#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""connection functions for right_person"""
from __future__ import unicode_literals


import boto3


_S3 = None


def get_s3_connection():
    """
    Get singleton S3 Connection
    :rtype boto3.resources.base.ServiceResource:
    """
    global _S3
    if _S3 is None:
        _S3 = boto3.resource('s3')
    return _S3
