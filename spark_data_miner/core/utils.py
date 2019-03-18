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


def get_spark_s3_files(s3_bucket, s3_prefix):
    """
    get the address of files on s3 that can be processed by spark (using the s3a protocol)
    :type s3_bucket: str
    :type s3_prefix: str
    :rtype: str
    """
    return 's3a://{}'.format(os.path.join(s3_bucket, s3_prefix))