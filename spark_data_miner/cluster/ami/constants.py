#!/usr/bin/env python
# -*- coding: utf-8 -*-


BASE_IMAGE = {
    'Owners': ['099720109477'],
    'Filters': [
        {
            'Name': 'name',
            'Values': [
                'ubuntu/images/*ubuntu-bionic-18.04-amd64-server-*'
            ]
        },
        {
            'Name': 'root-device-type',
            'Values': [
                'ebs']
        },
        {
            'Name': 'virtualization-type',
            'Values': [
                'hvm'
            ]
        }
    ]
}


SPARK_DIRECTORY = '/home/ubuntu/spark/'


APT_DEPENDENCIES = [
    'python',
    'htop',
    'python-pip',
    'ntp',
    'pkg-config',
    'rsyslog',
    'strace',
    'tcl',
    'unzip',
    'openjdk-8-jdk',
]

PYTHON_DEPENDENCIES = [
    'pip',
    'wheel',
    'setuptools',
]

PACKAGE_DEPENDENCIES = [
    'future',
    'boto3',
    'mmh3',
    'numpy',
    'pyspark',
    'requests',
    'scipy==1.1.0',
    'scikit-learn',
    'ujson',
    'six',
    'ioteclabs-wrapper',
    'retrying',
]

NAME_FORMAT = 'SPARK_DATA_MANAGER-{}'
