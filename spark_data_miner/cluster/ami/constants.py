#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pyspark


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
    'ujson',
    'numpy',
    'sklearn',
    'mmh3',
    'boto3',
    'requests',
    'scipy',
    'scikit-learn',
    'ulid'
]

NAME_FORMAT = 'SPARK_DATA_MANAGER-{}'

COMMANDS = [
    'timeout 180 /bin/bash -c "until stat /var/lib/cloud/instance/boot-finished 2>/dev/null; do echo .; sleep 1; done"',
    'apt update',
    'apt install {apt_dependencies} -y'.format(apt_dependencies=' '.join(APT_DEPENDENCIES)),
    'pip install {python_dependencies} --upgrade '.format(python_dependencies=' '.join(PYTHON_DEPENDENCIES)),
    'mkdir {}'.format(SPARK_DIRECTORY),
    'pip install pyspark=={pyspark_version}'.format(pyspark_version=pyspark.__version__),
    'wget -qO- http://archive.apache.org/dist/spark/spark-{pyspark_version}/spark-{pyspark_version}-bin-hadoop2.7.tgz '
    '| tar -xvz -C {spark} --strip-components=1'.format(pyspark_version=pyspark.__version__, spark=SPARK_DIRECTORY),
]
