#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utility functions for interfacing with a right person spark cluster.

SparkPackageManager:
- holds a dictionary of packages added to the spark context dynamically. Removes them on process finish

get_current_ipv4:
- gets the current public ipv4 of an instance

run_system_shell_process:
- runs a process, recording the stdout and stderr

get_terraform_vars:
- gets the current right_person terraform variables as a python dict

add_package_to_spark:
- adds an imported package to spark for use by the worker nodes.
- creates a tar file containing the package contents
- must be "pure python" (contain no other language dependencies)

get_spark_s3_files:
- get file addresses that spark can process (files marked with the s3a protocol)

No direct usage is expected.
"""
from __future__ import unicode_literals

import logging
import os
import shutil
import subprocess
import ujson
import sys

import requests

CLUSTER_DIR = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))

TERRAFORM_DIRECTORY = os.path.join(CLUSTER_DIR, 'terraform', '')
TERRAFORM_VARS = os.path.join(TERRAFORM_DIRECTORY, 'terraform.tfvars.json')


logger = logging.getLogger('right_person.cluster.utils')


class SparkPackageManager(dict):

    def __del__(self):
        for package_name, package_tar in self.items():
            try:
                os.remove(package_tar)
            except (Exception, ):
                logger.warning('Could not remove temporary package {} at {}'.format(package_name, package_tar))


def get_current_ipv4():
    """
    get the ipv4 of the current machine
    Tries to get the public ipv4 of an ec2 instance before deferring to an external ipv4 identifier
    :rtype: str
    """
    try:
        return requests.get("http://169.254.169.254/latest/meta-data/public-ipv4", timeout=0.01).text
    except requests.ConnectTimeout:
        logger.warning('Could not find ec2 public ipv4 address.')
        return requests.get('http://ip.42.pl/raw', timeout=1).text


def run_system_shell_process(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    """
    runs a shell process. raises errors and returns stdout
    :type cmd: str
    :param stdout: file like or None. Stores the results of system stdout
    :param stderr: file like or None. Stores the results of system stderr
    :rtype: str
    """
    process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, bufsize=1, shell=True)
    info = None
    if stdout:
        info = '\n'.join([line for line in iter(process.stdout.readline, b'')])
        process.stdout.close()
    if stderr:
        errors = '\n'.join([line for line in iter(process.stderr.readline, b'')])
        if errors:
            logger.error(errors)
            raise RuntimeError("Command failed: {}\n".format(cmd) + errors)
        process.stderr.close()
    return info


def get_terraform_vars():
    """
    load the terraform vars file as json
    :rtype: dict
    """
    return ujson.load(open(TERRAFORM_VARS))


def format_terraform_kwargs(kwargs):
    return '\n'.join("-var '{}={}'".format(k, v) for k, v in kwargs.items())


def add_package_to_spark(session, package_name):  # todo: refocus around the class rather than the function
    """
    adds a python package to the spark context by package import
    :type session: pyspark.SparkSession
    :param package_name: a python imported package name
    """
    package_location = os.path.abspath(os.path.dirname(sys.modules[package_name].__file__))
    package_tar = shutil.make_archive(package_name, 'zip', package_location)
    _SPARK_PACKAGE_MANAGER[package_name] = package_tar
    session.sparkContext.addPyFile(package_tar)


def get_spark_s3_files(s3_bucket, s3_prefix):
    """
    get the address of files on s3 that can be processed by spark (using the s3a protocol)
    :type s3_bucket: str
    :type s3_prefix: str
    :rtype: str
    """
    return 's3a://{}'.format(os.path.join(s3_bucket, s3_prefix))


_SPARK_PACKAGE_MANAGER = SparkPackageManager()
