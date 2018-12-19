#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Creates a session to interface with a spark cluster as an independent driver.
Usage:
>>> session = get_new_right_person_spark_session('127.0.0.1')
>>> # do session things...
>>> session.stop()
"""
from __future__ import unicode_literals

import logging

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession


from right_person.data_mining.cluster.utils import get_terraform_vars


_config = None


logger = logging.getLogger('right_person.data_mining.cluster.session')


def _get_right_person_spark_config(master_ip):
    """
    Creates a config for the right_person spark cluster
    Contains specific cluster parameters including extra jars
    to access s3 resources and ports to communicate with.
    The config is a singleton and won't be recreated once set.

    :param str master_ip: the ip of the clusters master node
    :rtype: pyspark.SparkConf
    """
    global _config
    if not _config:
        slave_memory = get_terraform_vars()['slave_max_memory_gb']

        _config = SparkConf().setAppName('right_person')
        _config.setMaster('spark://{}:7077'.format(master_ip))
        _config.set('spark.local.dir', '/mnt/spark/')
        _config.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.5,com.amazonaws:aws-java-sdk:1.7.4')

        _config.set('spark.blockManager.port', '50070')
        _config.set('spark.driver.port', '45523')
        _config.set('spark.port.maxRetries', '0')

        _config.set('spark.rdd.compress', 'True')
        _config.set('spark.driver.maxResultSize', '1536m')
        # _config.set('spark.driver.memory', '1536m')
        _config.set('spark.memory.fraction', str(max(0.6, round(1 - 6. / int(slave_memory), 2))))
        _config.set('spark.executor.memory', str(slave_memory) + "g")

        _config.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # config.set("spark.executor.extraClassPath", )
        _config.get("spark.rpc.message.maxSize", "256")
        _config.set("spark.hadoop.mapred.output.compress", "true")
        _config.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
        _config.set("spark.hadoop.mapred.output.compress.type", "BLOCK")
        _config.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    return _config


def get_new_right_person_spark_session(master_ip):
    """
    Create a session to communicate with the right_person spark cluster.

    :param str master_ip: the ip of the clusters master node
    :rtype: pyspark.SparkSession
    """

    try:
        spark_context = SparkContext(conf=_get_right_person_spark_config(master_ip))
    except (Exception, ):  # stop any existing contexts, we don't want them...
        logger.info('A spark context exists already on this machine.')
        SparkContext.getOrCreate().stop()
        spark_context = SparkContext(conf=_get_right_person_spark_config(master_ip))

    spark_session = SparkSession(spark_context)
    return spark_session
