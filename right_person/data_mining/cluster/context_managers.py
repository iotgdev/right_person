#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
provides a context manager for interfacing with the right person cluster.

provides a spark session to the user after creating a cluster
then destroys the cluster on namespace completion/error

Usage:
>>> with right_person_cluster_session('job-1') as session:
>>>     # do work, such as run a right person job
>>>     session.sparkContext.parallelize(range(10)).collect()
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
"""
from __future__ import unicode_literals

import logging
from contextlib import contextmanager

from right_person.data_mining.cluster.manager import create_right_person_cluster, destroy_right_person_cluster
from right_person.data_mining.cluster.session import get_new_right_person_spark_session

logger = logging.getLogger('right_person.data_mining.cluster.context_managers')


@contextmanager
def right_person_cluster_session(job_id):
    """
    context manager for a spark session and cluster.
    builds a spark cluster, provides a spark session
    and then destroys the cluster
    :type job_id: str
    :rtype: pyspark.SparkSession
    """
    try:
        master_node_ip = create_right_person_cluster(job_id)
        spark_session = get_new_right_person_spark_session(master_ip=master_node_ip)
        yield spark_session
    except:
        logger.exception('Right person session failed to run to completion!')
        raise
    finally:
        destroy_right_person_cluster(job_id)
