#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
functions for interacting with profiles.
Since profiles are primarily obtained via spark,
methods are provided to interface with profiles using either a list of pyspark.RDD
"""
import random
from functools import reduce

import pyspark
from collections import defaultdict


def filter_profiles(profiles, filter_fn):
    """
    filter profiles using a function
    :type profiles: pyspark.RDD|list
    :type filter_fn: Callable
    :rtype: pyspark.RDD|list
    """
    if isinstance(profiles, pyspark.RDD):
        return profiles.filter(filter_fn)
    else:
        return filter(filter_fn, profiles)


def sample_profiles(profiles, sample_percentage):
    """
    sample profiles (at some percentage)
    :type profiles: pyspark.RDD|list
    :param float sample_percentage: value between 0 and 1
    :rtype: pyspark.RDD|list
    """
    if isinstance(profiles, pyspark.RDD):
        return profiles.sample(False, sample_percentage)
    else:
        return random.sample(profiles, int(sample_percentage * len(profiles)))


def union_profiles(*profile_iterables):
    """
    takes some number of profile iterables and unions them together
    :type profile_iterables: list[pyspark.RDD|list]
    :rtype: pyspark.RDD|list
    """
    if isinstance(profile_iterables[0], pyspark.RDD):
        return reduce(pyspark.RDD.union, profile_iterables)
    else:
        return sum(map(list, profile_iterables), [])


def map_profiles(profiles, map_fn):
    """
    apply some map function across profiles
    :type profiles: pyspark.RDD|list
    :type map_fn: Callable
    :rtype: pyspark.RDD|list
    """
    if isinstance(profiles, pyspark.RDD):
        return profiles.map(map_fn)
    else:
        return map(map_fn, profiles)


def partition_profiles(profiles, partitions):
    """
    partition profiles into on of n partitions
    :type profiles: pyspark.RDD|list
    :type partitions: int
    :rtype: pyspark.RDD|list
    """
    if isinstance(profiles, pyspark.RDD):
        return profiles.partitionBy(partitions)
    else:
        rval = defaultdict(list)
        for key, profile in profiles:
            rval[hash(key) % partitions].append(profile)
        return dict(rval).items()


def map_profile_partitions(partitioned_profiles, map_fn):
    """
    map partitioned profiles using a function that considered the index of the partition
    :type partitioned_profiles: pyspark.RDD|list
    :type map_fn: Callable
    :rtype: pyspark.RDD|list
    """
    if isinstance(partitioned_profiles, pyspark.RDD):
        return partitioned_profiles.mapPartitionsWithIndex(map_fn)
    else:
        return sum([map_fn(index, partition) for index, partition in partitioned_profiles], [])


def flat_map_profiles(profiles, map_fn):
    """
    in essence create a record for each output of map_fn and combine into a single object
    :type profiles: pyspark.RDD|list
    :type map_fn: Callable
    :rtype: pyspark.RDD|list
    """
    if isinstance(profiles, pyspark.RDD):
        return profiles.flatMap(map_fn)
    else:
        return sum(map(map_fn, profiles), [])


def collect_profiles(profiles):
    """
    collect the profiles (get returnable values)
    :type profiles: pyspark.RDD|list
    :rtype: list
    """
    if isinstance(profiles, pyspark.RDD):
        return profiles.collect()
    return profiles


def count_profiles(profiles):
    """
    count the number of profiles
    :type profiles: pyspark.RDD|list
    :rtype: list
    """
    if isinstance(profiles, pyspark.RDD):
        # noinspection PyArgumentList
        return profiles.count()
    else:
        return len(profiles)


def match_profiles_type(profiles, match):
    """
    returns profiles cast to type of match
    :type profiles: list|pyspark.RDD
    :type match: list|pyspark.RDD
    :rtype: list|pyspark.RDD
    """
    if isinstance(match, pyspark.RDD) and not isinstance(profiles, pyspark.RDD):
        return match.context.parallelize(profiles, len(profiles))
    else:
        return collect_profiles(profiles)
