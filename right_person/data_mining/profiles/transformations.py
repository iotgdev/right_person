#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
functions for interacting with profiles.
Since profiles are primarily obtained via spark,
methods are provided to interface with profiles using either a list of pyspark.RDD
"""
import random

import pyspark
from collections import defaultdict


MIN_RECORDS_FOR_PROFILE = 5
MAX_RECORDS_FOR_PROFILE = 10000


def combine_profiles(profile_1, profile_2):
    """
    aggregate two right_person profiles combining the keys by type
    :type profile_1: dict
    :type profile_2: dict
    :rtype: dict
    """

    if (not profile_1) or (not profile_2) or profile_1['c'] + profile_2['c'] > MAX_RECORDS_FOR_PROFILE:
        return

    for feature, val in profile_2.items():

        if feature in profile_1:

            if isinstance(val, (bool, set)):  # bool must come first since bools are subtypes of ints
                profile_1[feature] |= val

            elif isinstance(val, int):
                profile_1[feature] += val
            elif isinstance(val, dict):
                for i in val:
                    if i in profile_1[feature]:
                        profile_1[feature][i] += val[i]
                    else:
                        profile_1[feature][i] = val[i]

        else:
            profile_1[feature] = val

    return profile_1


def global_filter_profile(profile):
    """
    globally filters the right_person profiles so they meet the requirements of a profile
    :type profile: tuple[str, dict]
    :rtype: bool
    """
    user_id, profile = profile
    return profile and MIN_RECORDS_FOR_PROFILE <= profile['c'] <= MAX_RECORDS_FOR_PROFILE and user_id


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
        return sum([map_fn(index, p) for index, partition in partitioned_profiles for p in partition], [])


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
        profiles = profiles.collect()
    return profiles


def count_profiles(profiles):
    """
    count the number of profiles
    :type profiles: pyspark.RDD|list
    :rtype: list
    """
    if isinstance(profiles, pyspark.RDD):
        return profiles.count()
    else:
        return len(profiles)
