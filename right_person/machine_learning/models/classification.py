#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Provides an interface to convert profile(s) to data
that can be used to train machine learning.
No direct usage is expected.
"""
import mmh3
from collections import Counter
from scipy.sparse import coo_matrix


HASH_SIZE = 1000000


def get_right_person_vector(profile):
    """
    Creates a sparse vector from a profile
    :type profile: dict
    :rtype: list
    """

    features = []

    for feature, values in profile.items():
        features.extend(flatten_profile_feature(feature, values))

    return sorted({mmh3.hash(feature) % HASH_SIZE for feature in features})


def flatten_profile_feature(feature, values):
    """
    Flattens a profile feature of unknown type into hasheable values
    :type feature: str
    :type values: Any
    :rtype: list
    """

    if isinstance(values, (set, Counter)):
        return ['{}-{}'.format(feature, val) for val in values]
    elif isinstance(values, (int, bool)) and values:
        return ['{}-{}'.format(feature, bool(values))]
    else:
        return ['{}-{}'.format(feature, values)]


def combine_vectors(vectors):
    """
    combines many sparse vectors into a training matrix
    :type vectors: list
    :rtype: numpy.array
    """

    column_indexes = sum(vectors, [])
    row_indexes = sum([[i] * len(j) for i, j in enumerate(vectors)], [])
    data = [True] * len(column_indexes)

    matrix = coo_matrix((data, (row_indexes, column_indexes)), shape=(len(vectors), HASH_SIZE), dtype=bool)

    return matrix.tocsr()
