#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Provides an interface to convert profile(s) to data
that can be used to train machine learning.
No direct usage is expected.
"""
from __future__ import unicode_literals
import mmh3
from scipy.sparse import coo_matrix


HASH_SIZE = 1000000


def get_right_person_vector(profile, valid_features):
    """
    Creates a sparse vector from a profile
    :type profile: dict
    :type valid_features: list|set
    :rtype: list
    """

    features = set()

    for feature, values in profile.items():
        if feature in valid_features:
            flat_feature = flatten_profile_feature(feature, values)
            features.update([mmh3.hash(f) % HASH_SIZE for f in flat_feature])

    return sorted(features)


def flatten_profile_feature(feature, values):
    """
    Flattens a profile feature of unknown type into hasheable values
    :type feature: str
    :type values: Any
    :rtype: list
    """

    if isinstance(values, (set, dict)):
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
    column_indexes = []
    row_indexes = []

    map(column_indexes.extend, vectors)  # super speedy speed round
    map(row_indexes.extend, ([i] * len(j) for i, j in enumerate(vectors)))

    data = [True] * len(column_indexes)
    matrix = coo_matrix((data, (row_indexes, column_indexes)), shape=(len(vectors), HASH_SIZE), dtype=bool)

    return matrix.tocsr()
