#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Models for right_person

Usage:
>>> from right_person.models.core import RightPersonModel
>>> model = RightPersonModel('name', 'account')
>>> model.partial_fit([{'test': 1}], [1])
>>> model.predict({'test': 1})
1
"""
from __future__ import unicode_literals

import mmh3 as mmh3
import numpy
from numpy import log
from pyspark.mllib.classification import LogisticRegressionModel
from pyspark.mllib.linalg import SparseVector
from scipy.sparse import coo_matrix
from sklearn.linear_model import LogisticRegression


class RightPersonModel(object):
    """Model for evaluating users based on auction history."""
    MAX_TRAINING_SET_SIZE = 100000

    def __init__(self, name, account, model_id=None, good_users=None, audience_size=0, audience_good_size=0,
                 weights=None, hash_size=1000000, l2reg=1, features=None, created_at=None, updated_at=None):
        self.name = name
        self.model_id = model_id
        self.account = account

        self.classifier = LogisticRegression(C=l2reg, fit_intercept=False, penalty='l2')
        if weights:
            self.classifier.coef_ = numpy.array(weights)
            coefs = self.classifier.coef_[0].tolist()
        else:
            coefs = []

        self.features = features or []

        self.good_users = set(good_users or [])
        self.audience_size = audience_size
        self.audience_good_size = audience_good_size

        self._predictor = LogisticRegressionModel(coefs, self.intercept, hash_size, 2)
        self._predictor.clearThreshold()

        self.created_at = created_at
        self.updated_at = updated_at

    # noinspection PyPropertyDefinition
    l2reg = property(lambda self: self.classifier.C, lambda self, value: setattr(self.classifier, 'C', value))
    hash_size = property(lambda self: self._predictor.numFeatures,
                         lambda self, value: setattr(self._predictor, 'numFeatures', value))

    @property
    def weights(self):
        try:
            return self.classifier.coef_.tolist()
        except AttributeError:
            return None

    @property
    def downsampling_rate(self):
        if not self.audience_size:
            return 1.0
        max_good_ratio = 10.0
        normal_ratio = float(self.MAX_TRAINING_SET_SIZE - self.audience_good_size) / self.audience_size
        good_ratio = float(self.audience_good_size * max_good_ratio) / self.audience_size
        return min(1.0, normal_ratio, good_ratio)

    @property
    def intercept(self):
        if self.downsampling_rate < 1:
            return log(self.downsampling_rate)
        else:
            return 0

    def predict(self, profile):
        """
        Predicts the probability of a profile being "positive"
        :param dict profile: a profile to predict a score for
        :rtype: float
        :return: probability of good
        """
        vector = self.get_right_person_vector(profile, self.features)
        return self._predictor.predict(SparseVector(self.hash_size, sorted(vector), [1] * len(vector)))

    def partial_fit(self, profiles, labels):
        """
        Fit data to the underlying classifier, utilities it
        :param list[dict] profiles: the data_miners to use for utilities
        :param list[int] labels: the corresponding labels (0 or 1) for the data_miners
        """
        vectors = [self.get_right_person_vector(profile, self.features) for profile in profiles]
        matrix = self.combine_vectors(vectors)

        self.classifier.fit(matrix, labels)

        self._predictor = LogisticRegressionModel(
            self.classifier.coef_[0].tolist(), self.intercept, self.hash_size, 2)
        self._predictor.clearThreshold()

    def get_right_person_vector(self, profile, valid_features):
        """
        Creates a sparse vector from a profile
        :type profile: dict
        :type valid_features: list|set
        :rtype: list
        """

        features = set()

        for feature, values in profile.items():
            if feature in valid_features:
                flat_feature = self.flatten_profile_feature(feature, values)
                features.update([mmh3.hash(f) % self.hash_size for f in flat_feature])

        return sorted(features)

    @staticmethod
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

    def combine_vectors(self, vectors):
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
        matrix = coo_matrix((data, (row_indexes, column_indexes)), shape=(len(vectors), self.hash_size), dtype=bool)

        return matrix.tocsr()
