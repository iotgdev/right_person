#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Models for right_person

Usage:
>>> model = RightPersonModel('test')
>>> model.partial_fit([{'test': 1}], [1])
>>> model.predict({'test': 1})
1
"""
from __future__ import unicode_literals

import numpy
from numpy import log
from pyspark.mllib.classification import LogisticRegressionModel
from pyspark.mllib.linalg import SparseVector
from sklearn.linear_model import LogisticRegression

from right_person.machine_learning.classification import combine_vectors
from right_person.machine_learning.classification import get_right_person_vector, HASH_SIZE
from right_person.machine_learning.config import RightPersonModelConfig

MAX_TRAINING_SET_SIZE = 100000
ID_DELIMITER = ':'


class RightPersonModel(object):
    """Model used to determine if an individuals auction history is similar to other users"""

    def __init__(self, name):
        self.name = name
        self.index = None
        self.version = None

        self.classifier = LogisticRegression(C=1, fit_intercept=False, penalty='l2')
        self._predictor = LogisticRegressionModel([], 0, HASH_SIZE, 2)
        self._predictor.clearThreshold()

        self.config = RightPersonModelConfig([], [], [], 10.0)

        self.good_users = set()
        self.audience_size = 0
        self.audience_good_size = 0

    # noinspection PyPropertyDefinition
    l2reg = property(lambda self: self.classifier.C, lambda self, value: setattr(self.classifier, 'C', value))

    @property
    def model_id(self):
        try:
            return ID_DELIMITER.join((self.index, self.version))
        except TypeError:
            raise ValueError('This model hasn\'t been registered with a store!')

    @property
    def downsampling_rate(self):
        return min(1,
                   float(MAX_TRAINING_SET_SIZE - self.audience_good_size) / self.audience_size,
                   float(self.audience_good_size * self.config.max_ratio) / self.audience_size
                   )

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
        vector = get_right_person_vector(profile, self.config.features)
        return self._predictor.predict(SparseVector(self._predictor.numFeatures, sorted(vector), [1] * len(vector)))

    def serialize(self):
        """Serializes a machine_learning into a format for storage"""
        return {
            'name': self.name,
            'config': self.config,
            'good_users': self.good_users,
            'audience_size': self.audience_size,
            'audience_good_size': self.audience_good_size,

            'l2reg': self.classifier.C,
            'coef': self.classifier.coef_.tolist(),
            'warm_start': self.classifier.warm_start,

            'num_features': self._predictor.numFeatures,
        }

    def deserialize(self, config, good_users, audience_good_size, audience_size, l2reg, coef, warm_start, num_features):
        """
        Deserialize a machine_learning for storage

        :type config: dict
        :type good_users: list
        :type audience_good_size: int
        :type audience_size: int
        :type l2reg: float
        :type coef: list[list[float]]
        :type warm_start: bool
        :type num_features: int
        """
        self.config = RightPersonModelConfig(**config)
        self.good_users = set(good_users)
        self.audience_good_size = audience_good_size
        self.audience_size = audience_size

        self.classifier = LogisticRegression(warm_start=warm_start, penalty='l2', fit_intercept=False, C=l2reg)
        self.classifier.coef_ = numpy.array(coef)

        self._predictor = LogisticRegressionModel(
            weights=self.classifier.coef_[0].tolist(), intercept=self.intercept, numFeatures=num_features, numClasses=2)
        self._predictor.clearThreshold()

    def partial_fit(self, profiles, labels):
        """
        Fit data to the underlying classifier, utilities it
        :param list[dict] profiles: the data_miners to use for utilities
        :param list[int] labels: the corresponding labels (0 or 1) for the data_miners
        """
        vectors = [get_right_person_vector(profile, self.config.features) for profile in profiles]
        matrix = combine_vectors(vectors)

        self.classifier.fit(matrix, labels)

        self._predictor = LogisticRegressionModel(
            self.classifier.coef_[0].tolist(), self.intercept, self._predictor.numFeatures, 2)
        self._predictor.clearThreshold()
