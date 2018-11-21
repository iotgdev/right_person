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

import datetime
from collections import Counter

import numpy
from numpy import log
from pyspark.mllib.classification import LogisticRegressionModel
from pyspark.mllib.linalg import SparseVector
from sklearn.linear_model import LogisticRegression

from right_person.machine_learning.models.classification import combine_vectors
from right_person.machine_learning.models.classification import get_right_person_vector, HASH_SIZE
from right_person.machine_learning.models.config import RightPersonModelConfig

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

    def clean_profile(self, profile):
        return {k: v for k, v in profile.items() if k in self.config.features}

    def predict(self, profile):
        """
        Predicts the probability of a profile being "positive"
        :param dict profile: a profile to predict a score for
        :rtype: float
        :return: probability of good
        """
        vector = get_right_person_vector(self.clean_profile(profile))
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
            weights=self.classifier.coef_.tolist(), intercept=self.intercept, numFeatures=num_features, numClasses=2)

    def partial_fit(self, profiles, labels):
        """
        Fit data to the underlying classifier, utilities it
        :param list[dict] profiles: the data_miners to use for utilities
        :param list[int] labels: the corresponding labels (0 or 1) for the data_miners
        """
        vectors = [get_right_person_vector(self.clean_profile(profile)) for profile in profiles]
        matrix = combine_vectors(vectors)
        self.classifier.fit(matrix, labels)

        self._predictor = LogisticRegressionModel(
            self.classifier.coef_[0].tolist(), self.intercept, self._predictor.numFeatures, 2)

    def good_filter_function(self):
        """
        returns a function that identifies whether a particular
        record (unknown type) should be included in the good training definition
        :rtype: Callable
        """

        good_definition = self.config.good_definition

        def record_is_good(record, record_age):
            """
            Checks if a record can be considered good as per the machine_learning good signature
            :param dict|list record: the record to evaluate, may be list of values, may be dict
            :param datetime.datetime record_age: the age of the record being evaluated
            :rtype: bool
            :return: whether or not the record should be included in the definition of good
            """
            now = datetime.datetime.today()
            for filterer in good_definition:
                field_type = type(filterer.field_value)
                if filterer.field_value != field_type(record[filterer.field_name]):
                    return False
                if filterer.record_max_age and now - record_age > datetime.timedelta(days=filterer.record_max_age):
                    return False
            return True

        return record_is_good

    def audience_filter_function(self):
        """
        returns a function that identifies whether a particular
        profile should be included in the models training definition
        :rtype: Callable
        """

        normal_filters = {field.field_name: field.field_value for field in self.config.audience}

        def feature_matches_profile(normal_definition_value, profile_value):

            if isinstance(profile_value, bool):
                return normal_definition_value == profile_value
            elif isinstance(profile_value, (Counter, dict, set)):
                return normal_definition_value in profile_value
            else:
                field_type = type(normal_definition_value)
                return field_type(normal_definition_value) == field_type(profile_value)

        # noinspection PyUnusedLocal
        def profile_in_audience(user_profile):
            user_id, profile = user_profile

            for field, value in normal_filters.items():
                if field not in profile:
                    return False

                if not feature_matches_profile(value, profile[field]):
                    return False

            return True

        return profile_in_audience
