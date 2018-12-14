#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Functions to train (start to finish, including cross validation) right person models
"""
from __future__ import unicode_literals

import logging
import random
from collections import Counter
from itertools import repeat
from operator import itemgetter

import datetime

from right_person.data_mining.profiles.transformations import filter_profiles, sample_profiles, map_profiles, \
    count_profiles, union_profiles, flat_map_profiles, partition_profiles, map_profile_partitions, collect_profiles
from right_person.machine_learning.cross_validation import get_candidate_models
from right_person.machine_learning.evaluation import TRAIN_TEST_RATIO, get_information_gain, get_best_model

logger = logging.getLogger('right_person.machine_learning.training')


def train_model(audience, model, cross_validation_folds=1, hyperparameters=None):
    """
    Train a right person model for some given audience and machine learning parameters
    :param list|pyspark.RDD audience: the audience (list of users and profiles) to use as a basis for training
    :type model: RightPersonModel
    :type cross_validation_folds: int
    :type hyperparameters: dict[str, list[float]]
    :rtype: RightPersonModel
    """
    model.audience_size = count_profiles(audience)

    # we have to filter the good users from the target group for training purposes
    good_set = filter_profiles(audience, lambda (user_id, profile): user_id in model.good_users)
    model.audience_good_size = count_profiles(good_set)

    if not model.audience_good_size:
        print model.audience_good_size
        logger.exception('model {} cannot be trained - no good users found in audience'.format(model.name))
        return

    normal_set = sample_profiles(audience, model.downsampling_rate)

    labelled_good_profiles = map_profiles(good_set, lambda (user_id, profile): (profile, 1))
    labelled_normal_profiles = map_profiles(normal_set, lambda (user_id, profile): (profile, 0))

    optimised_model = get_optimised_model(
        labelled_good_profiles, labelled_normal_profiles, model, cross_validation_folds, hyperparameters or {})

    return optimised_model


def get_shuffled_training_data(profiles, labels, seed):
    """shuffles training data for cross validation"""
    training_data = zip(profiles, labels)
    random.Random(seed).shuffle(training_data)
    return zip(*training_data)


def get_model_variant_training_function(models, cross_validation_folds):
    """
    Provides a model specific function for training and evaluating a model.
    :type models: list[RightPersonModel]
    :type cross_validation_folds: float
    :rtype: Callable
    """

    def model_variant_training_function(model_variant_index, training_data):

        training_profiles, training_labels = zip(*map(itemgetter(1), training_data))

        seed = model_variant_index % cross_validation_folds
        model = models[int(model_variant_index / cross_validation_folds)]

        shuffled_profiles, shuffled_labels = get_shuffled_training_data(training_profiles, training_labels, seed)

        while shuffled_labels[:int(len(training_profiles) * TRAIN_TEST_RATIO)].count(1) < model.audience_good_size / 2:
            shuffled_profiles, shuffled_labels = get_shuffled_training_data(training_profiles, training_labels, seed)

        information_gain = get_information_gain(shuffled_profiles, shuffled_labels, model)

        return [(model, information_gain)]

    return model_variant_training_function


def get_optimised_model(labelled_good, labelled_normal, model, cross_validation_folds, hyperparameters):
    """
    Gets an optimised right_person model for some given profile data, cross validation folds and hyperparameters
    :type labelled_good: list|pyspark.RDD
    :type labelled_normal: list|pyspark.RDD
    :type model: RightPersonModel
    :type cross_validation_folds: int
    :type hyperparameters: dict[str, list[float]]
    :rtype: RightPersonModel|None
    """

    training_data = union_profiles(labelled_good, labelled_normal)
    # todo: investigate shuffling the data here

    model_variants = [
        cv_model for model in get_candidate_models(model, hyperparameters)
        for cv_model in repeat(model, cross_validation_folds)
    ]

    total_training_data = flat_map_profiles(training_data, lambda data: [(i, data) for i in range(len(model_variants))])

    model_variant_training_data = partition_profiles(total_training_data, len(model_variants))
    model_variant_training_function = get_model_variant_training_function(model_variants, cross_validation_folds)

    trained_model_variants = map_profile_partitions(model_variant_training_data, model_variant_training_function)

    return get_best_model(collect_profiles(trained_model_variants))


def get_good_filter_function(model):
    """
    returns a function that identifies whether a particular
    record (unknown type) should be included in the good training definition
    :rtype: Callable
    """

    good_definition = model.config.good_definition

    def filter_is_good(filterer, record, record_age):
        """
        Checks if a record passes a good filter check
        :type filterer: right_person.machine_learning.models.config.ModelConfigFilter
        :type record: dict|list|tuple
        :type record_age: datetime.datetime
        :rtype: bool
        """
        now = datetime.datetime.today()
        field_type = type(filterer.field_value)

        if filterer.field_value != field_type(record[filterer.field_name]):
            return False
        if filterer.record_max_age and now - record_age > datetime.timedelta(days=filterer.record_max_age):
            return False
        return True

    def record_is_good(record, record_age):
        """
        Checks if a record can be considered good as per the machine_learning good signature
        :param dict|list record: the record to evaluate, may be list of values, may be dict
        :param datetime.datetime record_age: the age of the record being evaluated
        :rtype: bool
        :return: whether or not the record should be included in the definition of good
        """
        return any(filter_is_good(filterer, record, record_age) for filterer in good_definition)

    return record_is_good


def get_audience_filter_function(self):
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
