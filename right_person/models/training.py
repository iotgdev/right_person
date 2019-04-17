#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Functions to train (start to finish, including cross validation) right person models
"""
from __future__ import unicode_literals

import logging
import random
from itertools import repeat

from right_person.ml_utils.data.transformations import filter_profiles, sample_profiles, map_profiles, \
    count_profiles, union_profiles, collect_profiles
from right_person.ml_utils.cross_validation import get_candidate_models
from right_person.ml_utils.evaluation import TRAIN_TEST_RATIO, get_information_gain, get_best_model

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
    good_set = filter_profiles(audience, lambda user_profile: user_profile[0] in model.good_users)

    model.audience_size = count_profiles(audience)
    model.audience_good_size = count_profiles(good_set)

    if not model.audience_good_size:
        logger.exception('model "{}" ({}) cannot be trained - no good users found in audience'.format(
            model.name, model.model_id))
        return

    normal_set = filter_profiles(audience, lambda user_profile: user_profile[0] not in model.good_users)
    normal_sample = sample_profiles(normal_set, model.sampling_fraction)

    labelled_good_profiles = map_profiles(good_set, lambda user_profile: (user_profile[1], 1))
    labelled_normal_profiles = map_profiles(normal_sample, lambda user_profile: (user_profile[1], 0))

    optimised_model = get_optimised_model(
        labelled_good_profiles, labelled_normal_profiles, model, cross_validation_folds, hyperparameters or {})

    return optimised_model


def get_shuffled_training_data(training_data, seed, model):
    """shuffles training data for cross validation"""

    good_limit = model.audience_good_size / 2

    while list(zip(*training_data))[1][:int(len(training_data) * TRAIN_TEST_RATIO)].count(1) < good_limit:
        random.Random(seed).shuffle(training_data)

    return list(zip(*training_data))


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
    # TODO: revisit parallel training

    training_data = collect_profiles(union_profiles(labelled_good, labelled_normal))  # MAX 200K

    model_variants = [
        m for cv_model in repeat(model, cross_validation_folds) for m in get_candidate_models(cv_model, hyperparameters)
    ]

    trained_model_variants = []

    for model_variant_index, variant in enumerate(model_variants):

        seed = int(model_variant_index / cross_validation_folds)

        shuffled_profiles, shuffled_labels = get_shuffled_training_data(training_data, seed, model)
        information_gain = get_information_gain(shuffled_profiles, shuffled_labels, model)
        trained_model_variants.append((variant, information_gain))

    return get_best_model(trained_model_variants)
