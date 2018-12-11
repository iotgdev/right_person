#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Functions to train (start to finish, including cross validation) right person models
"""
from __future__ import unicode_literals

import logging


from right_person.machine_learning.cross_validation import get_optimised_model
from right_person.data_mining.profiles.transformations import filter_profiles, sample_profiles, map_profiles, \
    count_profiles


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

    good_profiles = map_profiles(good_set, lambda (user_id, profile): profile)
    normal_profiles = map_profiles(normal_set, lambda (user_id, profile): profile)

    optimised_model = get_optimised_model(
        good_profiles, normal_profiles, model, cross_validation_folds, hyperparameters or {})

    return optimised_model
