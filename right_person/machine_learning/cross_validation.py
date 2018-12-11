#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
functions to cross train models
"""
from itertools import product, repeat

import copy
from operator import itemgetter

from right_person.machine_learning.evaluation import get_best_model, get_information_gain, TRAIN_TEST_RATIO
from right_person.data_mining.profiles.transformations import union_profiles, flat_map_profiles, partition_profiles, \
    map_profile_partitions, collect_profiles
from right_person.machine_learning.utils.functions import get_labelled_profiles, get_shuffled_training_data


def get_hyperparameter_combinations(hyperparams):
    """
    combines model hyperparameters to produce all model combinations
    :param dict[str, list[float]] hyperparams: the hyperparameters to combine
    :rtype: tuple[list[str], list[tuple[float]]]
    :return: a list of hyperparameter names and a list of the hyperparameter combinations
    """
    hyperparam_names = sorted(hyperparams.keys())
    return hyperparam_names, list(product(*[hyperparams[h] for h in hyperparams]))


def get_candidate_models(model, hyperparameters):
    """
    Get a list of candidate models for each hyperparameter combination
    :type model: RightPersonModel
    :type hyperparameters: dict[str, list[float]]
    :rtype: list[RightPersonModel]
    """
    models = []
    hyperparam_names, model_combinations = get_hyperparameter_combinations(hyperparameters)

    for candidate_attributes in model_combinations:
        candidate = copy.deepcopy(model)
        for attr_name, attr_value in zip(hyperparam_names, candidate_attributes):
            setattr(candidate, attr_name, attr_value)
        models.append(candidate)

    return models or [model]


def get_optimised_model(good_profiles, normal_profiles, model, cross_validation_folds, hyperparameters):
    """
    Gets an optimised right_person model for some given profile data, cross validation folds and hyperparameters
    :type good_profiles: list|pyspark.RDD
    :type normal_profiles: list|pyspark.RDD
    :type model: RightPersonModel
    :type cross_validation_folds: int
    :type hyperparameters: dict[str, list[float]]
    :rtype: RightPersonModel|None
    """

    labelled_good_profiles = get_labelled_profiles(good_profiles, 1)
    labelled_normal_profiles = get_labelled_profiles(normal_profiles, 0)

    training_data = union_profiles(labelled_good_profiles, labelled_normal_profiles)
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

        while shuffled_labels[:int(len(training_data) * TRAIN_TEST_RATIO)].count(1) < model.audience_good_size / 2:
            shuffled_profiles, shuffled_labels = get_shuffled_training_data(training_profiles, training_labels, seed)

        information_gain = get_information_gain(shuffled_profiles, shuffled_labels, model)

        return [(model, information_gain)]

    return model_variant_training_function
