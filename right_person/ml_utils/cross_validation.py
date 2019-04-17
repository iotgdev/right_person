#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
functions to cross train models
"""
import copy
from itertools import product


def get_hyperparameter_combinations(hyperparams):
    """
    combines model hyperparameters to produce all model combinations
    :param dict[str, list[float]] hyperparams: the hyperparameters to combine
    :rtype: tuple[list[str], list[tuple[float]]]
    :return: a list of hyperparameter names and a list of the hyperparameter combinations
    """
    hyperparam_names = sorted(hyperparams.keys())
    return hyperparam_names, list(product(*[hyperparams[h] for h in hyperparam_names]))


def get_candidate_models(model, hyperparameters):
    """
    Get a list of candidate models for each hyperparameter combination
    :type model: RightPersonModel
    :type hyperparameters: dict[str, list[float]]
    :rtype: list[RightPersonModel]
    """
    hyperparam_names, model_combinations = get_hyperparameter_combinations(hyperparameters)

    for candidate_attributes in model_combinations:
        candidate = copy.deepcopy(model)
        for attr_name, attr_value in zip(hyperparam_names, candidate_attributes):
            setattr(candidate, attr_name, attr_value)
        yield candidate
    else:
        yield model
