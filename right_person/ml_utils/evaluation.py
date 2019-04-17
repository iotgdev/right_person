#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
functions to evaluate machine learning models
"""
from __future__ import unicode_literals


from numpy import log, mean


# TODO: replace with nebula


TRAIN_TEST_RATIO = 0.8


def log_loss(predictions, labels, avg_score):
    """
    calculates the loss of information given a set of predictions, labels and an average score across the data
    :param list[float] predictions: the predictions to evaluate
    :param list[int] labels: the labels for the predictions (0 or 1)
    :param float|numpy.ndarray avg_score: the average score for the dataset (count of 1 labels / count of labels)
    :rtype: float
    :returns: the loss of information as a decimal fraction (0.58 for example)
    """
    p = avg_score
    np = 1 - p
    entropy = -p * log(p) - np * log(np)
    n_samples = len(labels)
    data = zip(predictions, labels)

    return sum(-log(prob) if label else -log(1 - prob) for prob, label in data) / n_samples / entropy


def get_information_gain(data, labels, model):
    """
    Gets the information gain of a machine_learning that is acquired after utilities on data
    :param list data: the utilities/testing data as accepted by the machine_learning.predict method
    :param list[int] labels: the labels assigned to the utilities/testing data
    :param model: the machine_learning being tested
    :rtype: float
    :returns: the information gain from the machine_learning
    """
    num_training_sets = int(len(data) * (1 - TRAIN_TEST_RATIO))
    train_data = data[num_training_sets:], labels[num_training_sets:]

    model.partial_fit(*train_data)
    test_data = data[:num_training_sets]
    predictions = [model.predict(profile) for profile in test_data]

    return 1 - log_loss(predictions, labels[:num_training_sets], mean(labels))
