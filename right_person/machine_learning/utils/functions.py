#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A few base classes to interface with the right person models
Not required for the module, should be marked for deprecation.
"""
import random
import datetime

from right_person.data_mining.profiles.transformations import map_profiles


def bulk_add_good_user_ids_to_models(models, log_reader):
    """
    bulk process to add good profile_ids to machine_learning
    reads data from log_reader and evaluates if the profile_id should be included
    :param list[RightPersonModel] models: the machine_learning to update
    :param log_reader:
    :return:
    """
    # machine_learning require different start dates to get the good ids.
    max_days = max([max(f.record_max_age for f in model.config.good_signature) for model in models])

    # to avoid redefining the functions
    filter_functions = {model: model.good_filter_function() for model in models}

    end_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    for day in range(max_days, 0, -1):
        date = end_date - datetime.timedelta(days=1)
        for record in log_reader.read(date):
            for model in models:
                if filter_functions[model](record, date):
                    model.good_profile_ids.add(record[log_reader.profile_id_field])


def get_shuffled_training_data(profiles, labels, seed):
    training_data = zip(profiles, labels)
    random.Random(seed).shuffle(training_data)
    return zip(*training_data)


def get_labelled_profiles(profiles, label):
    return map_profiles(profiles, lambda profile: (profile, label))
