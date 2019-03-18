#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Base class for RightPerson configs.

The AttributeCleaningMetaclass supplies the following behaviour:
1. Class attempts to set an attribute to a value
2. the metaclass searches the class namespace and looks for a corresponding clean_<attribute> method
3. the method runs and cleans the attribute (possibly raising an error)
4. the attribute is set on the class (having been cleaned)

No direct usage is expected.
"""
from __future__ import unicode_literals

import datetime
from abc import ABCMeta, abstractmethod


class LogReader(object):

    __metaclass__ = ABCMeta

    @property
    @abstractmethod
    def profile_id_field(self):
        """indicates which field defines the user_id of the profile"""
        pass

    @abstractmethod
    def read(self, date):
        """method for reading logs for a particular date"""
        return []

    def add_good_user_ids_to_models(self, models):
        """
        bulk process to add good profile_ids to machine_learning
        reads data from log_reader and evaluates if the profile_id should be included
        :type models: list[right_person.machine_learning.models.profile_model.RightPersonModel]
        """
        # machine_learning require different start dates to get the good ids.
        max_days = max([max(f.record_max_age for f in model.config.good_definition) for model in models])

        # to avoid redefining the functions
        filter_functions = {model: get_good_filter_function(model) for model in models}

        end_date = datetime.datetime(2019, 2, 13).replace(hour=0, minute=0, second=0, microsecond=0)
        for day in range(max_days, 0, -1):
            date = end_date - datetime.timedelta(days=day)
            for record in self.read(date):
                for model in models:
                    if filter_functions[model](record, date):
                        model.good_users.add(record[self.profile_id_field])


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
        elif isinstance(profile_value, (dict, set)):
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
