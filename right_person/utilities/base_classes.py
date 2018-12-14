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
from abc import ABCMeta, abstractproperty, abstractmethod

from right_person.machine_learning.training import get_good_filter_function


class AttributeCleaningMetaclass(type):
    """A baseclass providing attribute validation on object change - no bad objects"""

    def __new__(mcs, name, bases, attrs):
        """this function does the work"""
        cls = super(AttributeCleaningMetaclass, mcs).__new__(mcs, name, bases, attrs)
        for attr in attrs.keys():
            if attr.startswith('clean_') and attr[6:]:
                func = getattr(cls, attr)  # handle static/class methods
                attr = attr[6:]
                new_property = mcs.create_property(attr, func)
                setattr(cls, attr, new_property)
        return cls

    @staticmethod
    def create_property(attr, func):
        """makes the property for attribute validation"""

        def getter(self):
            return getattr(self, '__' + attr)

        def setter(self, value):
            try:
                value = func(value)
            except TypeError:  # handle static/class methods
                value = func(self, value)
            return setattr(self, '__' + attr, value)

        # noinspection PyUnusedLocal
        def deleter(self):
            raise AttributeError('cannot delete {}!'.format(attr))

        return property(getter, setter, deleter)


class LogReader(object):

    __metaclass__ = ABCMeta

    @abstractproperty
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

        end_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        for day in range(max_days, 0, -1):
            date = end_date - datetime.timedelta(days=day)
            for record in self.read(date):
                for model in models:
                    if filter_functions[model](record, date):
                        model.good_users.add(record[self.profile_id_field])
