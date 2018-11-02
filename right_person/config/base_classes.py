#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Base classes for RightPerson configs.

- An attribute cleaning metaclass for config validation
- A Json baseclass for serialisation

No direct usage is expected.
"""
from __future__ import unicode_literals


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
