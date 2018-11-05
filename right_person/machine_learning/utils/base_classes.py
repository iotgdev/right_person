#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utility base classes to help interface with the right_person code.

LogReader:
- a log reader base for adding good users (from logs) to right_person models
- interfaces with the bulk_add_good_user_ids_to_models function
"""

from abc import ABCMeta, abstractmethod, abstractproperty


class LogReader(object):

    __metaclass__ = ABCMeta

    @abstractproperty
    def profile_id_field(self):
        """indicates which field defines the user_id of the profile"""
        pass

    @abstractmethod
    def read(self, date):
        """method for reading logs for a particular date"""
        pass
