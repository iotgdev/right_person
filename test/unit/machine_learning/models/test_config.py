#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals


import unittest

import mock

from right_person.machine_learning.config import RightPersonModelConfig, ModelConfigFilter


class TestRightPersonModelConfig(unittest.TestCase):

    def test_clean_good_definition_fail(self):
        with self.assertRaises(ValueError):
            RightPersonModelConfig.clean_good_definition(mock.Mock())

    def test_clean_audience_fail(self):
        with self.assertRaises(ValueError):
            RightPersonModelConfig.clean_audience(mock.Mock())

    def test_clean_max_ratio_fail(self):
        with self.assertRaises(ValueError):
            RightPersonModelConfig.clean_max_ratio('test')


class TestModelSignatureFilter(unittest.TestCase):

    def test_clean_record_max_age_fail(self):
        with self.assertRaises(ValueError):
            ModelConfigFilter.clean_record_max_age(10.0)
