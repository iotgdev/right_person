#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals


import unittest

import mock

from right_person.machine_learning.models.config import RightPersonModelConfig, ModelSignatureFilter


class TestRightPersonModelConfig(unittest.TestCase):

    def test_clean_good_signature_fail(self):
        with self.assertRaises(ValueError):
            RightPersonModelConfig.clean_good_signature(mock.Mock())

    def test_clean_normal_signature_fail(self):
        with self.assertRaises(ValueError):
            RightPersonModelConfig.clean_normal_signature(mock.Mock())

    def test_clean_max_ratio_fail(self):
        with self.assertRaises(ValueError):
            RightPersonModelConfig.clean_max_ratio('test')


class TestModelSignatureFilter(unittest.TestCase):

    def test_clean_record_max_age_fail(self):
        with self.assertRaises(ValueError):
            ModelSignatureFilter.clean_record_max_age(10.0)
