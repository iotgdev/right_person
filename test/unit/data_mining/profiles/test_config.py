#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest

from right_person.data_mining.profiles.config import ProfileFieldConfig


class TestProfileFieldConfig(unittest.TestCase):

    def test_clean_field_type_pass(self):
        self.assertEqual(ProfileFieldConfig.clean_field_type('str'), 'str')

    def test_clean_field_type_fail(self):
        with self.assertRaises(ValueError):
            ProfileFieldConfig.clean_field_type(str)

    def test_clean_field_position_pass(self):
        self.assertEqual(ProfileFieldConfig.clean_field_position(1), [1])

    def test_clean_field_position_fail(self):
        with self.assertRaises(ValueError):
            ProfileFieldConfig.clean_field_position('asdf')

    def test_clean_store_as_fail(self):
        with self.assertRaises(ValueError):
            ProfileFieldConfig.clean_store_as('test')
