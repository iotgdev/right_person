#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

from right_person.models.core import RightPersonModel


class TestNewModel(unittest.TestCase):

    def test_weights(self):
        model = RightPersonModel('name', 'account')
        self.assertIsNone(model.weights)

    def test_downsampling_rate(self):
        model = RightPersonModel('name', 'account')
        self.assertEqual(model.downsampling_rate, 1.0)

    def test_sampling_fraction(self):
        model = RightPersonModel('name', 'account')
        self.assertEqual(model.sampling_fraction, 1.0)

    def test_intercept(self):
        model = RightPersonModel('name', 'account')
        self.assertEqual(model.intercept, 0)
