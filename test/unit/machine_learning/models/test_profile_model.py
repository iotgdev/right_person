#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest

from right_person.machine_learning.models.profile_model import RightPersonModel


class TestRightPersonModel(unittest.TestCase):

    def test_model_id_fail(self):
        with self.assertRaises(ValueError):
            getattr(RightPersonModel('test'), 'model_id')
