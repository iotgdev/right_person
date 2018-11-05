#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
import mock

from right_person.data_mining.cluster.manager import TerraformManager


class TestTerraformManager(unittest.TestCase):

    def test_logging(self):

        manager = TerraformManager(cluster_example=('127.0.0.1', mock.Mock()))

        with mock.patch('right_person.data_mining.cluster.manager.logger') as logger:

            del manager
            self.assertTrue(logger.warning.called)
