#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
import mock
import requests

from right_person.data_mining.cluster.utils import SparkPackageManager, get_current_ipv4

MODULE_IMPORT_LOCATION = 'right_person.data_mining.cluster.utils.'


class TestSparkPackageManager(unittest.TestCase):

    def test_logging(self):

        manager = SparkPackageManager(cluster_example=('127.0.0.1', mock.Mock()))

        with mock.patch(MODULE_IMPORT_LOCATION + 'logger') as logger:

            del manager
            self.assertTrue(logger.warning.called)


class TestGetCurrentIpv4(unittest.TestCase):

    def test_logging(self):

        requests_mock = mock.Mock(get=mock.Mock(side_effect=[requests.ConnectTimeout, mock.Mock()]))
        requests_mock.ConnectTimeout = requests.ConnectTimeout

        with mock.patch(MODULE_IMPORT_LOCATION + 'requests', new=requests_mock):
            with mock.patch(MODULE_IMPORT_LOCATION + 'logger') as logger:

                get_current_ipv4()
                self.assertTrue(logger.warning.called)
