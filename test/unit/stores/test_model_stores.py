#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
from datetime import datetime

import mock

from right_person.stores.model_stores import get_next_version

MODULE_IMPORT_LOCATION = 'right_person.stores.model_stores.'


class TestGetNextVersion(unittest.TestCase):

    def test_logging(self):
        datetime_mock = mock.Mock()
        datetime_mock.date.today.return_value = datetime(2018, 1, 1)
        with mock.patch(MODULE_IMPORT_LOCATION + 'logger') as logger, \
                mock.patch(MODULE_IMPORT_LOCATION + 'datetime', new=datetime_mock):
            self.assertEqual(get_next_version(), "2018-01-01-00")
            self.assertTrue(logger.warning.called)
