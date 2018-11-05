#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
import mock


from right_person.data_mining.cluster.context_managers import get_spark_cluster_session


MODULE_IMPORT_LOCATION = 'right_person.data_mining.cluster.context_managers.'


class TestGetSparkClusterSession(unittest.TestCase):

    def test_logger_call(self):
        with mock.patch(MODULE_IMPORT_LOCATION + 'create_right_person_cluster') as create, \
                mock.patch(MODULE_IMPORT_LOCATION + 'destroy_right_person_cluster') as destroy, \
                mock.patch(MODULE_IMPORT_LOCATION + 'get_new_right_person_spark_session') as get_session, \
                mock.patch(MODULE_IMPORT_LOCATION + 'logger') as logger:

            with self.assertRaises(AssertionError):
                with get_spark_cluster_session(str('test')):

                    self.assertTrue(create.called)
                    self.assertTrue(get_session.called)
                    raise AssertionError()

            self.assertTrue(logger.exception.called)
            self.assertTrue(destroy.called)
