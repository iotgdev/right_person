#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
RightPersonTrainingJob config objects
The configs contain information required to run a right_person job.

- ProfileDocumentConfig contains the information required to build a profile
- ProfileFieldConfig defines a single field in a profile document.

These objects should only be used by a RightPersonTrainingJob
"""
from __future__ import unicode_literals


from right_person.utilities.base_classes import AttributeCleaningMetaclass


class ProfileDocumentConfig(object):
    """Config for right person job to process a file via cluster"""
    __metaclass__ = AttributeCleaningMetaclass

    def __init__(self, doc_name, delimiter, fields, profile_id_field, files_contain_headers, s3_bucket, s3_prefix):
        """
        :param str delimiter: a single character delimiter for processing log files
        :param list[ProfileFieldConfig] fields: the fields to process from the document
        :param ProfileFieldConfig profile_id_field: the field that defines the id of the profiles
        :param files_contain_headers: when true indicates that the first line of the file should be ignored
        :param str s3_bucket: the name of the s3 bucket containing the files
        :param str s3_prefix: the location on th s3 bucket (containing python datetime format strings) of the files
        """
        self.doc_name = doc_name
        self.delimiter = delimiter
        self.fields = fields
        self.profile_id_field = profile_id_field
        self.files_contain_headers = files_contain_headers
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix  # should include time settings

    @staticmethod
    def clean_delimiter(delimiter):
        if isinstance(delimiter, unicode):
            delimiter = str(delimiter)
        if len(delimiter) != 1:
            raise ValueError('delimiter should be single character!')
        return delimiter

    @staticmethod
    def clean_fields(fields):
        try:
            return [f if isinstance(f, ProfileFieldConfig) else ProfileFieldConfig(**f) for f in fields]
        except:
            raise ValueError('invalid fields! should be list of {}'.format(ProfileFieldConfig.__name__))

    @staticmethod
    def clean_doc_type(doc_type):
        if not isinstance(doc_type, basestring):
            raise ValueError('doc_type must be string!')


class ProfileFieldConfig(object):
    """A config object outlining relevant fields in a document used to build profiles"""
    __metaclass__ = AttributeCleaningMetaclass

    def __init__(self, field_name, field_position, field_type, store_as=None):
        """
        :param str field_name: the full name of the field to add to the profile
        :param list[int] field_position: the field indexes to derive the field from
        :param str field_type: the string representation of a python executable to create the field value.
        :param str|None store_as: the type of storage in the profile (currently supported are "Counter", "set", None)
        """
        self.field_name = field_name
        self.field_position = field_position
        self.field_type = field_type
        self.store_as = store_as

    @staticmethod
    def clean_field_name(field_name):
        try:
            return str(field_name)
        except:
            raise ValueError('field_name must be string!')

    @staticmethod
    def clean_field_type(field_type):
        try:
            if callable(eval(field_type)):
                return field_type
            raise
        except:
            raise ValueError('invalid field_type: {}.'.format(field_type))

    @staticmethod
    def clean_field_position(field_position):
        if isinstance(field_position, (int, str)):
            field_position = [field_position]
        return field_position

    @staticmethod
    def clean_store_as(stored_as):
        valid_values = {'Counter', 'set', None}
        if stored_as not in valid_values:
            raise ValueError('invalid value for stored_as! should be in {}'.format(tuple(valid_values)))
        return stored_as
