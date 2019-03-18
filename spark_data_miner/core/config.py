from collections import namedtuple

_miner_config = namedtuple('_miner_config', 'name delimiter fields id_field headers s3_bucket s3_prefix')
_miner_field = namedtuple('_miner_field', 'name index rtype stype')


class MinerField(_miner_field):

    __NAME_ERROR_MESSAGE = 'name must be string'
    __INDEX_ERROR_MESSAGE = 'index must be of type int or list[int]'
    __RTYPE_ERROR_MESSAGE = 'rtype must be callable'
    __STYPE_ERROR_MESSAGE = 'stype must be "dict", "set" or None'

    def __new__(cls, name, index, rtype, stype=None):
        assert isinstance(name, str), cls.__NAME_ERROR_MESSAGE
        assert type(index) in (int, list), cls.__INDEX_ERROR_MESSAGE
        if isinstance(index, list):
            assert all(isinstance(arg, int) for arg in index), cls.__INDEX_ERROR_MESSAGE
        else:
            index = [index]
        # noinspection PyBroadException
        try:
            assert callable(eval(rtype)), cls.__RTYPE_ERROR_MESSAGE
        except Exception:
            raise TypeError(cls.__RTYPE_ERROR_MESSAGE)
        assert stype in {'dict', 'set', None}, cls.__STYPE_ERROR_MESSAGE
        # noinspection PyArgumentList
        return super(MinerField, cls).__new__(cls, name, index, rtype, stype)


class MinerConfig(_miner_config):

    __NAME_ERROR_MESSAGE = 'name must be string'
    __DELIMITER_ERROR_MESSAGE = 'delimiter must be a single character'
    __FIELDS_ERROR_MESSAGE = 'fields must be a list of MinerField'
    __ID_FIELD_ERROR_MESSAGE = 'id_field must be an integer'
    __HEADER_ERROR_MESSAGE = 'header must be boolean'
    __S3_BUCKET_ERROR_MESSAGE = 's3 bucket must be string'
    __S3_PREFIX_ERROR_MESSAGE = 's3 prefix must be string'

    def __new__(cls, name, delimiter, fields, id_field, headers, s3_bucket, s3_prefix):
        assert isinstance(name, str), cls.__NAME_ERROR_MESSAGE
        assert isinstance(delimiter, str) and len(str(delimiter)) == 1, cls.__DELIMITER_ERROR_MESSAGE
        try:
            fields = [f if isinstance(f, MinerField) else MinerField(**f) for f in fields]
        except Exception:
            raise TypeError(cls.__FIELDS_ERROR_MESSAGE)
        assert isinstance(id_field, int), cls.__ID_FIELD_ERROR_MESSAGE
        assert isinstance(headers, bool), cls.__HEADER_ERROR_MESSAGE
        assert isinstance(s3_bucket, str), cls.__S3_BUCKET_ERROR_MESSAGE
        assert isinstance(s3_prefix, str), cls.__S3_PREFIX_ERROR_MESSAGE
        # noinspection PyArgumentList
        return super(MinerConfig, cls).__new__(cls, name, delimiter, fields, id_field, headers, s3_bucket, s3_prefix)
