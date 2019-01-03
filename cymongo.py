try:
    from mongoc_api_define import *
except ImportError:
    from .mongoc_api_define import *
try:
    from logger import with_logger
except ImportError:
    from .logger import with_logger
from collections import OrderedDict
import numpy as np
import pandas as pd
import json
import datetime
import time
import gc


class CyMongo:
    mongoc_api = cdll.LoadLibrary("./mongoc_api.so")
    mongoc_api.get_pool.restype = POINTER(c_void_p)
    mongoc_api.get_client.restype = POINTER(c_void_p)
    mongoc_api.get_database.restype = POINTER(c_void_p)
    mongoc_api.get_collection.restype = POINTER(c_void_p)
    mongoc_api.find_as_data_frame.restype = POINTER(DataFrameData)
    mongoc_api.find_as_table.restype = POINTER(Table)

    @staticmethod
    def to_bytes(string, name, accept_none=False):
        if isinstance(string, str):
            return bytes(string, encoding='utf8')
        if isinstance(string, bytes):
            return string
        if not accept_none:
            raise TypeError('the type of "{}" should be str or bytes'.format(name))
        if string is None:
            return None
        raise TypeError('the type of "{}" should be str, bytes or None'.format(name))

    @staticmethod
    def to_str(byte, name, accept_none=False):
        if isinstance(byte, bytes):
            return str(byte, encoding='utf8')
        if isinstance(byte, str):
            return byte
        if not accept_none:
            raise TypeError('the type of "{}" should be bytes or str'.format(name))
        if byte is None:
            return None
        raise TypeError('the type of "{}" should be bytes, str or None'.format(name))

    @classmethod
    def mongoc_uri_has_auth_source(cls, mongoc_uri):
        mongoc_uri = cls.to_str(mongoc_uri, 'mongoc_uri')
        return '?authSource=' in mongoc_uri or '&authSource=' in mongoc_uri

    @classmethod
    def mongoc_uri_append_auth_source(cls, mongoc_uri, db_name, return_bytes=True):
        mongoc_uri = cls.to_str(mongoc_uri, 'mongoc_uri')
        db_name = cls.to_str(db_name, 'db_name')
        if not cls.mongoc_uri_has_auth_source(mongoc_uri):
            if '?' in mongoc_uri:
                mongoc_uri == '&authSource={}'.format(db_name)
            else:
                if not mongoc_uri.endswith('/'):
                    mongoc_uri += '/'
                mongoc_uri += '?authSource={}'.format(db_name)
        return cls.to_bytes(mongoc_uri, 'mongoc_uri') if return_bytes else mongoc_uri

    @staticmethod
    def datetime_as_json_dict(dt, offset_hour=None):
        '''
        transfer python datetime to json (in dict format, used to create bson)
        :param offset_hour:
        :return:
        '''
        if isinstance(offset_hour, int):
            dt += datetime.timedelta(hours=offset_hour)
        return {'$date': int(time.mktime(dt.timetuple())) * 1000 + dt.microsecond // 1000}

    @staticmethod
    def int_datetime_as_json_dict(int_dt, offset_hour=None, tzinfo=None):
        '''
        transfer python int datetime to json (in dict format, used to create bson)
        :param int_dt: for example, 20100101123456789, which is represent as 2010-01-01 12:34:56.789
        :param offset_hour:
        :param tzinfo:
        :return:
        '''
        year = int_dt // 10000000000000
        month = int_dt % 10000000000000 // 100000000000
        day = int_dt % 100000000000 // 1000000000
        hour = int_dt % 1000000000 // 10000000
        minute = int_dt % 10000000 // 100000
        second = int_dt % 100000 // 1000
        microsecond = int_dt % 1000 * 1000
        dt = datetime.datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo)
        if isinstance(offset_hour, int):
            dt += datetime.timedelta(hours=offset_hour)
        return CyMongo.datetime_as_json_dict(dt)


class CyMongoClient(CyMongo):
    def __init__(self, mongoc_uri, tz_aware=False, tzinfo=None, use_client_pool=True, max_pool_size=3):
        if tz_aware and tzinfo:
            try:
                self.__tz_offset_second = c_int(tzinfo._minutes * 60)
            except AttributeError:
                self.__tz_offset_second = c_int(0)
        else:
            self.__tz_offset_second = c_int(0)
        try:
            at_idx = mongoc_uri.index('@')
        except ValueError:
            raise CyMongoException('mongo uri is illegal, it should include "@".')
        split_uri = mongoc_uri.split('/')
        last_slash_idx = -1
        for s in split_uri[: -1]:
            last_slash_idx += len(s) + 1
        if last_slash_idx > at_idx:
            mongoc_uri = mongoc_uri[: last_slash_idx]
            self.__default_db_name = split_uri[-1]
            if self.__default_db_name == '':
                self.__default_db_name = None
        else:
            self.__default_db_name = None
        self.__mongoc_uri = self.to_bytes(mongoc_uri, 'mongoc_uri')
        if self.mongoc_uri_has_auth_source(mongoc_uri):
            self.__mongoc_client_pool, self.__mongoc_client = self.get_mongoc_client(self.__mongoc_uri)
        else:
            self.__mongoc_client_pool, self.__mongoc_client = None, None
        self.__use_client_pool = use_client_pool
        self.__max_pool_size = c_uint32(max_pool_size)
        self.__cymongo_dbs = []
        self.__is_closed = False

    @property
    def is_closed(self):
        return self.__is_closed

    @property
    def tz_offset_second(self):
        return self.__tz_offset_second

    def get_database(self):
        if self.__default_db_name is None:
            raise CyMongoClientHasNoDefaultDatabaseNameException
        return CyMongoDatabase(self.__default_db_name, self, self.__mongoc_client, self.__mongoc_uri,
                               self.__mongoc_client_pool)

    def add_cymongo_db(self, cymongo_db):
        self.__cymongo_dbs.append(cymongo_db)

    @staticmethod
    def __check_error_code(error_code):
        if error_code == ILLEGAL_MONGOC_URI_ERROR_CODE:
            raise CyMongoException('illegal mongo uri')
        if error_code == CREATE_CLIENT_INSTANCE_ERROR_CODE:
            raise CyMongoException('create client instance failed')

    def get_mongoc_client(self, mongoc_uri):
        mongoc_uri = self.to_bytes(mongoc_uri, 'mongoc_uri')
        error_code = c_int()
        if self.__use_client_pool and self.__mongoc_client_pool is None:
            self.__mongoc_client_pool = self.mongoc_api.get_pool(mongoc_uri, byref(error_code), self.__max_pool_size)
            self.__check_error_code(error_code.value)
        self.__mongoc_client = self.mongoc_api.get_client(mongoc_uri, self.__mongoc_client_pool, byref(error_code))
        self.__check_error_code(error_code.value)
        return self.__mongoc_client_pool, self.__mongoc_client

    def close(self):
        if self.__is_closed:
            raise CyMongoClientIsClosedException
        self.__is_closed = True
        for cymongo_db in self.__cymongo_dbs:
            for cymongo_collection in cymongo_db.cymongo_collections:
                self.mongoc_api.close_collection(cymongo_collection.mongoc_collection)
            self.mongoc_api.close_database(cymongo_db.mongoc_db)
        self.mongoc_api.close_client(self.__mongoc_client_pool, self.__mongoc_client)
        self.mongoc_api.close_pool(self.__mongoc_client_pool)
        self.__mongoc_client_pool = None
        self.__mongoc_client = None
        gc.collect()

    def __getitem__(self, db_name):
        if self.__is_closed:
            raise CyMongoClientIsClosedException
        return CyMongoDatabase(db_name, self, self.__mongoc_client, self.__mongoc_uri, self.__mongoc_client_pool)


class CyMongoDatabase(CyMongo):
    def __init__(self, db_name, cymongo_client, mongoc_client=None, mongoc_uri=None, mongoc_client_pool=None):
        self.__cymongo_client = cymongo_client
        if mongoc_client is None:
            if mongoc_uri is None:
                raise CyMongoException('"mongoc_client" and "mongoc_uri" cannot be both None')
            self.__mongoc_uri = self.mongoc_uri_append_auth_source(mongoc_uri, db_name)
            self.__mongoc_client_pool, self.__mongoc_client = self.__cymongo_client.get_mongoc_client(self.__mongoc_uri)
        else:
            self.__mongoc_client = mongoc_client
            self.__mongoc_client_pool = mongoc_client_pool
        self.__db_name = self.to_bytes(db_name, 'db_name')
        self.__mongoc_db = self.mongoc_api.get_database(self.__mongoc_client, self.__db_name)
        self.__cymongo_collections = []
        cymongo_client.add_cymongo_db(self)

    @property
    def mongoc_db(self):
        return self.__mongoc_db

    @property
    def cymongo_client(self):
        return self.__cymongo_client

    @property
    def cymongo_collections(self):
        return self.__cymongo_collections

    def add_cymongo_collection(self, cymongo_collection):
        self.__cymongo_collections.append(cymongo_collection)

    def __getitem__(self, collection_name):
        if self.__cymongo_client.is_closed:
            raise CyMongoClientIsClosedException
        return CyMongoCollection(self, self.__cymongo_client, self.__mongoc_client, self.__db_name, collection_name,
                                 self.__mongoc_client_pool)


@with_logger
class CyMongoCollection(CyMongo):
    __supported_number_types = ['date_time', 'int32', 'int64', 'float64', 'bool']
    __supported_types = ['string'] + __supported_number_types
    __supported_bson_types = (BSON_TYPE_UTF8, BSON_TYPE_DATE_TIME, BSON_TYPE_INT32,
                              BSON_TYPE_INT64, BSON_TYPE_DOUBLE, BSON_TYPE_BOOL)
    __supported_bson_types_to_types = {bson_type: _type
                                       for bson_type, _type in zip(__supported_bson_types, __supported_types)}
    __sys_addr_byte_cnt = 8
    __default_int32_nan_value = -2**31
    __default_int64_nan_value = -2**63
    __default_date_time_nan_value = 0
    __default_bool_value = False

    @property
    def default_int32_nan_value(self):
        return self.__default_int32_nan_value

    @property
    def default_int64_nan_value(self):
        return self.__default_int64_nan_value

    @property
    def default_date_time_nan_value(self):
        return self.__default_date_time_nan_value

    @property
    def default_bool_value(self):
        return self.__default_bool_value

    def __init__(self, cymongo_db, cymongo_client, mongoc_client, db_name, collection_name, mongoc_client_pool=None):
        self.__cymongo_client = cymongo_client
        self.__mongoc_client = mongoc_client
        self.__db_name = self.to_bytes(db_name, 'db_name')
        self.__collection_name = self.to_bytes(collection_name, 'collection_name')
        self.__mongoc_collection = self.mongoc_api.get_collection(self.__mongoc_client, self.__db_name,
                                                                  self.__collection_name)
        self.__index_key = None
        self.__column_key = None
        self.__value_keys = []
        self.__data_frame_info = None
        self.__column_names = None
        self.__table_info = None
        self.__options = None
        self.__debug = False
        # keep int (-2^31 or -2^63) when it is nan, or transform all values to float64
        self.__keep_int_when_has_nan_value = True
        self.__nan_process_method = DefaultNanValue(
            c_int32(self.__default_int32_nan_value),  # default_int32_nan_value
            c_int64(self.__default_int64_nan_value),  # default_int64_nan_value
            c_int64(self.__default_date_time_nan_value),  # default_date_time_nan_value, it is just zero in int datetime, not 1970-01-01 00:00:00.000
            c_bool(self.__default_bool_value)  # default_bool_value
        )
        self.__mongoc_client_pool = mongoc_client_pool
        cymongo_db.add_cymongo_collection(self)

    @property
    def mongoc_collection(self):
        return self.__mongoc_collection

    @classmethod
    def bson_type_to_type(cls, bson_type):
        return cls.__supported_bson_types_to_types.get(bson_type)

    def enable_debug(self):
        self.__debug = True

    def disable_debug(self):
        self.__debug = False

    def set_data_frame_info(self, index_key, column_key, value_keys):
        self.__index_key = index_key
        self.__column_key = column_key
        self.__value_keys = list(value_keys)
        if self.__index_key in self.__value_keys or self.__column_key in self.__value_keys:
            raise ValueError('index_key or column_key cannot in value_keys.')
        if len(self.__value_keys) == 0:
            raise ValueError('The count of elements in value_keys cannot be 0.')
        projection = OrderedDict()
        projection[self.__index_key] = True
        projection[self.__column_key] = True
        index_key = self.to_bytes(index_key, 'index_key', accept_none=True)
        column_key = self.to_bytes(column_key, 'column_key', accept_none=True)
        byte_value_keys = []
        value_types = []
        for value_key in self.__value_keys:
            byte_value_keys.append(self.to_bytes(value_key, 'element of value_keys'))
            value_types.append(c_int(BSON_TYPE_UNKNOWN))
            projection[value_key] = True
        self.__options = self.to_bytes(json.dumps({'projection': projection}), 'projection')
        value_cnt = len(byte_value_keys)
        c_char_p_array = c_char_p * value_cnt
        c_int_array = c_int * value_cnt
        self.__data_frame_info = DataFrameInfo(
            c_char_p(index_key),
            c_int(BSON_TYPE_UNKNOWN),
            c_char_p(column_key),
            c_int(BSON_TYPE_UNKNOWN),
            c_char_p_array(*byte_value_keys),
            c_int_array(*value_types),
            c_uint(value_cnt)
        )

    def set_table_info(self, column_names, sort=None):
        self.__column_names = list(column_names)
        if len(self.__column_names) == 0:
            raise ValueError('The count of elements in column_names cannot be 0.')
        byte_column_names = []
        column_types = []
        projection = OrderedDict()
        for column_name in self.__column_names:
            byte_column_names.append(self.to_bytes(column_name, 'element of column_names'))
            column_types.append(c_int(BSON_TYPE_UNKNOWN))
            projection[column_name] = True
        options = {'projection': projection}
        if sort is not None:
            options['sort'] = sort
        self.__options = self.to_bytes(json.dumps(options), 'options')
        column_cnt = len(byte_column_names)
        c_char_p_array = c_char_p * column_cnt
        c_int_array = c_int * column_cnt
        self.__table_info = TableInfo(
            c_char_p_array(*byte_column_names),
            c_int_array(*column_types),
            c_uint(column_cnt)
        )

    def set_nan_process_method(self, keep_int_when_has_nan_value=True, default_int32_nan_value=None,
                               default_int64_nan_value=None, default_date_time_nan_value=None,
                               default_bool_value=None):
        self.__keep_int_when_has_nan_value = keep_int_when_has_nan_value
        if default_int32_nan_value is None:
            default_int32_nan_value = self.__default_int32_nan_value
        else:
            self.__default_int32_nan_value = default_int32_nan_value
        if default_int64_nan_value is None:
            default_int64_nan_value = self.__default_int64_nan_value
        else:
            self.__default_int64_nan_value = default_int64_nan_value
        if default_date_time_nan_value is None:
            default_date_time_nan_value = self.__default_date_time_nan_value
        else:
            self.__default_date_time_nan_value = default_date_time_nan_value
        if default_bool_value is None:
            default_bool_value = self.__default_bool_value
        else:
            self.__default_bool_value = default_bool_value
        self.__nan_process_method = DefaultNanValue(
            c_int32(default_int32_nan_value),
            c_int64(default_int64_nan_value),
            c_int64(default_date_time_nan_value),
            c_bool(default_bool_value)
        )

    def __get_index_or_column(self, data_frame_data, index_or_column):
        for array_type in self.__supported_types:
            ctype_array = getattr(data_frame_data.contents, '{}_{}_array'.format(array_type, index_or_column))
            if ctype_array:
                if array_type == 'string':
                    str_len = getattr(data_frame_data.contents, 'string_{}_max_length'.format(index_or_column))
                    numpy_array = np.ctypeslib.as_array(
                            ctype_array, shape=(data_frame_data.contents.col_cnt, str_len)).copy()
                    numpy_array.dtype = 'U{}'.format(str_len)
                    numpy_array.shape = (numpy_array.shape[0], )
                    return numpy_array
                return np.ctypeslib.as_array(ctype_array, shape=(
                    getattr(data_frame_data.contents, 'row_cnt' if index_or_column == 'index' else 'col_cnt'), )).copy()

    @classmethod
    def __get_offset_c_pointer(cls, c_pointer, offset):
        return cast(c_void_p(cast(c_pointer, c_void_p).value + offset * cls.__sys_addr_byte_cnt), type(c_pointer))

    def __transfer_int_array_with_nan_to_float_array(self, array):
        if array.dtype in [np.int32, np.int64] and not self.__keep_int_when_has_nan_value:
            if array.dtype == np.int32 and self.__default_int32_nan_value in array:
                nan_mask = array == self.__default_int32_nan_value
                array = array.astype(np.float64)
                array[nan_mask] = np.nan
            elif array.dtype == np.int64 and self.__default_int64_nan_value in array:
                nan_mask = array == self.__default_int64_nan_value
                array = array.astype(np.float64)
                array[nan_mask] = np.nan
        return array

    def __get_values(self, data_frame_data):
        values = OrderedDict()
        for value_idx, value_key in enumerate(self.__value_keys):
            row_cnt = data_frame_data.contents.row_cnt
            col_cnt = data_frame_data.contents.col_cnt
            array_type = self.bson_type_to_type(self.__data_frame_info.value_types[value_idx])
            if array_type is None:
                continue
            c_arrays_p_p = getattr(data_frame_data.contents, '{}_value_arrays'.format(array_type))
            c_array_p = self.__get_offset_c_pointer(c_arrays_p_p, value_idx).contents
            if array_type == 'string':
                string_max_length = self.__get_offset_c_pointer(
                    data_frame_data.contents.string_value_max_lengths, value_idx).contents.value
                value_array = np.ctypeslib.as_array(c_array_p, shape=(row_cnt, col_cnt, string_max_length)).copy()
                value_array.dtype = 'U{}'.format(string_max_length)
                value_array.shape = (value_array.shape[0], value_array.shape[1])
                values[value_key] = value_array
            elif array_type in self.__supported_number_types:
                value_array = np.ctypeslib.as_array(c_array_p, shape=(row_cnt, col_cnt)).copy()
                value_array = self.__transfer_int_array_with_nan_to_float_array(value_array)
                values[value_key] = value_array
            else:
                raise TypeError('Unknown array_type: {}'.format(array_type))
        return values

    def __get_table(self, c_table):
        table = OrderedDict()
        for idx in range(self.__table_info.column_cnt):
            dtype = self.bson_type_to_type(self.__table_info.column_types[idx])
            if dtype is None:
                self.logger.warning('Got unknown column type, maybe the connection to MongoDB lost or the column name '
                                    '"{}" is not correct.'.format(self.__column_names[idx]))
                continue
            c_array = getattr(c_table.contents, '{}_columns'.format(dtype))[idx]
            if c_array:
                if dtype == 'string':
                    max_length = c_table.contents.string_column_max_lengths[idx]
                    array = np.ctypeslib.as_array(c_array, shape=(c_table.contents.row_cnt, max_length)).copy()
                    array.dtype = 'U{}'.format(max_length)
                    array.shape = (array.shape[0], )
                else:
                    array = np.ctypeslib.as_array(c_array, shape=(c_table.contents.row_cnt, )).copy()
                    array = self.__transfer_int_array_with_nan_to_float_array(array)
                table[self.__column_names[idx]] = array
        return table

    def find(self, filter=None, return_type='table'):
        if self.__debug:
            begin = time.time()
        if isinstance(filter, dict):
            filter = json.dumps(filter)
        if isinstance(filter, str):
            filter = self.to_bytes(filter, 'filter')
        if return_type == 'data_frame':
            data_frame_data = None
            try:
                data_frame_data = self.mongoc_api.find_as_data_frame(
                        self.__mongoc_collection, pointer(self.__nan_process_method), pointer(self.__data_frame_info),
                        filter, self.__options, self.__cymongo_client.tz_offset_second, self.__debug)
                if self.__debug:
                    self.logger.debug('get_data_from_c cost: {}'.format(time.time() - begin))
                index = self.__get_index_or_column(data_frame_data, 'index')
                index = pd.Index(index, name=self.__index_key)
                if self.__debug:
                    self.logger.debug('get df index cost: {}'.format(time.time() - begin))
                    self.logger.debug(index)
                column = self.__get_index_or_column(data_frame_data, 'column')
                column = pd.Index(column, name=self.__column_key)
                if self.__debug:
                    self.logger.debug('get df column cost: {}'.format(time.time() - begin))
                    self.logger.debug(column)
                values = self.__get_values(data_frame_data)
                if self.__debug:
                    self.logger.debug('get df values cost: {}'.format(time.time() - begin))
                dfs = OrderedDict()
                for value_key, value in values.items():
                    dfs[value_key] = pd.DataFrame(value, index=index, columns=column)
            except:
                dfs = OrderedDict()
            finally:
                if data_frame_data is not None:
                    self.mongoc_api.free_data_frame_memory(data_frame_data, c_uint(len(self.__value_keys)))
                if self.__debug:
                    self.logger.debug('create dfs cost: {}'.format(time.time() - begin))
            return dfs
        if return_type == 'table':
            c_table = self.mongoc_api.find_as_table(
                    self.__mongoc_collection, pointer(self.__nan_process_method), pointer(self.__table_info),
                    filter, self.__options, self.__cymongo_client.tz_offset_second, self.__debug)
            if self.__debug:
                self.logger.debug('get_data_from_c cost: {}'.format(time.time() - begin))
            table = self.__get_table(c_table)
            if self.__debug:
                self.logger.debug('transfer data into dict cost: {}'.format(time.time() - begin))
            df = pd.DataFrame(table)
            self.mongoc_api.free_table_memory(c_table, c_uint(self.__table_info.column_cnt))
            if self.__debug:
                self.logger.debug('create df cost: {}'.format(time.time() - begin))
            return df
        raise ValueError('Unknown return_type: {}.'.format(return_type))


class CyMongoException(Exception):
    pass


class CyMongoUriIllegalException(CyMongoException):
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)


class CyMongoClientIsClosedException(CyMongoException):
    def __init__(self, *args, **kwargs):
        self.message = 'Cannot support this operation, when the client is closed.'
        self.args = (self.message, )
        super().__init__(args, kwargs)


class CyMongoClientHasNoDefaultDatabaseNameException(CyMongoException):
    def __init__(self, *args, **kwargs):
        self.message = 'No default database defined.'
        self.args = (self.message, )
        super().__init__(args, kwargs)
