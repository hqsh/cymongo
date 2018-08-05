from mongoc_api_define import *
from collections import OrderedDict
import numpy as np
import pandas as pd


class CyMongo:
    mongoc_api = cdll.LoadLibrary("./mongoc_api.so")
    mongoc_api.get_client.restype = POINTER(c_void_p)
    mongoc_api.get_database.restype = POINTER(c_void_p)
    mongoc_api.get_collection.restype = POINTER(c_void_p)
    mongoc_api.find_as_data_frame.restype = POINTER(DataFrameData)
    mongoc_api.find.restype = POINTER(Table)

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


class CyMongoClient(CyMongo):
    def __init__(self, mongoc_uri):
        self.__mongoc_uri = self.to_bytes(mongoc_uri, 'mongoc_uri')
        if self.mongoc_uri_has_auth_source(mongoc_uri):
            self.__mongoc_client = self.get_mongoc_client(self.__mongoc_uri)
        else:
            self.__mongoc_client = None

    @classmethod
    def get_mongoc_client(cls, mongoc_uri):
        mongoc_uri = cls.to_bytes(mongoc_uri, 'mongoc_uri')
        error_code = c_int()
        mongoc_client = cls.mongoc_api.get_client(mongoc_uri, byref(error_code))
        if error_code.value == ILLEGAL_MONGOC_URI_ERROR_CODE:
            raise CyMongoException('illegal mongo uri')
        if error_code.value == CREATE_CLIENT_INSTANCE_ERROR_CODE:
            raise CyMongoException('create client instance failed')
        return mongoc_client

    def __getitem__(self, db_name):
        return CyMongoDatabase(db_name, self.__mongoc_client, self.__mongoc_uri)


class CyMongoDatabase(CyMongo):
    def __init__(self, db_name, mongoc_client=None, mongoc_uri=None):
        if mongoc_client is None:
            if mongoc_uri is None:
                raise CyMongoException('"mongoc_client" and "mongoc_uri" cannot be both None')
            self.__mongoc_uri = self.mongoc_uri_append_auth_source(mongoc_uri, db_name)
            self.__mongoc_client = CyMongoClient.get_mongoc_client(self.__mongoc_uri)
        else:
            self.__mongoc_client = mongoc_client
        self.__db_name = self.to_bytes(db_name, 'db_name')
        self.__mongoc_db = self.mongoc_api.get_database(self.__mongoc_client, self.__db_name)

    def __getitem__(self, collection_name):
        return CyMongoCollection(self.__mongoc_client, self.__db_name, collection_name)


class CyMongoCollection(CyMongo):
    __supported_number_types = ['date_time', 'int32', 'int64', 'float64', 'bool']
    __supported_types = ['string'] + __supported_number_types
    __supported_bson_types = (BSON_TYPE_UTF8, BSON_TYPE_DATE_TIME, BSON_TYPE_INT32,
                              BSON_TYPE_INT64, BSON_TYPE_DOUBLE, BSON_TYPE_BOOL)
    __supported_bson_types_to_types = {bson_type: _type
                                       for bson_type, _type in zip(__supported_bson_types, __supported_types)}
    __sys_addr_byte_cnt = 8

    def __init__(self, mongoc_client, db_name, collection_name):
        self.__mongoc_client = mongoc_client
        self.__db_name = self.to_bytes(db_name, 'db_name')
        self.__collection_name = self.to_bytes(collection_name, 'collection_name')
        self.__mongoc_collection = self.mongoc_api.get_collection(self.__mongoc_client, self.__db_name,
                                                                  self.__collection_name)
        self.__index_key = None
        self.__column_key = None
        self.__value_keys = None
        self.__data_frame_info = None
        self.__column_names = None
        self.__table_info = None
        self.__debug = False

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
        index_key = self.to_bytes(index_key, 'index_key', accept_none=True)
        column_key = self.to_bytes(column_key, 'column_key', accept_none=True)
        byte_value_keys = []
        value_types = []
        for value_key in self.__value_keys:
            byte_value_keys.append(self.to_bytes(value_key, 'element of value_keys'))
            value_types.append(c_int(BSON_TYPE_UNKNOWN))
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

    def set_table_info(self, column_names):
        self.__column_names = column_names
        byte_column_names = []
        column_types = []
        for column_name in self.__column_names:
            byte_column_names.append(self.to_bytes(column_name, 'element of column_names'))
            column_types.append(c_int(BSON_TYPE_UNKNOWN))
        column_cnt = len(byte_column_names)
        c_char_p_array = c_char_p * column_cnt
        c_int_array = c_int * column_cnt
        self.__table_info = TableInfo(
            c_char_p_array(*byte_column_names),
            c_int_array(*column_types),
            c_uint(column_cnt)
        )

    def __get_index_or_column(self, data_frame_data, index_or_column):
        for array_type in self.__supported_types:
            ctype_array = getattr(data_frame_data.contents, '{}_{}_array'.format(array_type, index_or_column))
            if ctype_array:
                if array_type == 'string':
                    str_len = getattr(data_frame_data.contents, 'string_{}_max_length'.format(index_or_column))
                    numpy_array = np.ctypeslib.as_array(ctype_array, shape=(data_frame_data.contents.col_cnt, str_len))
                    numpy_array.dtype = 'U{}'.format(str_len)
                    numpy_array.shape = (numpy_array.shape[0], )
                    return numpy_array
                return np.ctypeslib.as_array(ctype_array, shape=(
                    getattr(data_frame_data.contents, 'row_cnt' if index_or_column == 'index' else 'col_cnt'), ))

    @classmethod
    def __get_offset_c_pointer(cls, c_pointer, offset):
        return cast(c_void_p(cast(c_pointer, c_void_p).value + offset * cls.__sys_addr_byte_cnt), type(c_pointer))

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
                value_array = np.ctypeslib.as_array(c_array_p, shape=(row_cnt, col_cnt, string_max_length))
                value_array.dtype = 'U{}'.format(string_max_length)
                value_array.shape = (value_array.shape[0], value_array.shape[1])
                values[value_key] = value_array
            elif array_type in self.__supported_number_types:
                value_array = np.ctypeslib.as_array(c_array_p, shape=(row_cnt, col_cnt))
                values[value_key] = value_array
            else:
                raise TypeError('Unknown array_type: {}'.format(array_type))
        return values

    def __get_table(self, c_table):
        table = OrderedDict()
        for idx in range(self.__table_info.column_cnt):
            dtype = self.bson_type_to_type(self.__table_info.column_types[idx])
            c_array = getattr(c_table.contents, '{}_columns'.format(dtype))[idx]
            if c_array:
                if dtype == 'string':
                    max_length = c_table.contents.string_column_max_lengths[idx]
                    array = np.ctypeslib.as_array(c_array, shape=(c_table.contents.row_cnt, max_length))
                    array.dtype = 'U{}'.format(max_length)
                    array.shape = (array.shape[0], )
                else:
                    array = np.ctypeslib.as_array(c_array, shape=(c_table.contents.row_cnt, ))
                table[self.__column_names[idx]] = array
        return table

    def find_as_data_frame(self, filter=None):
        # import time
        # begin = time.time()
        data_frame_data = self.mongoc_api.find(self.__mongoc_collection, pointer(self.__data_frame_info), self.__debug)
        # print(time.time() - begin)
        index = self.__get_index_or_column(data_frame_data, 'index')
        index = pd.Index(index, name=self.__index_key)
        column = self.__get_index_or_column(data_frame_data, 'column')
        column = pd.Index(column, name=self.__column_key)
        values = self.__get_values(data_frame_data)
        dfs = OrderedDict()
        for value_key, value in values.items():
            dfs[value_key] = pd.DataFrame(value, index=index, columns=column)
        # print(time.time() - begin)
        return dfs

    def find(self, filter=None):
        # import time
        # begin = time.time()
        c_table = self.mongoc_api.find(self.__mongoc_collection, pointer(self.__table_info), self.__debug)
        # print(time.time() - begin)
        table = self.__get_table(c_table)
        # print(time.time() - begin)
        df = pd.DataFrame(table)
        # print(time.time() - begin)
        return df


class CyMongoException(Exception):
    pass
