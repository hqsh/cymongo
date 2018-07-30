from mongoc_api_define import *
import numpy as np


class CyMongoException(Exception):
    pass


class CyMongo:
    mongoc_api = cdll.LoadLibrary("./mongoc_api.so")
    mongoc_api.get_client.restype = POINTER(c_void_p)
    mongoc_api.get_database.restype = POINTER(c_void_p)
    mongoc_api.get_collection.restype = POINTER(c_void_p)
    mongoc_api.find.restype = POINTER(DataFrameData)

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
    supported_types = ('string', 'date_time', 'int32', 'int64', 'float64', 'bool')

    def __init__(self, mongoc_client, db_name, collection_name):
        self.__mongoc_client = mongoc_client
        self.__db_name = self.to_bytes(db_name, 'db_name')
        self.__collection_name = self.to_bytes(collection_name, 'collection_name')
        self.__mongoc_collection = self.mongoc_api.get_collection(self.__mongoc_client, self.__db_name,
                                                                  self.__collection_name)
        self.__data_frame_info = None

    def set_data_frame_info(self, index_key, column_key, value_keys):
        value_keys = np.array(list(value_keys))
        index_key = self.to_bytes(index_key, 'index_key', accept_none=True)
        column_key = self.to_bytes(column_key, 'column_key', accept_none=True)
        byte_value_keys = []
        value_types = []
        for value_key in value_keys:
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

    def find(self, filter=None, debug=False):
        data_frame_data = self.mongoc_api.find(self.__mongoc_collection, pointer(self.__data_frame_info), debug)
        if debug:
            print((data_frame_data.contents.row_cnt, data_frame_data.contents.col_cnt))
        ctype_index_array = None
        for array_type in self.supported_types:
            ctype_index_array = getattr(data_frame_data.contents, '{}_index_array'.format(array_type))
            if ctype_index_array:
                if debug:
                    print('index type is: {}'.format(array_type))
                break
        ctype_column_array = None
        for array_type in self.supported_types:
            ctype_column_array = getattr(data_frame_data.contents, '{}_column_array'.format(array_type))
            if ctype_column_array:
                if debug:
                    print('column type is: {}'.format(array_type))
                break
        index = np.ctypeslib.as_array(ctype_index_array, shape=(data_frame_data.contents.row_cnt,))
        # column = np.ctypeslib.as_array(ctype_column_array, shape=(data_frame_data.contents.col_cnt,))
        print(index)
        # print(column)
