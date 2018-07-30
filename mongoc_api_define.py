from ctypes import *

ILLEGAL_MONGOC_URI_ERROR_CODE = -1         # illegal mongo uri
CREATE_CLIENT_INSTANCE_ERROR_CODE = -2    # create client instance failed

# bson_type_t enum
BSON_TYPE_UNKNOWN = -1       # haven't get any document to set the BSON type
BSON_TYPE_EOD = 0x00
BSON_TYPE_DOUBLE = 0x01      # supported for DataFrame dtypes
BSON_TYPE_UTF8 = 0x02        # supported for DataFrame dtypes
BSON_TYPE_DOCUMENT = 0x03
BSON_TYPE_ARRAY = 0x04
BSON_TYPE_BINARY = 0x05
BSON_TYPE_UNDEFINED = 0x06
BSON_TYPE_OID = 0x07
BSON_TYPE_BOOL = 0x08        # supported for DataFrame dtypes
BSON_TYPE_DATE_TIME = 0x09   # supported for DataFrame dtypes
BSON_TYPE_NULL = 0x0A        # supported for DataFrame dtypes
BSON_TYPE_REGEX = 0x0B
BSON_TYPE_DBPOINTER = 0x0C
BSON_TYPE_CODE = 0x0D
BSON_TYPE_SYMBOL = 0x0E
BSON_TYPE_CODEWSCOPE = 0x0F
BSON_TYPE_INT32 = 0x10       # supported for DataFrame dtypes
BSON_TYPE_TIMESTAMP = 0x11
BSON_TYPE_INT64 = 0x12       # supported for DataFrame dtypes
BSON_TYPE_MAXKEY = 0x7F
BSON_TYPE_MINKEY = 0xFF


# data_frame_info_t
class DataFrameInfo(Structure):
    _fields_ = [
        ('index_key', c_char_p),
        ('index_type', c_int),
        ('column_key', c_char_p),
        ('column_type', c_int),
        ('value_keys', POINTER(c_char_p)),
        ('value_types', POINTER(c_int)),
        ('value_cnt', c_uint)
    ]


# data_frame_data_t
class DataFrameData(Structure):
    _fields_ = [
        ('row_cnt', c_uint64),
        ('col_cnt', c_uint64),
        ('string_index_array', POINTER(c_uint32)),
        ('int32_index_array', POINTER(c_int32)),
        ('int64_index_array', POINTER(c_int64)),
        ('date_time_index_array', POINTER(c_int64)),
        ('float64_index_array', POINTER(c_double)),
        ('bool_index_array', POINTER(c_bool)),
        ('string_column_array', POINTER(c_uint32)),
        ('int32_column_array', POINTER(c_int32)),
        ('int64_column_array', POINTER(c_int64)),
        ('date_time_column_array', POINTER(c_int64)),
        ('float64_column_array', POINTER(c_double)),
        ('bool_column_array', POINTER(c_bool)),
    ]
