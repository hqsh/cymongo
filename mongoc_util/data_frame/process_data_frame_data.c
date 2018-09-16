#include <stdio.h>


#define __PROCESS_NUMBER_INDEX_OR_COLUMN(TYPE, INDEX_TYPE, HEAD, P_INDEX, DATA, P_IDX) \
    HASH_FIND (hh, p_mongo_data->HEAD, &DATA, sizeof(TYPE), P_INDEX); \
    if (!P_INDEX) { \
        P_INDEX = (INDEX_TYPE *) malloc (sizeof(INDEX_TYPE)); \
        memset (P_INDEX, 0, sizeof(INDEX_TYPE)); \
        P_INDEX->data = DATA; \
        HASH_ADD (hh, p_mongo_data->HEAD, data, sizeof(TYPE), P_INDEX); \
    } \
    P_IDX = &(P_INDEX->idx);


#define _PROCESS_INDEX_OR_COLUMN(IS_INDEX, TYPE, ITER, P_MONGO_DATA, MAX_STRING_LENGTH, P_IDX) \
    if (data_frame_info->TYPE == BSON_TYPE_UNKNOWN) { \
        data_frame_info->TYPE = bson_iter_type(&ITER); \
    } \
    if (data_frame_info->TYPE == BSON_TYPE_UTF8) { \
        string_data = bson_iter_utf8 (&ITER, &length); \
        if (MAX_STRING_LENGTH < length) { \
            MAX_STRING_LENGTH = length; \
        } \
        if (IS_INDEX) { \
            HASH_FIND_STR (P_MONGO_DATA->string_index_head, string_data, p_string_index); \
        } \
        else { \
            HASH_FIND_STR (P_MONGO_DATA->string_column_head, string_data, p_string_index); \
        } \
        if (!p_string_index) { \
            p_string_index = (string_index_t *) malloc (sizeof(string_index_t)); \
            memset (p_string_index, 0, sizeof(string_index_t)); \
            p_string_index->key = string_data; \
            _CHAR_STRING_TO_UNI_CHAR_STRING(string_data, p_string_index->data.string, p_string_index->data.length) \
            if (IS_INDEX) { \
                HASH_ADD_STR (P_MONGO_DATA->string_index_head, key, p_string_index); \
            } \
            else { \
                HASH_ADD_STR (P_MONGO_DATA->string_column_head, key, p_string_index); \
            } \
        } \
        P_IDX = &(p_string_index->idx); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_INT32) { \
        int32_data = bson_iter_int32 (&ITER); \
        if (IS_INDEX) { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (int32_t, int32_index_t, int32_index_head, p_int32_index, int32_data, P_IDX) \
        } \
        else { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (int32_t, int32_index_t, int32_column_head, p_int32_index, int32_data, P_IDX) \
        } \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_INT64) { \
        int64_data = bson_iter_int64 (&ITER); \
        if (IS_INDEX) { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (int64_t, int64_index_t, int64_index_head, p_int64_index, int64_data, P_IDX) \
        } \
        else { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (int64_t, int64_index_t, int64_column_head, p_int64_index, int64_data, P_IDX) \
        } \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_DATE_TIME) { \
        date_time_data = bson_iter_date_time (&ITER); \
        if (IS_INDEX) { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (date_time_t, date_time_index_t, date_time_index_head, p_date_time_index, date_time_data, P_IDX) \
        } \
        else { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (date_time_t, date_time_index_t, date_time_column_head, p_date_time_index, date_time_data, P_IDX) \
        } \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_DOUBLE) { \
        float64_data = bson_iter_double (&ITER); \
        if (IS_INDEX) { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (float64_t, float64_index_t, float64_index_head, p_float64_index, float64_data, P_IDX) \
        } \
        else { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (float64_t, float64_index_t, float64_column_head, p_float64_index, float64_data, P_IDX) \
        } \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_BOOL) { \
        bool_data = bson_iter_bool (&ITER); \
        if (IS_INDEX) { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (bool_t, bool_index_t, bool_index_head, p_bool_index, bool_data, P_IDX) \
        } \
        else { \
            __PROCESS_NUMBER_INDEX_OR_COLUMN (bool_t, bool_index_t, bool_column_head, p_bool_index, bool_data, P_IDX) \
        } \
    } \
    else { \
        continue; \
    }


#define _INSERT_VALUE_NODE(VALUE_NODE_T, VALUE_CHAIN_HEAD, CREATE_DATA, DATA, P_INDEX_IDX, P_COLUMN_IDX) \
    VALUE_NODE_T *__p_node = (VALUE_NODE_T *) malloc (sizeof (VALUE_NODE_T) ); \
    CREATE_DATA (__p_node, DATA) \
    __p_node->p_index_idx = P_INDEX_IDX; \
    __p_node->p_column_idx = P_COLUMN_IDX; \
    __p_node->next = VALUE_CHAIN_HEAD; \
    VALUE_CHAIN_HEAD = __p_node;


#define _PROCESS_VALUE(P_DATA_FRAME_INFO, ITER, P_MONGO_DATA, P_INDEX_IDX, P_COLUMN_IDX, STRING_VALUE_MAX_LENGTHS) \
    for (unsigned int value_idx = 0; value_idx < P_DATA_FRAME_INFO->value_cnt; value_idx++) { \
        if (bson_iter_init (&ITER, doc) && bson_iter_find (&ITER, P_DATA_FRAME_INFO->value_keys[value_idx])) { \
            if (P_DATA_FRAME_INFO->value_types[value_idx] == BSON_TYPE_UNKNOWN) { \
                P_DATA_FRAME_INFO->value_types[value_idx] = bson_iter_type(&ITER); \
            } \
            if (P_DATA_FRAME_INFO->value_types[value_idx] == BSON_TYPE_UTF8) { \
                string_data = bson_iter_utf8 (&ITER, &length); \
                _INSERT_VALUE_NODE (string_value_node_t, P_MONGO_DATA->string_value_chain_heads[value_idx], _CREATE_UNI_CHAR_STRING_DATA, string_data, P_INDEX_IDX, P_COLUMN_IDX) \
                if (STRING_VALUE_MAX_LENGTHS[value_idx] < __p_node->data.length) { \
                    STRING_VALUE_MAX_LENGTHS[value_idx] = __p_node->data.length; \
                } \
            } \
            else if (P_DATA_FRAME_INFO->value_types[value_idx] == BSON_TYPE_INT32) { \
                int32_data = bson_iter_int32 (&ITER); \
                _INSERT_VALUE_NODE (int32_value_node_t, P_MONGO_DATA->int32_value_chain_heads[value_idx], _CREATE_NUMBER_DATA, int32_data, P_INDEX_IDX, P_COLUMN_IDX) \
            } \
            else if (P_DATA_FRAME_INFO->value_types[value_idx] == BSON_TYPE_INT64) { \
                int64_data = bson_iter_int64 (&ITER); \
                _INSERT_VALUE_NODE (int64_value_node_t, P_MONGO_DATA->int64_value_chain_heads[value_idx], _CREATE_NUMBER_DATA, int64_data, P_INDEX_IDX, P_COLUMN_IDX) \
            } \
            else if (P_DATA_FRAME_INFO->value_types[value_idx] == BSON_TYPE_DATE_TIME) { \
                int64_data = bson_iter_date_time (&ITER); \
                _INSERT_VALUE_NODE (date_time_value_node_t, P_MONGO_DATA->date_time_value_chain_heads[value_idx], _CREATE_NUMBER_DATA, int64_data, P_INDEX_IDX, P_COLUMN_IDX) \
            } \
            else if (P_DATA_FRAME_INFO->value_types[value_idx] == BSON_TYPE_DOUBLE) { \
                float64_data = bson_iter_double (&ITER); \
                _INSERT_VALUE_NODE (float64_value_node_t, P_MONGO_DATA->float64_value_chain_heads[value_idx], _CREATE_NUMBER_DATA, float64_data, P_INDEX_IDX, P_COLUMN_IDX) \
            } \
            else if (P_DATA_FRAME_INFO->value_types[value_idx] == BSON_TYPE_BOOL) { \
                bool_data = bson_iter_bool (&ITER); \
                _INSERT_VALUE_NODE (bool_value_node_t, P_MONGO_DATA->bool_value_chain_heads[value_idx], _CREATE_NUMBER_DATA, bool_data, P_INDEX_IDX, P_COLUMN_IDX) \
            } \
        } \
    }
