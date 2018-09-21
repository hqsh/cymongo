#include "../basic_operation.c"


#define _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(ARRAY, HEAD, CNT, TYPE, INDEX_NODE_T, P_INDEX, P_TMP_INDEX, INDEX_SORT_FUNC) \
    if (HEAD != NULL) { \
        CNT = HASH_COUNT (HEAD); \
        HASH_SORT (HEAD, INDEX_SORT_FUNC); \
        ARRAY = (TYPE *) malloc (sizeof(TYPE) * CNT); \
        uint64_t i = 0; \
        HASH_ITER (hh, HEAD, P_INDEX, P_TMP_INDEX) { \
            P_INDEX->idx = i; \
            ARRAY[i] = P_INDEX->data; \
            i++; \
        } \
    }


#define _CREATE_INDEX_OR_COLUMN_ARRAY(IS_INDEX, BSON_TYPE, P_MONGO_DATA, MAX_STRING_LENGTH, CNT, STRING_INDEX_OR_COLUMN_MAX_LENGTH, STRING_ARRAY, INT32_ARRAY, INT64_ARRAY, DATE_TIME_ARRAY, FLOAT64_ARRAY, BOOL_ARRAY) \
    if (BSON_TYPE == BSON_TYPE_UTF8) { \
        string_index_t *head; \
        if (IS_INDEX) { \
            head = P_MONGO_DATA->string_index_head; \
        } \
        else { \
            head = P_MONGO_DATA->string_column_head; \
        } \
        CNT = HASH_COUNT (head); \
        HASH_SORT (head, string_index_sort); \
        STRING_ARRAY = (bson_unichar_t *) malloc (sizeof(bson_unichar_t) * MAX_STRING_LENGTH * CNT); \
        memset (STRING_ARRAY, 0, sizeof(bson_unichar_t) * MAX_STRING_LENGTH * CNT); \
        uint64_t i = 0; \
        HASH_ITER (hh, head, p_string_index, p_tmp_string_index) { \
            p_string_index->idx = i; \
            memcpy (&(STRING_ARRAY[i * MAX_STRING_LENGTH]), p_string_index->data.string, sizeof(bson_unichar_t) * p_string_index->data.length); \
            i++; \
        } \
        STRING_INDEX_OR_COLUMN_MAX_LENGTH = MAX_STRING_LENGTH; \
    } \
    else if (BSON_TYPE == BSON_TYPE_INT32) { \
        if (IS_INDEX) { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (INT32_ARRAY, P_MONGO_DATA->int32_index_head, CNT, int32_t, int32_index_t, p_int32_index, p_tmp_int32_index, int32_index_sort) \
        } \
        else { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (INT32_ARRAY, P_MONGO_DATA->int32_column_head, CNT, int32_t, int32_index_t, p_int32_index, p_tmp_int32_index, int32_index_sort) \
        } \
    } \
    else if (BSON_TYPE == BSON_TYPE_INT64) { \
        if (IS_INDEX) { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (INT64_ARRAY, P_MONGO_DATA->int64_index_head, CNT, int64_t, int64_index_t, p_int64_index, p_tmp_int64_index, int64_index_sort) \
        } \
        else { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (INT64_ARRAY, P_MONGO_DATA->int64_column_head, CNT, int64_t, int64_index_t, p_int64_index, p_tmp_int64_index, int64_index_sort) \
        } \
    } \
    else if (BSON_TYPE == BSON_TYPE_DATE_TIME) { \
        if (IS_INDEX) { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (DATE_TIME_ARRAY, P_MONGO_DATA->date_time_index_head, CNT, date_time_t, date_time_index_t, p_date_time_index, p_tmp_date_time_index, date_time_index_sort) \
        } \
        else { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (DATE_TIME_ARRAY, P_MONGO_DATA->date_time_column_head, CNT, date_time_t, date_time_index_t, p_date_time_index, p_tmp_date_time_index, date_time_index_sort) \
        } \
    } \
    else if (BSON_TYPE == BSON_TYPE_DOUBLE) { \
        if (IS_INDEX) { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (FLOAT64_ARRAY, P_MONGO_DATA->float64_index_head, CNT, float64_t, float64_index_t, p_float64_index, p_tmp_float64_index, float64_index_sort) \
        } \
        else { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (FLOAT64_ARRAY, P_MONGO_DATA->float64_column_head, CNT, float64_t, float64_index_t, p_float64_index, p_tmp_float64_index, float64_index_sort) \
        } \
    } \
    else if (BSON_TYPE == BSON_TYPE_BOOL) { \
        if (IS_INDEX) { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (BOOL_ARRAY, P_MONGO_DATA->bool_index_head, CNT, bool_t, bool_index_t, p_bool_index, p_tmp_bool_index, bool_index_sort) \
        } \
        else { \
            _CREATE_INDEX_OR_COLUMN_NUM_ARRAY (BOOL_ARRAY, P_MONGO_DATA->bool_column_head, CNT, bool_t, bool_index_t, p_bool_index, p_tmp_bool_index, bool_index_sort) \
        } \
    }


#define __CREATE_NUMBER_VALUE_ARRAY(TYPE, VALUE_ARRAYS, VALUE_NODE_T, VALUE_CHAIN_HEADS, NAN_VALUE, NAN_VALUE_IS_BYTE) \
    mem_size = data_frame_size * sizeof(TYPE); \
    p_data_frame_data->VALUE_ARRAYS[value_idx] = (TYPE *) malloc (mem_size); \
    if (NAN_VALUE_IS_BYTE) { \
        memset (p_data_frame_data->VALUE_ARRAYS[value_idx], NAN_VALUE, mem_size); \
    } \
    else { \
        for (uint64_t i = 0; i < data_frame_size; i++) { \
            memcpy (p_data_frame_data->VALUE_ARRAYS[value_idx] + i, &NAN_VALUE, sizeof(TYPE)); \
        } \
    } \
    VALUE_NODE_T *p_node, *p_last_node; \
    for (p_node = p_mongo_data->VALUE_CHAIN_HEADS[value_idx]; p_node; ) { \
        p_data_frame_data->VALUE_ARRAYS[value_idx][*(p_node->p_index_idx) * col_cnt + *(p_node->p_column_idx)] = p_node->data; \
        p_last_node=p_node; \
        p_node=p_node->next; \
        free(p_last_node); \
    }


void create_value_array (data_frame_info_t *p_data_frame_info, mongo_data_t *p_mongo_data, data_frame_data_t *p_data_frame_data, default_nan_value_t *p_default_nan_value) {
    uint64_t row_cnt = p_data_frame_data->row_cnt;
    uint64_t col_cnt = p_data_frame_data->col_cnt;
    uint64_t data_frame_size = row_cnt * col_cnt;
    uint64_t mem_size, row_item_cnt, uni_char_string_length;
    for (unsigned int value_idx = 0; value_idx < p_data_frame_info->value_cnt; value_idx++) {
        if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_UTF8) {
            uni_char_string_length = p_data_frame_data->string_value_max_lengths[value_idx];
            row_item_cnt = uni_char_string_length * col_cnt;
            mem_size = row_item_cnt * row_cnt * sizeof(bson_unichar_t);
            p_data_frame_data->string_value_arrays[value_idx] = (bson_unichar_t *) malloc (mem_size);
            memset (p_data_frame_data->string_value_arrays[value_idx], 0, mem_size);
            string_value_node_t *p_node, *p_last_node;
            for (p_node = p_mongo_data->string_value_chain_heads[value_idx], p_last_node=NULL; p_node; ) {
                memcpy (p_data_frame_data->string_value_arrays[value_idx] + *(p_node->p_index_idx) * row_item_cnt +
                    *(p_node->p_column_idx) * uni_char_string_length, p_node->data.string, p_node->data.length * sizeof(bson_unichar_t));
                p_last_node=p_node;
                p_node=p_node->next;
                free(p_last_node);
            }
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_INT32) {
            __CREATE_NUMBER_VALUE_ARRAY(int32_t, int32_value_arrays, int32_value_node_t, int32_value_chain_heads, p_default_nan_value->default_int32_nan_value, false)
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_INT64) {
            __CREATE_NUMBER_VALUE_ARRAY(int64_t, int64_value_arrays, int64_value_node_t, int64_value_chain_heads, p_default_nan_value->default_int64_nan_value, false)
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_DATE_TIME) {
            __CREATE_NUMBER_VALUE_ARRAY(date_time_t, date_time_value_arrays, date_time_value_node_t, date_time_value_chain_heads, p_default_nan_value->default_date_time_nan_value, false)
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_DOUBLE) {
            uint8_t float64_nan_value = -1;  // 4 "-1"s in memory is float64 nan
            __CREATE_NUMBER_VALUE_ARRAY(float64_t, float64_value_arrays, float64_value_node_t, float64_value_chain_heads, float64_nan_value, true)
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_BOOL) {
            __CREATE_NUMBER_VALUE_ARRAY(bool_t, bool_value_arrays, bool_value_node_t, bool_value_chain_heads, p_default_nan_value->default_bool_nan_value, false)
        }
    }
}
