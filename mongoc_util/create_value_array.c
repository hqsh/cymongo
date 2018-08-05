#include "cymongo_type.h"

// todo if use custom (not default) nan value, should transform the value in memory, then use memset
#define __CREATE_NUMBER_VALUE_ARRAY(TYPE, VALUE_ARRAYS, VALUE_NODE_T, VALUE_CHAIN_HEADS, NAN_VALUE) \
    mem_size = data_frame_size * sizeof(TYPE); \
    p_data_frame_data->VALUE_ARRAYS[value_idx] = (TYPE *) malloc (mem_size); \
    memset (p_data_frame_data->VALUE_ARRAYS[value_idx], NAN_VALUE, mem_size); \
    VALUE_NODE_T *p_node, *p_last_node; \
    for (p_node = p_mongo_data->VALUE_CHAIN_HEADS[value_idx]; p_node; ) { \
        p_data_frame_data->VALUE_ARRAYS[value_idx][*(p_node->p_index_idx) * col_cnt + *(p_node->p_column_idx)] = p_node->data; \
        p_last_node=p_node; \
        p_node=p_node->next; \
        free(p_last_node); \
    }


void create_value_array (data_frame_info_t *p_data_frame_info, mongo_data_t *p_mongo_data, data_frame_data_t *p_data_frame_data) {
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
            __CREATE_NUMBER_VALUE_ARRAY(int32_t, int32_value_arrays, int32_value_node_t, int32_value_chain_heads, 0)
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_INT64) {
            __CREATE_NUMBER_VALUE_ARRAY(int64_t, int64_value_arrays, int64_value_node_t, int64_value_chain_heads, 0)
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_DATE_TIME) {
            __CREATE_NUMBER_VALUE_ARRAY(int64_t, date_time_value_arrays, date_time_value_node_t, date_time_value_chain_heads, 0)
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_DOUBLE) {
            __CREATE_NUMBER_VALUE_ARRAY(float64_t, float64_value_arrays, float64_value_node_t, float64_value_chain_heads, -1)
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_BOOL) {
            __CREATE_NUMBER_VALUE_ARRAY(bool_t, bool_value_arrays, bool_value_node_t, bool_value_chain_heads, 0)
        }
    }
}
