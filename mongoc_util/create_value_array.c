#include "cymongo_type.h"

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
            p_data_frame_data->string_value_arrays[value_idx] = (bson_unichar_t ***) malloc (mem_size);
            memset (p_data_frame_data->string_value_arrays[value_idx], 0, mem_size);
            for (string_value_node_t *p_node = p_mongo_data->string_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
                memcpy (p_data_frame_data->string_value_arrays[value_idx] + *(p_node->p_index_idx) * row_item_cnt +
                    *(p_node->p_column_idx) * uni_char_string_length, p_node->data.string, p_node->data.length * sizeof(bson_unichar_t));
            }
//            printf ("==================================================================================\n");
//            for (uint64_t str_i = 0; str_i < mem_size / 4; str_i++) {
//                printf ("%lu ", p_data_frame_data->string_value_arrays[value_idx][str_i]);
//            }
//            printf ("\n");
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_INT32) {
            for (int32_value_node_t *p_node = p_mongo_data->int32_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
//                printf ("%7llu      %7llu      %d\n", *(it->p_index_idx), *(it->p_column_idx), it->data);
            }
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_INT64) {
            for (int64_value_node_t *p_node = p_mongo_data->int64_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
//                printf ("%7llu      %7llu      %lld\n", *(it->p_index_idx), *(it->p_column_idx), it->data);
            }
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_DOUBLE) {
            for (float64_value_node_t *p_node = p_mongo_data->float64_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
//                printf ("%7llu      %7llu      %f\n", *(it->p_index_idx), *(it->p_column_idx), it->data);
            }
        }
        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_BOOL) {
            for (bool_value_node_t *p_node = p_mongo_data->bool_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
//                printf ("%7llu      %7llu      %d\n", *(it->p_index_idx), *(it->p_column_idx), it->data);
            }
        }
    }
}
