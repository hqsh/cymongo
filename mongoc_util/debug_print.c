//#include "cymongo_type.h"
//
//void print_values_in_chains (data_frame_info_t *p_data_frame_info, mongo_data_t *p_mongo_data) {
//    for (unsigned int value_idx = 0; value_idx < p_data_frame_info->value_cnt; value_idx++) {
//        printf ("====================================================\n");
//        printf ("value: %s, value_type: %d\n", p_data_frame_info->value_keys[value_idx], p_data_frame_info->value_types[value_idx]);
//        printf ("====================================================\n");
//        printf ("index_idx    column_idx     value\n");
//        if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_UTF8) {
//            for (string_value_node_t *p_node = p_mongo_data->string_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
//                printf ("%7llu      %7llu      ", *(p_node->p_index_idx), *(p_node->p_column_idx));
//                for (uint64_t str_i = 0; str_i < p_node->data.length; str_i++) {
//                    printf ("%u ", p_node->data.string[str_i]);
//                }
//                printf ("\n");
//            }
//        }
//        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_INT32) {
//            for (int32_value_node_t *p_node = p_mongo_data->int32_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
//                printf ("%7llu      %7llu      %d\n", *(p_node->p_index_idx), *(p_node->p_column_idx), p_node->data);
//            }
//        }
//        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_INT64) {
//            for (int64_value_node_t *p_node = p_mongo_data->int64_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
//                printf ("%7llu      %7llu      %lld\n", *(p_node->p_index_idx), *(p_node->p_column_idx), p_node->data);
//            }
//        }
//        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_DOUBLE) {
//            for (float64_value_node_t *p_node = p_mongo_data->float64_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
//                printf ("%7llu      %7llu      %f\n", *(p_node->p_index_idx), *(p_node->p_column_idx), p_node->data);
//            }
//        }
//        else if (p_data_frame_info->value_types[value_idx] == BSON_TYPE_BOOL) {
//            for (bool_value_node_t *p_node = p_mongo_data->bool_value_chain_heads[value_idx]; p_node; p_node=p_node->next) {
//                printf ("%7llu      %7llu      %d\n", *(p_node->p_index_idx), *(p_node->p_column_idx), p_node->data);
//            }
//        }
//    }
//}
