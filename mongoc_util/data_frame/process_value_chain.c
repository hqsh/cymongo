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
