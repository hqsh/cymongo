node_chain_heads_t * init_node_chain_heads_t (unsigned int column_cnt) {
    node_chain_heads_t *p_node_chain_heads = (node_chain_heads_t *) malloc (sizeof(node_chain_heads_t));
    p_node_chain_heads->string_node_chain_heads = (string_node_t **) malloc (sizeof(string_node_t *) * column_cnt);
    p_node_chain_heads->int32_node_chain_heads = (int32_node_t **) malloc (sizeof(int32_node_t *) * column_cnt);
    p_node_chain_heads->int64_node_chain_heads = (int64_node_t **) malloc (sizeof(int64_node_t *) * column_cnt);
    p_node_chain_heads->date_time_node_chain_heads = (date_time_node_t **) malloc (sizeof(date_time_node_t *) * column_cnt);
    p_node_chain_heads->float64_node_chain_heads = (float64_node_t **) malloc (sizeof(float64_node_t *) * column_cnt);
    p_node_chain_heads->bool_node_chain_heads = (bool_node_t **) malloc (sizeof(bool_node_t *) * column_cnt);
    memset (p_node_chain_heads->string_node_chain_heads, 0, sizeof(string_node_t *) * column_cnt);
    memset (p_node_chain_heads->int32_node_chain_heads, 0, sizeof(int32_node_t *) * column_cnt);
    memset (p_node_chain_heads->int64_node_chain_heads, 0, sizeof(int64_node_t *) * column_cnt);
    memset (p_node_chain_heads->date_time_node_chain_heads, 0, sizeof(date_time_node_t *) * column_cnt);
    memset (p_node_chain_heads->float64_node_chain_heads, 0, sizeof(float64_node_t *) * column_cnt);
    memset (p_node_chain_heads->bool_node_chain_heads, 0, sizeof(bool_node_t *) * column_cnt);
    return p_node_chain_heads;
}


#define _CREATE_NUMBER_NODE(NODE_T, P_NODE, IDX, DATA) \
    P_NODE = (NODE_T *) malloc (sizeof(NODE_T)); \
    P_NODE->idx = IDX; \
    P_NODE->data = DATA;

#define _INSERT_NODE(CHAIN_HEAD, P_NODE) \
    P_NODE->next = CHAIN_HEAD; \
    CHAIN_HEAD = P_NODE;

#define _PROCESS_TABLE(ITER, BSON_TYPE, COLUMN_IDX, IDX) \
    if (bson_type == BSON_TYPE_UTF8) { \
        string_data = bson_iter_utf8 (&ITER, &length); \
        p_string_node = (string_node_t *) malloc (sizeof(string_node_t)); \
        _CHAR_STRING_TO_UNI_CHAR_STRING(string_data, p_string_node->data.string, p_string_node->data.length) \
        p_string_node->idx = IDX; \
        if (p_table->string_column_max_lengths[COLUMN_IDX] < p_string_node->data.length) { \
            p_table->string_column_max_lengths[COLUMN_IDX] = p_string_node->data.length; \
        } \
        _INSERT_NODE (p_node_chain_heads->string_node_chain_heads[COLUMN_IDX], p_string_node) \
    } \
    else if (bson_type == BSON_TYPE_INT32) { \
        int32_data = bson_iter_int32 (&ITER); \
        _CREATE_NUMBER_NODE (int32_node_t, p_int32_node, IDX, int32_data) \
        _INSERT_NODE (p_node_chain_heads->int32_node_chain_heads[COLUMN_IDX], p_int32_node) \
    } \
    else if (bson_type == BSON_TYPE_INT64 && !is_int64) { \
        int32_data = bson_iter_int32 (&ITER); \
        _CREATE_NUMBER_NODE (int64_node_t, p_int64_node, IDX, int32_data) \
        _INSERT_NODE (p_node_chain_heads->int64_node_chain_heads[COLUMN_IDX], p_int64_node) \
    } \
    else if (bson_type == BSON_TYPE_INT64 && is_int64) { \
        int64_data = bson_iter_int64 (&ITER); \
        _CREATE_NUMBER_NODE (int64_node_t, p_int64_node, IDX, int64_data) \
        _INSERT_NODE (p_node_chain_heads->int64_node_chain_heads[COLUMN_IDX], p_int64_node) \
    } \
    else if (bson_type == BSON_TYPE_DATE_TIME) { \
        date_time_data = bson_iter_date_time (&ITER); \
        _TRANSFER_DATE_TIME_FORMAT(date_time_data, formatted_date_time) \
        _CREATE_NUMBER_NODE (date_time_node_t, p_date_time_node, IDX, formatted_date_time) \
        _INSERT_NODE (p_node_chain_heads->date_time_node_chain_heads[COLUMN_IDX], p_date_time_node) \
    } \
    else if (bson_type == BSON_TYPE_DOUBLE) { \
        float64_data = bson_iter_double (&ITER); \
        _CREATE_NUMBER_NODE (float64_node_t, p_float64_node, IDX, float64_data) \
        _INSERT_NODE (p_node_chain_heads->float64_node_chain_heads[COLUMN_IDX], p_float64_node) \
    } \
    else if (bson_type == BSON_TYPE_BOOL) { \
        bool_data = bson_iter_bool (&ITER); \
        _CREATE_NUMBER_NODE (bool_node_t, p_bool_node, IDX, bool_data) \
        _INSERT_NODE (p_node_chain_heads->bool_node_chain_heads[COLUMN_IDX], p_bool_node) \
    }
