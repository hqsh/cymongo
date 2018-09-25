table_t * init_table (unsigned int column_cnt) {
    table_t *p_table = (table_t *) malloc (sizeof(table_t));
    p_table->string_column_max_lengths = (uint64_t *) malloc (sizeof(uint64_t) * column_cnt);
    p_table->string_columns = (bson_unichar_t **) malloc (sizeof(bson_unichar_t *) * column_cnt);
    p_table->int32_columns = (int32_t **) malloc (sizeof(int32_t *) * column_cnt);
    p_table->int64_columns = (int64_t **) malloc (sizeof(int64_t *) * column_cnt);
    p_table->date_time_columns = (int64_t **) malloc (sizeof(int64_t *) * column_cnt);
    p_table->float64_columns = (float64_t **) malloc (sizeof(float64_t *) * column_cnt);
    p_table->bool_columns = (bool_t **) malloc (sizeof(bool_t *) * column_cnt);
    memset (p_table->string_column_max_lengths, 0, sizeof(uint64_t) * column_cnt);
    memset (p_table->string_columns, 0, sizeof(bson_unichar_t *) * column_cnt);
    memset (p_table->int32_columns, 0, sizeof(int32_t *) * column_cnt);
    memset (p_table->int64_columns, 0, sizeof(int64_t *) * column_cnt);
    memset (p_table->date_time_columns, 0, sizeof(int64_t *) * column_cnt);
    memset (p_table->bool_columns, 0, sizeof(float64_t *) * column_cnt);
    return p_table;
}

#define __CREATE_TABLE_NUMBER_COLUMN(COLUMNS, TYPE, NODE_T, NODE_CHAIN_HEADS, NAN_VALUE, NAN_VALUE_IS_BYTE) \
    p_table->COLUMNS[idx] = (TYPE *) malloc (sizeof(TYPE) * p_table->row_cnt); \
    if (NAN_VALUE_IS_BYTE) { \
        memset (p_table->COLUMNS[idx], NAN_VALUE, sizeof(TYPE) * p_table->row_cnt); \
    } \
    else { \
        for (uint64_t i = 0; i < p_table->row_cnt; i++) { \
            memcpy (p_table->COLUMNS[idx] + i, &NAN_VALUE, sizeof(TYPE)); \
        } \
    } \
    NODE_T *p_last_node; \
    for (NODE_T *p_node = p_node_chain_heads->NODE_CHAIN_HEADS[idx]; p_node; ) { \
        p_table->COLUMNS[idx][p_node->idx] = p_node->data; \
        p_last_node = p_node; \
        p_node = p_node->next; \
        free (p_last_node); \
    }

void create_table_array (table_info_t *p_table_info, node_chain_heads_t *p_node_chain_heads, table_t *p_table, default_nan_value_t *p_default_nan_value) {
    for (unsigned int idx = 0; idx < p_table_info->column_cnt; idx++) {
        int column_type = p_table_info->column_types[idx];
        if (column_type == BSON_TYPE_UTF8) {
            uint64_t max_length = p_table->string_column_max_lengths[idx];
            uint64_t mem_size = sizeof(bson_unichar_t) * p_table->row_cnt * max_length;
            p_table->string_columns[idx] = (bson_unichar_t *) malloc (mem_size);
            memset (p_table->string_columns[idx], 0, mem_size);
            string_node_t *p_last_node;
            for (string_node_t *p_node = p_node_chain_heads->string_node_chain_heads[idx]; p_node; ) {
                memcpy (p_table->string_columns[idx] + p_node->idx * max_length,
                    p_node->data.string, p_node->data.length * sizeof(bson_unichar_t));
                p_last_node = p_node;
                p_node = p_node->next;
                free (p_last_node->data.string);
                free (p_last_node);
            }
        }
        else if (column_type == BSON_TYPE_INT32) {
            __CREATE_TABLE_NUMBER_COLUMN (int32_columns, int32_t, int32_node_t, int32_node_chain_heads, p_default_nan_value->default_int32_nan_value, false)
        }
        else if (column_type == BSON_TYPE_INT64) {
            __CREATE_TABLE_NUMBER_COLUMN (int64_columns, int64_t, int64_node_t, int64_node_chain_heads, p_default_nan_value->default_int64_nan_value, false)
        }
        else if (column_type == BSON_TYPE_DATE_TIME) {
            __CREATE_TABLE_NUMBER_COLUMN (date_time_columns, date_time_t, date_time_node_t, date_time_node_chain_heads, p_default_nan_value->default_date_time_nan_value, false)
        }
        else if (column_type == BSON_TYPE_DOUBLE) {
            uint8_t float64_nan_value = 255;  // 4 "255"s in memory is float64 nan
            __CREATE_TABLE_NUMBER_COLUMN (float64_columns, float64_t, float64_node_t, float64_node_chain_heads, float64_nan_value, true)
        }
        else if (column_type == BSON_TYPE_BOOL) {
            __CREATE_TABLE_NUMBER_COLUMN (bool_columns, bool_t, bool_node_t, bool_node_chain_heads, p_default_nan_value->default_bool_nan_value, false)
        }
    }
    free (p_node_chain_heads->string_node_chain_heads);
    free (p_node_chain_heads->int32_node_chain_heads);
    free (p_node_chain_heads->int64_node_chain_heads);
    free (p_node_chain_heads->date_time_node_chain_heads);
    free (p_node_chain_heads->float64_node_chain_heads);
    free (p_node_chain_heads->bool_node_chain_heads);
}
