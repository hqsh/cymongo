#define HASH_DEL_ALL_STRING(HEAD, TYPE) \
    if (HEAD != NULL) { \
        TYPE *p, *p_tmp; \
        HASH_ITER (hh, HEAD, p, p_tmp) { \
            free ((void *) p->key); \
            free (p->data.string); \
            HASH_DEL (HEAD, p); \
        } \
    }

#define HASH_DEL_ALL_NUMBER(HEAD, TYPE) \
    if (HEAD != NULL) { \
        TYPE *p, *p_tmp; \
        HASH_ITER (hh, HEAD, p, p_tmp) { \
            HASH_DEL (HEAD, p); \
        } \
    }

void free_data_frame_memory (mongo_data_t *p_mongo_data) {
    HASH_DEL_ALL_STRING (p_mongo_data->string_index_head, string_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->int32_index_head, int32_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->int64_index_head, int64_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->date_time_index_head, date_time_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->float64_index_head, float64_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->bool_index_head, bool_index_t);
    HASH_DEL_ALL_STRING (p_mongo_data->string_column_head, string_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->int32_column_head, int32_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->int64_column_head, int64_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->date_time_column_head, date_time_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->float64_column_head, float64_index_t);
    HASH_DEL_ALL_NUMBER (p_mongo_data->bool_column_head, bool_index_t);
    free (p_mongo_data->string_value_chain_heads);
    free (p_mongo_data->int32_value_chain_heads);
    free (p_mongo_data->int64_value_chain_heads);
    free (p_mongo_data->date_time_value_chain_heads);
    free (p_mongo_data->float64_value_chain_heads);
    free (p_mongo_data->bool_value_chain_heads);
    free (p_mongo_data);
}
