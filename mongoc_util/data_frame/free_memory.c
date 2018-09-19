#define HASH_DEL_ALL(HEAD, TYPE) \
    if (HEAD != NULL) { \
        TYPE *p, *p_tmp; \
        HASH_ITER (hh, HEAD, p, p_tmp) { \
            HASH_DEL (HEAD, p); \
        } \
    }

void free_data_frame_memory (mongo_data_t *p_mongo_data) {
    HASH_DEL_ALL (p_mongo_data->string_index_head, string_index_t);
    HASH_DEL_ALL (p_mongo_data->int32_index_head, int32_index_t);
    HASH_DEL_ALL (p_mongo_data->int64_index_head, int64_index_t);
    HASH_DEL_ALL (p_mongo_data->date_time_index_head, date_time_index_t);
    HASH_DEL_ALL (p_mongo_data->float64_index_head, float64_index_t);
    HASH_DEL_ALL (p_mongo_data->bool_index_head, bool_index_t);
    free (p_mongo_data);
}
