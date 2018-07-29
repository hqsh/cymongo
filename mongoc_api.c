#include "cymongo_utils.c"

mongoc_client_t * get_client (const char *mongoc_uri, int *error_code)
{
    mongoc_uri_t *uri;
    mongoc_client_t *client;
    bson_error_t error;
    mongoc_init ();
    uri = mongoc_uri_new_with_error (mongoc_uri, &error);
    if (!uri) {
        *error_code = ILLEGAL_MONGOC_URI_ERROR_CODE;
        return NULL;
    }
    client = mongoc_client_new_from_uri (uri);
    if (!client) {
        *error_code = CREATE_CLIENT_INSTANCE_ERROR_CODE;
        return NULL;
    }
    *error_code = EXIT_SUCCESS;
    return client;
}

mongoc_database_t * get_database (mongoc_client_t *client, const char *db_name)
{
    return mongoc_client_get_database (client, db_name);
}


mongoc_collection_t * get_collection (mongoc_client_t *client, const char *db_name, const char *collection_name)
{
    return mongoc_client_get_collection (client, db_name, collection_name);
}

#define _SET_INDEX_CHAIN_HEAD_TYPE(INDEX_NODE_T, CHAIN_HEAD) \
    if (is_first_doc) { \
        CHAIN_HEAD = (INDEX_NODE_T *) CHAIN_HEAD; \
    }

#define _PROCESS_INDEX_OR_COLUMN(TYPE, ITER, KEY, CHAIN_HEAD) \
    if (data_frame_info->TYPE == BSON_TYPE_UNKNOWN) { \
        data_frame_info->TYPE = bson_iter_type(&ITER); \
    } \
    if (data_frame_info->TYPE == BSON_TYPE_UTF8) { \
        string_data = bson_iter_utf8 (&ITER, &length); \
        if (debug) \
            printf ("%s : %s [BSON_TYPE_UTF8 %d] length = %d\n", data_frame_info->KEY, string_data, BSON_TYPE_UTF8, length); \
        _SET_INDEX_CHAIN_HEAD_TYPE (string_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_UTF8, string_data, &CHAIN_HEAD); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_INT32) { \
        int32_data = bson_iter_int32 (&ITER); \
        if (debug) \
            printf ("%s : %d [BSON_TYPE_INT32 %d]\n", data_frame_info->KEY, int32_data, BSON_TYPE_INT32); \
        _SET_INDEX_CHAIN_HEAD_TYPE (int32_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_INT32, &int32_data, &CHAIN_HEAD); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_INT64) { \
        int64_data = bson_iter_int64 (&ITER); \
        if (debug) \
            printf ("%s : %ld [BSON_TYPE_INT64 %d]\n", data_frame_info->KEY, int64_data, BSON_TYPE_INT64); \
        _SET_INDEX_CHAIN_HEAD_TYPE (int64_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_INT64, &int64_data, &CHAIN_HEAD); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_DOUBLE) { \
        float64_data = bson_iter_double (&ITER); \
        if (debug) \
            printf ("%s : %lf [BSON_TYPE_DOUBLE %d]\n", data_frame_info->KEY, float64_data, BSON_TYPE_DOUBLE); \
        _SET_INDEX_CHAIN_HEAD_TYPE (float64_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_DOUBLE, &float64_data, &CHAIN_HEAD); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_BOOL) { \
        bool_data = bson_iter_bool (&ITER); \
        if (debug) \
            printf ("%s : %d [BSON_TYPE_BOOL %d]\n", data_frame_info->KEY, bool_data, BSON_TYPE_BOOL); \
        _SET_INDEX_CHAIN_HEAD_TYPE (bool_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_BOOL, &bool_data, &CHAIN_HEAD); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_DATE_TIME) { \
        int64_data = bson_iter_date_time (&ITER); \
        if (debug) \
            printf ("%s : %ld [BSON_TYPE_DATE_TIME %d]\n", data_frame_info->KEY, int64_data, BSON_TYPE_DATE_TIME); \
        _SET_INDEX_CHAIN_HEAD_TYPE (int64_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_DATE_TIME, &int64_data, &CHAIN_HEAD); \
    }

#define _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(CHAIN_HEAD, CNT, TYPE, INDEX_NODE_T) \
    TYPE *array = (TYPE *) malloc (sizeof(TYPE) * CNT); \
    INDEX_NODE_T *p_node; \
    uint64_t i; \
    for (p_node = CHAIN_HEAD, i = 0; p_node != NULL; p_node = p_node->next, i++) { \
        array[i] = p_node->data; \
    }

#define _CREATE_INDEX_OR_COLUMN_ARRAY(BSON_TYPE, CHAIN_HEAD, CNT, STRING_ARRAY, INT32_ARRAY, INT64_ARRAY, DATE_TIME_ARRAY, FLOAT64_ARRAY, BOOL_ARRAY) \
    if (BSON_TYPE == BSON_TYPE_UTF8) { \
    } \
    else if (BSON_TYPE == BSON_TYPE_INT32) { \
    } \
    else if (BSON_TYPE == BSON_TYPE_INT64) { \
        _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(CHAIN_HEAD, CNT, int64_t, int64_index_node_t) \
        INT64_ARRAY = array; \
    } \
    else if (BSON_TYPE == BSON_TYPE_DATE_TIME) { \
        _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(CHAIN_HEAD, CNT, int64_t, date_time_index_node_t) \
        DATE_TIME_ARRAY = array; \
    } \
    else if (BSON_TYPE == BSON_TYPE_DOUBLE) { \
    } \
    else if (BSON_TYPE == BSON_TYPE_BOOL) { \
    }

data_frame_data_t * find (mongoc_collection_t *collection, data_frame_info_t *data_frame_info, bool debug)
{
    mongoc_cursor_t *cursor;
    const bson_t *doc;
    bson_t *query;
    bson_iter_t index_iter, column_iter;
    string_t string_data;
    int32_t int32_data;
    int64_t int64_data;
    float64_t float64_data;
    bool_t bool_data;
    uint32_t length;
    unsigned int i;
//    void *index_chain_head = NULL;
//    void *column_chain_head = NULL;
    bool is_first_doc = true;
    mongo_data_t *p_mongo_data = (mongo_data_t *) malloc (sizeof (mongo_data_t));
    data_frame_data_t *p_data_frame_data = (data_frame_data_t *) malloc (sizeof (data_frame_data_t));
    p_mongo_data->index_chain_head = NULL;
    p_mongo_data->column_chain_head = NULL;

    if (debug) {
        printf ("index: %s %d\ncolumn: %s %d\n", data_frame_info->index_key, data_frame_info->index_type,
            data_frame_info->column_key, data_frame_info->column_type);
        for (int i = 0; i < data_frame_info->value_cnt; i++) {
            printf ("%s %d\n", data_frame_info->value_keys[i], data_frame_info->value_types[i]);
        }
    }

    query = bson_new ();
    cursor = mongoc_collection_find_with_opts (collection, query, NULL, NULL);
    while (mongoc_cursor_next (cursor, &doc)) {
        if (bson_iter_init (&index_iter, doc) && bson_iter_find (&index_iter, data_frame_info->index_key) &&
                bson_iter_init (&column_iter, doc) && bson_iter_find (&column_iter, data_frame_info->column_key)) {
            _PROCESS_INDEX_OR_COLUMN (index_type, index_iter, index_key, p_mongo_data->index_chain_head)
            _PROCESS_INDEX_OR_COLUMN (column_type, column_iter, column_key, p_mongo_data->column_chain_head)
            is_first_doc = false;
        }
    }
    bson_destroy (query);

    p_data_frame_data->row_cnt = set_index_node_index (data_frame_info->index_type, p_mongo_data->index_chain_head);
    p_data_frame_data->col_cnt = set_index_node_index (data_frame_info->column_type, p_mongo_data->column_chain_head);
    _CREATE_INDEX_OR_COLUMN_ARRAY (data_frame_info->index_type, p_mongo_data->index_chain_head,
                                   p_data_frame_data->row_cnt, p_data_frame_data->string_index_array,
                                   p_data_frame_data->int32_index_array, p_data_frame_data->int64_index_array,
                                   p_data_frame_data->date_time_index_array, p_data_frame_data->float64_index_array,
                                   p_data_frame_data->bool_index_array)
    _CREATE_INDEX_OR_COLUMN_ARRAY (data_frame_info->column_type, p_mongo_data->column_chain_head,
                                   p_data_frame_data->col_cnt, p_data_frame_data->string_column_array,
                                   p_data_frame_data->int32_column_array, p_data_frame_data->int64_column_array,
                                   p_data_frame_data->date_time_column_array, p_data_frame_data->float64_column_array,
                                   p_data_frame_data->bool_column_array)

    if (debug) {
        int64_index_node_t *index_node;
        for (index_node = p_mongo_data->index_chain_head; index_node != NULL; index_node = index_node->next) {
            printf ("%lld %llu\n", index_node->data, index_node->index);
        }
        string_index_node_t *column_node;
        for (column_node = p_mongo_data->column_chain_head; column_node != NULL; column_node = column_node->next) {
            printf ("%s %llu\n", column_node->data, column_node->index);
        }
    }
    return p_data_frame_data;
}
