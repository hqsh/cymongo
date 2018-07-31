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

data_frame_data_t * find (mongoc_collection_t *collection, data_frame_info_t *data_frame_info, bool debug)
{
    mongoc_cursor_t *cursor;
    const bson_t *doc;
    bson_t *query;
    bson_iter_t index_iter, column_iter, value_iter;
    char *string_data;
    int32_t int32_data;
    int64_t int64_data;
    float64_t float64_data;
    bool_t bool_data;
    uint32_t length;
    uint64_t uni_string_length;
    unsigned int i;
    bool is_first_doc = true;
    mongo_data_t *p_mongo_data = (mongo_data_t *) malloc (sizeof (mongo_data_t));
    p_mongo_data->index_chain_head = NULL;
    p_mongo_data->column_chain_head = NULL;
    p_mongo_data->string_index_max_length = 0;
    p_mongo_data->string_column_max_length = 0;
    p_mongo_data->string_value_key_index = NULL;
    p_mongo_data->int32_value_key_index = NULL;
    p_mongo_data->int64_value_key_index = NULL;
    p_mongo_data->date_time_value_key_index = NULL;
    p_mongo_data->float64_value_key_index = NULL;
    p_mongo_data->bool_value_key_index = NULL;
    p_mongo_data->string_value_cnt = 0;
    p_mongo_data->int32_value_cnt = 0;
    p_mongo_data->int64_value_cnt = 0;
    p_mongo_data->date_time_value_cnt = 0;
    p_mongo_data->float64_value_cnt = 0;
    p_mongo_data->bool_value_cnt = 0;
    data_frame_data_t *p_data_frame_data = (data_frame_data_t *) malloc (sizeof (data_frame_data_t));
    p_data_frame_data->string_index_array = NULL;
    p_data_frame_data->int32_index_array = NULL;
    p_data_frame_data->int64_index_array = NULL;
    p_data_frame_data->date_time_index_array = NULL;
    p_data_frame_data->float64_index_array = NULL;
    p_data_frame_data->bool_index_array = NULL;
    p_data_frame_data->string_column_array = NULL;
    p_data_frame_data->int32_column_array = NULL;
    p_data_frame_data->int64_column_array = NULL;
    p_data_frame_data->date_time_column_array = NULL;
    p_data_frame_data->float64_column_array = NULL;
    p_data_frame_data->bool_column_array = NULL;

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
            _PROCESS_INDEX_OR_COLUMN (index_type, index_iter, index_key, p_mongo_data->index_chain_head, p_mongo_data->string_index_max_length, uni_string_length)
            _PROCESS_INDEX_OR_COLUMN (column_type, column_iter, column_key, p_mongo_data->column_chain_head, p_mongo_data->string_column_max_length, uni_string_length)
            for (int i = 0; i < data_frame_info->value_cnt; i++) {
                if (bson_iter_init (&column_iter, doc) && bson_iter_find (&value_iter, data_frame_info->value_keys[i])) {
                    if (data_frame_info->value_types[i] == BSON_TYPE_UNKNOWN) {
                        data_frame_info->value_types[i] = bson_iter_type(&value_iter);
                        if (data_frame_info->value_types[i] == BSON_TYPE_UTF8) {
//                            _SET_VALUE_KEY_TYPE (p_mongo_data->string_value_keys)
                        }
                    }
                }
            }
            is_first_doc = false;
        }
    }
    bson_destroy (query);

    p_data_frame_data->row_cnt = set_index_node_index (data_frame_info->index_type, p_mongo_data->index_chain_head);
    p_data_frame_data->col_cnt = set_index_node_index (data_frame_info->column_type, p_mongo_data->column_chain_head);
    _CREATE_INDEX_OR_COLUMN_ARRAY (data_frame_info->index_type, p_mongo_data->index_chain_head,
                                   p_mongo_data->string_index_max_length, p_data_frame_data->row_cnt,
                                   p_data_frame_data->string_index_max_length,
                                   p_data_frame_data->string_index_array, p_data_frame_data->int32_index_array,
                                   p_data_frame_data->int64_index_array, p_data_frame_data->date_time_index_array,
                                   p_data_frame_data->float64_index_array, p_data_frame_data->bool_index_array)
    _CREATE_INDEX_OR_COLUMN_ARRAY (data_frame_info->column_type, p_mongo_data->column_chain_head,
                                   p_mongo_data->string_column_max_length, p_data_frame_data->col_cnt,
                                   p_data_frame_data->string_column_max_length,
                                   p_data_frame_data->string_column_array, p_data_frame_data->int32_column_array,
                                   p_data_frame_data->int64_column_array, p_data_frame_data->date_time_column_array,
                                   p_data_frame_data->float64_column_array, p_data_frame_data->bool_column_array)

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
