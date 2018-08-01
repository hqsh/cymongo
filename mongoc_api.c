#include "mongoc_util/init_data_structure.c"
#include "mongoc_util/process_index_column_chain.c"
#include "mongoc_util/process_value_chain.c"
#include "mongoc_util/create_index_column_array.c"
#include "mongoc_util/create_value_array.c"
#include "mongoc_util/debug_print.c"

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
    // mongodb variable
    mongoc_cursor_t *cursor;
    const bson_t *doc;
    bson_t *query;
    bson_iter_t index_iter, column_iter, value_iter;
    // temporary variable
    char *string_data;
    int32_t int32_data;
    int64_t int64_data;
    float64_t float64_data;
    bool_t bool_data;
    uint32_t length;
    uint64_t uni_string_length;
    uint64_t *p_index_idx, *p_column_idx;
    unsigned int i;
    // whether the document is the first document
    bool is_first_doc = true;
    // mongo_data_t
    mongo_data_t *p_mongo_data = init_mongo_data (data_frame_info->value_cnt);
    // data_frame_data_t
    data_frame_data_t *p_data_frame_data = init_data_frame_data (data_frame_info->value_cnt);

    if (debug) {
        printf ("the index of DataFrame: %s %d\nthe column of DataFrame: %s %d\n", data_frame_info->index_key,
            data_frame_info->index_type, data_frame_info->column_key, data_frame_info->column_type);
        printf ("the values of DataFrame:\n");
    }
    for (unsigned int value_idx = 0; value_idx < data_frame_info->value_cnt; value_idx++) {
        if (debug) {
            printf ("%s %d\n", data_frame_info->value_keys[value_idx], data_frame_info->value_types[value_idx]);
        }
    }

    query = bson_new ();
    cursor = mongoc_collection_find_with_opts (collection, query, NULL, NULL);
    while (mongoc_cursor_next (cursor, &doc)) {
        if (bson_iter_init (&index_iter, doc) && bson_iter_find (&index_iter, data_frame_info->index_key) &&
                bson_iter_init (&column_iter, doc) && bson_iter_find (&column_iter, data_frame_info->column_key)) {
            _PROCESS_INDEX_OR_COLUMN (index_type, index_iter, index_key, p_mongo_data->index_chain_head, p_mongo_data->string_index_max_length, uni_string_length, p_index_idx)
            _PROCESS_INDEX_OR_COLUMN (column_type, column_iter, column_key, p_mongo_data->column_chain_head, p_mongo_data->string_column_max_length, uni_string_length, p_column_idx)
            _PROCESS_VALUE (data_frame_info, value_iter, p_mongo_data, p_index_idx, p_column_idx, p_data_frame_data->string_value_max_lengths)
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

    create_value_array (data_frame_info, p_mongo_data, p_data_frame_data);

    if (debug) {
        print_values_in_chains (data_frame_info, p_mongo_data);
    }

    if (debug) {
        int64_index_node_t *index_node;
        for (index_node = p_mongo_data->index_chain_head; index_node != NULL; index_node = index_node->next) {
            printf ("%lld %llu\n", index_node->data, index_node->idx);
        }
        string_index_node_t *column_node;
        for (column_node = p_mongo_data->column_chain_head; column_node != NULL; column_node = column_node->next) {
            printf ("%s %llu\n", column_node->data, column_node->idx);
        }
    }
    return p_data_frame_data;
}
