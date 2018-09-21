#include "mongoc_util/cymongo_type.h"
#include "mongoc_util/error_code.h"
#include "mongoc_util/basic_operation.c"
#include "mongoc_util/data_frame/init_data_structure.c"
#include "mongoc_util/data_frame/process_mongo_data.c"
#include "mongoc_util/data_frame/create_array.c"
#include "mongoc_util/data_frame/free_memory.c"
#include "mongoc_util/table/process_mongo_data.c"
#include "mongoc_util/table/create_array.c"
#include <time.h>

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

data_frame_data_t * find_as_data_frame (mongoc_collection_t *collection, default_nan_value_t* p_default_nan_value, data_frame_info_t *data_frame_info, char *filter, bool debug)
{
    // mongodb variable
    mongoc_cursor_t *cursor;
    const bson_t *doc;
    bson_t *query;
    bson_iter_t index_iter, column_iter, value_iter;
    // temporary variable
    _DEFINE_TEMPORARY_VARIABLE
    string_index_t *p_string_index, *p_tmp_string_index;
    int32_index_t *p_int32_index, *p_tmp_int32_index;
    int64_index_t *p_int64_index, *p_tmp_int64_index;
    date_time_index_t *p_date_time_index, *p_tmp_date_time_index;
    float64_index_t *p_float64_index, *p_tmp_float64_index;
    bool_index_t *p_bool_index, *p_tmp_bool_index;
    unsigned int i;
    void * p_this_node;
    bool b;

    // init mongo_data_t
    mongo_data_t *p_mongo_data = init_mongo_data (data_frame_info->value_cnt);
    // init data_frame_data_t
    time_t begin_time = clock();
    data_frame_data_t *p_data_frame_data = init_data_frame_data (data_frame_info->value_cnt);

    if (debug) {
        printf ("find_as_data_frame step 1 [init_data_frame_data], cost: %f\n", (clock() - begin_time) / 1000000.0);
        printf ("the index of DataFrame: %s %d\nthe column of DataFrame: %s %d\n", data_frame_info->index_key,
            data_frame_info->index_type, data_frame_info->column_key, data_frame_info->column_type);
        printf ("the values of DataFrame:\n");
        for (unsigned int value_idx = 0; value_idx < data_frame_info->value_cnt; value_idx++) {
            printf ("%s %d\n", data_frame_info->value_keys[value_idx], data_frame_info->value_types[value_idx]);
        }
    }

    if (filter == NULL) {
        query = bson_new ();
    }
    else {
        query = bson_new_from_json ((uint8_t *) filter, strlen(filter), NULL);
    }
    cursor = mongoc_collection_find_with_opts (collection, query, NULL, NULL);
    while (mongoc_cursor_next (cursor, &doc)) {
        b = bson_iter_init (&index_iter, doc) && bson_iter_find (&index_iter, data_frame_info->index_key) &&
                bson_iter_init (&column_iter, doc) && bson_iter_find (&column_iter, data_frame_info->column_key);
        if (b) {
            _PROCESS_INDEX_OR_COLUMN (true, index_type, index_iter, p_mongo_data, p_mongo_data->string_index_max_length, p_index_idx, p_data_frame_data)
            _PROCESS_INDEX_OR_COLUMN (false, column_type, column_iter, p_mongo_data, p_mongo_data->string_column_max_length, p_column_idx, p_data_frame_data)
            _PROCESS_VALUE (data_frame_info, value_iter, p_mongo_data, p_index_idx, p_column_idx, p_data_frame_data->string_value_max_lengths)
        }
    }

    if (debug) {
        printf ("find_as_data_frame step 2 [process_mongo_data], cost: %f\n", (clock() - begin_time) / 1000000.0);
    }

    bson_destroy (query);

    _CREATE_INDEX_OR_COLUMN_ARRAY (true, data_frame_info->index_type, p_mongo_data,
                                   p_mongo_data->string_index_max_length, p_data_frame_data->row_cnt,
                                   p_data_frame_data->string_index_max_length, p_data_frame_data->string_index_array,
                                   p_data_frame_data->int32_index_array, p_data_frame_data->int64_index_array,
                                   p_data_frame_data->date_time_index_array, p_data_frame_data->float64_index_array,
                                   p_data_frame_data->bool_index_array)
    if (debug) {
        printf ("find_as_data_frame step 3 [create_index_array], cost: %f\n", (clock() - begin_time) / 1000000.0);
    }
    _CREATE_INDEX_OR_COLUMN_ARRAY (false, data_frame_info->column_type, p_mongo_data,
                                   p_mongo_data->string_column_max_length, p_data_frame_data->col_cnt,
                                   p_data_frame_data->string_column_max_length, p_data_frame_data->string_column_array,
                                   p_data_frame_data->int32_column_array, p_data_frame_data->int64_column_array,
                                   p_data_frame_data->date_time_column_array, p_data_frame_data->float64_column_array,
                                   p_data_frame_data->bool_column_array)
    if (debug) {
        printf ("find_as_data_frame step 4 [create_column_array], cost: %f\n", (clock() - begin_time) / 1000000.0);
    }
    create_value_array (data_frame_info, p_mongo_data, p_data_frame_data, p_default_nan_value);
    if (debug) {
        printf ("find_as_data_frame step 5 [create_value_array], cost: %f\n", (clock() - begin_time) / 1000000.0);
    }
    // free memory
    free_data_frame_memory (p_mongo_data);
    if (debug) {
        printf ("find_as_data_frame step 6 [free_memory], cost: %f\n", (clock() - begin_time) / 1000000.0);
    }
    return p_data_frame_data;
}


table_t * find_as_table (mongoc_collection_t *collection, default_nan_value_t* p_default_nan_value, table_info_t *p_table_info, char *filter, bool debug)
{
    // mongodb variable
    mongoc_cursor_t *cursor;
    const bson_t *doc;
    bson_t *query;
    bson_iter_t iter;

    // node_chain_heads_t
    node_chain_heads_t *p_node_chain_heads;

    // table_t
    table_t * p_table;

    // temporary variable
    _DEFINE_TEMPORARY_VARIABLE
    int bson_type;
    uint64_t idx;
    string_node_t *p_string_node;
    int32_node_t *p_int32_node;
    int64_node_t *p_int64_node;
    date_time_node_t *p_date_time_node;
    float64_node_t *p_float64_node;
    bool_node_t *p_bool_node;
    uint64_t row_cnt;
    unsigned int column_cnt = p_table_info->column_cnt;
    void * p_this_node;

    if (filter == NULL) {
        query = bson_new ();
    }
    else {
        query = bson_new_from_json ((uint8_t *) filter, strlen(filter), NULL);
    }
    cursor = mongoc_collection_find_with_opts (collection, query, NULL, NULL);

    p_node_chain_heads = init_node_chain_heads_t (column_cnt);
    p_table = init_table (column_cnt);

    for (row_cnt = 0; mongoc_cursor_next (cursor, &doc); row_cnt++) {
        for (unsigned int column_idx = 0; column_idx < column_cnt; column_idx++) {
            if (bson_iter_init (&iter, doc) && bson_iter_find (&iter, p_table_info->column_names[column_idx])) {
                bson_type = p_table_info->column_types[column_idx];
                if (bson_type == BSON_TYPE_UNKNOWN) {
                    if (debug) {
                        printf ("%s\n", p_table_info->column_names[column_idx]);
                    }
                    bson_type = bson_iter_type(&iter);
                    if (bson_type == BSON_TYPE_INT32) {
                        bson_type = BSON_TYPE_INT64;
                    }
                    p_table_info->column_types[column_idx] = bson_type;
                }
                is_int64 = bson_iter_type(&iter) == BSON_TYPE_INT64;
                _PROCESS_TABLE (iter, bson_type, column_idx, row_cnt)
            }
        }
    }
    p_table->row_cnt = row_cnt;
    create_table_array (p_table_info, p_node_chain_heads, p_table, p_default_nan_value);
    return p_table;
}
