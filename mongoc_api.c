#include "mongoc_util/cymongo_type.h"
#include "mongoc_util/error_code.h"
#include "mongoc_util/basic_operation.c"
#include "mongoc_util/data_frame/init_data_structure.c"
#include "mongoc_util/data_frame/process_index_column_chain.c"
#include "mongoc_util/data_frame/process_value_chain.c"
#include "mongoc_util/data_frame/create_index_column_array.c"
#include "mongoc_util/data_frame/create_value_array.c"
#include "mongoc_util/table/process_table_chain.c"
#include "mongoc_util/table/create_table_array.c"
#include "mongoc_util/debug_print.c"
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

data_frame_data_t * find_as_data_frame (mongoc_collection_t *collection, data_frame_info_t *data_frame_info, bool debug)
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
    int64_t date_time_data;
    float64_t float64_data;
    bool_t bool_data;
    uint32_t length;
    uint64_t uni_string_length;
    uint64_t *p_index_idx, *p_column_idx;
    unsigned int i;
    void * p_this_node;
    // whether the document is the first document
    bool is_first_doc = true;
    // mongo_data_t
    mongo_data_t *p_mongo_data = init_mongo_data (data_frame_info->value_cnt);
    // data_frame_data_t
    data_frame_data_t *p_data_frame_data = init_data_frame_data (data_frame_info->value_cnt);
    time_t start_time, begin_time, begin, time0 = 0, time1 = 0, time2 = 0, time3 = 0, time11 = 0, time12 = 0, time21 = 0, time22 = 0;
    bool b;

    if (debug) {
        printf ("the index of DataFrame: %s %d\nthe column of DataFrame: %s %d\n", data_frame_info->index_key,
            data_frame_info->index_type, data_frame_info->column_key, data_frame_info->column_type);
        printf ("the values of DataFrame:\n");
        for (unsigned int value_idx = 0; value_idx < data_frame_info->value_cnt; value_idx++) {
            printf ("%s %d\n", data_frame_info->value_keys[value_idx], data_frame_info->value_types[value_idx]);
        }
    }
    begin_time = clock();

    query = bson_new ();
    cursor = mongoc_collection_find_with_opts (collection, query, NULL, NULL);
    while (mongoc_cursor_next (cursor, &doc)) {
        start_time = clock();
        b = bson_iter_init (&index_iter, doc) && bson_iter_find (&index_iter, data_frame_info->index_key) &&
                bson_iter_init (&column_iter, doc) && bson_iter_find (&column_iter, data_frame_info->column_key);
        time0 += clock() - start_time;
        if (b) {

            start_time = clock();
            _PROCESS_INDEX_OR_COLUMN (index_type, index_iter, index_key, p_mongo_data->index_chain_head, p_mongo_data->string_index_max_length, uni_string_length, p_index_idx)
            time1 += clock() - start_time;

            start_time = clock();
            _PROCESS_INDEX_OR_COLUMN (column_type, column_iter, column_key, p_mongo_data->column_chain_head, p_mongo_data->string_column_max_length, uni_string_length, p_column_idx)
            time2 += clock() - start_time;

            start_time = clock();
            _PROCESS_VALUE (data_frame_info, value_iter, p_mongo_data, p_index_idx, p_column_idx, p_data_frame_data->string_value_max_lengths)
            time3 += clock() - start_time;

            is_first_doc = false;
        }
    }
    printf ("step 1 : %ld\n", clock() - begin_time);
    printf ("time 0: %ld\n", time0);
    printf ("time 1: %ld\n", time1);
    printf ("time 2: %ld\n", time2);
    printf ("time 3: %ld\n", time3);
    printf ("time 11: %ld\n", time11);
    printf ("time 12: %ld\n", time12);
    printf ("time 21: %ld\n", time21);
    printf ("time 22: %ld\n", time22);

    bson_destroy (query);

    start_time = clock();

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

    printf ("time 4: %ld\n", clock() - start_time);
    printf ("total : %ld\n", clock() - begin_time);

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

table_t * find (mongoc_collection_t *collection, table_info_t *p_table_info, bool debug)
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
    int bson_type;
    uint64_t idx;
    char *string_data;
    int32_t int32_data;
    int64_t int64_data;
    int64_t date_time_data;
    float64_t float64_data;
    bool_t bool_data;

    string_node_t *p_string_node;
    int32_node_t *p_int32_node;
    int64_node_t *p_int64_node;
    date_time_node_t *p_date_time_node;
    float64_node_t *p_float64_node;
    bool_node_t *p_bool_node;

    uint64_t row_cnt;

    uint32_t length;
    uint64_t uni_string_length;
    uint64_t *p_index_idx, *p_column_idx;
    unsigned int column_cnt = p_table_info->column_cnt;
    void * p_this_node;

    query = bson_new ();
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
                    p_table_info->column_types[column_idx] = bson_type;
                }
                _PROCESS_TABLE (iter, bson_type, column_idx, row_cnt)
            }
        }
    }
    p_table->row_cnt = row_cnt;
    create_table_array (p_table_info, p_node_chain_heads, p_table);
    return p_table;
}
