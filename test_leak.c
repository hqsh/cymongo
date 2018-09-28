#include "mongoc_api.c"

#define TEST_TABLE 0
#define TEST_DATA_FRAME 1

#define TEST_MODE TEST_DATA_FRAME


int main()
{
    printf ("run test start.\n");
    const char *mongoc_uri = "mongodb://hqs:f@localhost:27017/test";
    int error_code;
    struct _mongoc_client_t *p_client = get_client (mongoc_uri, NULL, &error_code);
    mongoc_database_t *p_db = get_database (p_client, "test");
    mongoc_collection_t *p_collection = get_collection (p_client, "test", "cymongo");
    default_nan_value_t default_nan_value;
    default_nan_value.default_int32_nan_value = 0;
    default_nan_value.default_int64_nan_value = 0;
    default_nan_value.default_date_time_nan_value = 0;
    default_nan_value.default_bool_nan_value = false;
    if (TEST_MODE == TEST_TABLE) {
        printf ("test mode: test table\n");
        table_info_t table_info;
        table_info.column_names = (char **) malloc (sizeof(char *));
        table_info.column_names[0] = (char *) malloc (strlen("blogger_id") + 1);
        strcpy (table_info.column_names[0], "blogger_id");
        table_info.column_types = (int *) malloc (sizeof(int));
        table_info.column_types[0] = -1;
        table_info.column_cnt = 1;
        table_t * p_table = find_as_table (p_collection, &default_nan_value, &table_info, "{}", NULL, false);
        free_table_memory (p_table, 1);
    }
    else if (TEST_MODE == TEST_DATA_FRAME) {
        printf ("test mode: test data frame\n");
        data_frame_info_t data_frame_info;
        data_frame_info.index_key = (char *) malloc (strlen("date_time") + 1);
        strcpy (data_frame_info.index_key, "date_time");
        data_frame_info.index_type = -1;
        data_frame_info.column_key = (char *) malloc (strlen("blogger_id") + 1);
        strcpy (data_frame_info.column_key, "date_time");
        data_frame_info.column_type = -1;
        data_frame_info.value_cnt = 1;
        data_frame_info.value_keys = (char **) malloc (sizeof(char *) * data_frame_info.value_cnt);
        data_frame_info.value_keys[0] = (char *) malloc (strlen("fan_cnt") + 1);
        strcpy (data_frame_info.value_keys[0], "fan_cnt");
        data_frame_info.value_types = (int *) malloc (sizeof(int) * data_frame_info.value_cnt);
        data_frame_info.value_types[0] = -1;
        data_frame_data_t * p_data_frame_data = find_as_data_frame (p_collection, &default_nan_value, &data_frame_info, "{}", NULL, false);
        free_data_frame_memory (p_data_frame_data, 1);
    }
    printf ("run test end.\n");
    return 0;
}
