#include "mongoc_api.c"
#ifdef DMALLOC
#include "dmalloc.h"
#endif


int main()
{
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
    table_info_t table_info;
    table_info.column_names = (char **) malloc (sizeof(char *));
    printf ("=========================\n");
//    table_info.column_names[0] = (char *) malloc (strlen("blogger_id") + 1);
//    strcpy (table_info.column_names[0], "blogger_id");
//    table_info.column_types = (int *) malloc (sizeof(int));
//    table_info.column_types[0] = -1;
//    table_info.column_cnt = 1;
//    find_as_table (p_collection, &default_nan_value, &table_info, "{}", NULL, false);
    return 0;
}
