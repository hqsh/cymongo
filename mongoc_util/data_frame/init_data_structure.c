#include <stdio.h>

// init mongo_data_t for "find" function
mongo_data_t * init_mongo_data (unsigned int value_cnt) {
    unsigned int i;
    mongo_data_t *p_mongo_data = (mongo_data_t *) malloc (sizeof (mongo_data_t));
    memset (p_mongo_data, 0, sizeof(mongo_data_t));
    p_mongo_data->string_value_chain_heads = (string_value_node_t **) malloc (sizeof(string_value_node_t *) * value_cnt);
    p_mongo_data->int32_value_chain_heads = (int32_value_node_t **) malloc (sizeof(int32_value_node_t *) * value_cnt);
    p_mongo_data->int64_value_chain_heads = (int64_value_node_t **) malloc (sizeof(int64_value_node_t *) * value_cnt);
    p_mongo_data->date_time_value_chain_heads = (date_time_value_node_t **) malloc (sizeof(date_time_value_node_t *) * value_cnt);
    p_mongo_data->float64_value_chain_heads = (float64_value_node_t **) malloc (sizeof(float64_value_node_t *) * value_cnt);
    p_mongo_data->bool_value_chain_heads = (bool_value_node_t **) malloc (sizeof(bool_value_node_t *) * value_cnt);
    for (unsigned int i = 0; i < value_cnt; i++) {
        p_mongo_data->string_value_chain_heads[i] = NULL;
        p_mongo_data->int32_value_chain_heads[i] = NULL;
        p_mongo_data->int64_value_chain_heads[i] = NULL;
        p_mongo_data->date_time_value_chain_heads[i] = NULL;
        p_mongo_data->float64_value_chain_heads[i] = NULL;
        p_mongo_data->bool_value_chain_heads[i] = NULL;
    }
    return p_mongo_data;
}

// init data_frame_data_t for "find" function
data_frame_data_t * init_data_frame_data (unsigned int value_cnt) {
    data_frame_data_t *p_data_frame_data = (data_frame_data_t *) malloc (sizeof (data_frame_data_t));
    memset (p_data_frame_data, 0, sizeof(data_frame_data_t));

    p_data_frame_data->string_value_max_lengths = (uint64_t *) malloc (sizeof(uint64_t) * value_cnt);
    memset (p_data_frame_data->string_value_max_lengths, 0, sizeof(uint64_t) * value_cnt);
    p_data_frame_data->string_value_arrays = (bson_unichar_t **) malloc (sizeof(bson_unichar_t *) * value_cnt);
    memset (p_data_frame_data->string_value_arrays, 0, sizeof(bson_unichar_t *) * value_cnt);
    p_data_frame_data->int32_value_arrays = (int32_t **) malloc (sizeof(int32_t *) * value_cnt);
    memset (p_data_frame_data->int32_value_arrays, 0, sizeof(int32_t *) * value_cnt);
    p_data_frame_data->int64_value_arrays = (int64_t **) malloc (sizeof(int64_t *) * value_cnt);
    memset (p_data_frame_data->int64_value_arrays, 0, sizeof(int64_t *) * value_cnt);
    p_data_frame_data->date_time_value_arrays = (int64_t **) malloc (sizeof(int64_t *) * value_cnt);
    memset (p_data_frame_data->date_time_value_arrays, 0, sizeof(int64_t *) * value_cnt);
    p_data_frame_data->float64_value_arrays = (float64_t **) malloc (sizeof(float64_t *) * value_cnt);
    memset (p_data_frame_data->float64_value_arrays, 0, sizeof(float64_t *) * value_cnt);
    p_data_frame_data->bool_value_arrays = (bool_t **) malloc (sizeof(bool_t *) * value_cnt);
    memset (p_data_frame_data->bool_value_arrays, 0, sizeof(bool_t *) * value_cnt);
    return p_data_frame_data;
}
