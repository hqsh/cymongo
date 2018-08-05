#define _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(CHAIN_HEAD, CNT, TYPE, INDEX_NODE_T) \
    TYPE *__array = (TYPE *) malloc (sizeof(TYPE) * CNT); \
    INDEX_NODE_T *p_node; \
    uint64_t i; \
    for (p_node = CHAIN_HEAD, i = 0; p_node != NULL; p_node = p_node->next, i++) { \
        __array[i] = p_node->data; \
    }

#define _CREATE_INDEX_OR_COLUMN_ARRAY(BSON_TYPE, CHAIN_HEAD, MAX_STRING_LENGTH, CNT, DF_STRING_MAX_LENGTH, STRING_ARRAY, INT32_ARRAY, INT64_ARRAY, DATE_TIME_ARRAY, FLOAT64_ARRAY, BOOL_ARRAY) \
    if (BSON_TYPE == BSON_TYPE_UTF8) { \
        bson_unichar_t *__array = (bson_unichar_t *) malloc (sizeof(bson_unichar_t) * (MAX_STRING_LENGTH) * CNT); \
        memset (__array, 0, sizeof(bson_unichar_t) * (MAX_STRING_LENGTH) * CNT); \
        string_index_node_t *p_node; \
        uint64_t i, __zero_uni_char_cnt; \
        for (p_node = CHAIN_HEAD, i = 0; p_node != NULL; p_node = p_node->next, i++) { \
            memcpy (__array + (i * MAX_STRING_LENGTH), p_node->uni_string.string, p_node->uni_string.length * sizeof(bson_unichar_t)); \
        } \
        STRING_ARRAY = (bson_unichar_t *) __array; \
        DF_STRING_MAX_LENGTH = MAX_STRING_LENGTH; \
    } \
    else if (BSON_TYPE == BSON_TYPE_INT32) { \
        _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(CHAIN_HEAD, CNT, int32_t, int32_index_node_t) \
        INT32_ARRAY = __array; \
    } \
    else if (BSON_TYPE == BSON_TYPE_INT64) { \
        _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(CHAIN_HEAD, CNT, int64_t, int64_index_node_t) \
        INT64_ARRAY = __array; \
    } \
    else if (BSON_TYPE == BSON_TYPE_DATE_TIME) { \
        _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(CHAIN_HEAD, CNT, int64_t, date_time_index_node_t) \
        DATE_TIME_ARRAY = __array; \
    } \
    else if (BSON_TYPE == BSON_TYPE_DOUBLE) { \
        _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(CHAIN_HEAD, CNT, float64_t, float64_index_node_t) \
        FLOAT64_ARRAY = __array; \
    } \
    else if (BSON_TYPE == BSON_TYPE_BOOL) { \
        _CREATE_INDEX_OR_COLUMN_NUM_ARRAY(CHAIN_HEAD, CNT, bool_t, bool_index_node_t) \
        BOOL_ARRAY = __array; \
    }
