#include "cymongo_types.h"
#include "error_code.h"
#include <string.h>

#define _CHAR_STRING_TO_UNICHAR_STRING(CHAR_STRING, UNI_CHAR_STRING, STRING_LENGTH, UNI_STRING_LENGTH) \
    bson_unichar_t __uni_char; \
    UNI_CHAR_STRING = (bson_unichar_t *) malloc (sizeof (bson_unichar_t) * STRING_LENGTH); \
    char *__char_string_iter = CHAR_STRING; \
    uint64_t __uni_string_length; \
    for (__uni_string_length = 0; *__char_string_iter; __char_string_iter = bson_utf8_next_char (__char_string_iter), __uni_string_length++) { \
        __uni_char = bson_utf8_get_char (__char_string_iter); \
        UNI_CHAR_STRING[__uni_string_length] = __uni_char; \
    } \
    UNI_STRING_LENGTH = __uni_string_length;

#define _CREATE_STR_DATA(P_NODE) \
    P_NODE->data = (char *) malloc (sizeof (char) * (strlen (data) + 1)); \
    strcpy (P_NODE->data, data); \
    _CHAR_STRING_TO_UNICHAR_STRING(data, P_NODE->uni_string.string, strlen(data), P_NODE->uni_string.length)

#define _CREATE_NUM_DATA(P_NODE) \
    P_NODE->data = data;

#define _CREATE_INDEX_NODE(INDEX_NODE_T, CREATE_DATA) \
    INDEX_NODE_T *p_node = (INDEX_NODE_T *) malloc (sizeof (INDEX_NODE_T) ); \
    CREATE_DATA(p_node) \
    p_node->next = next;

string_index_node_t * _create_string_index_node (char *data, string_index_node_t *next) {
    _CREATE_INDEX_NODE (string_index_node_t, _CREATE_STR_DATA)
    return p_node;
}

int32_index_node_t * _create_int32_index_node (int32_t data, int32_index_node_t *next) {
    _CREATE_INDEX_NODE (int32_index_node_t, _CREATE_NUM_DATA)
    return p_node;
}

int64_index_node_t * _create_int64_index_node (int64_t data, int64_index_node_t *next) {
    _CREATE_INDEX_NODE (int64_index_node_t, _CREATE_NUM_DATA)
    return p_node;
}

date_time_index_node_t * _create_date_time_index_node (int64_t data, date_time_index_node_t *next) {
    _CREATE_INDEX_NODE (date_time_index_node_t, _CREATE_NUM_DATA)
    return p_node;
}

float64_index_node_t * _create_float64_index_node (float64_t data, float64_index_node_t *next) {
    _CREATE_INDEX_NODE (float64_index_node_t, _CREATE_NUM_DATA)
    return p_node;
}

bool_index_node_t * _create_bool_index_node (bool_t data, bool_index_node_t *next) {
    _CREATE_INDEX_NODE (bool_index_node_t, _CREATE_NUM_DATA)
    return p_node;
}

#define _STR_CMP(S1, S2) \
    strcmp (S1, S2)

#define _NUM_CMP(N1, N2) \
    N1 - N2

#define _FIND_INDEX_NODE(INDEX_NODE_T, CMP) \
    INDEX_NODE_T *prev, *this; \
    int cmp_result; \
    for (prev = NULL, this = chain_head; this != NULL; prev = this, this = this->next) { \
        cmp_result = CMP(this->data, data); \
        if (cmp_result == 0) { \
            *prev_node = prev; \
            return true; \
        } \
        if (cmp_result > 0) { \
            *prev_node = prev; \
            return false; \
        } \
    } \
    *prev_node = prev; \
    return false;

bool _find_string_index_node (char *data, string_index_node_t *chain_head, string_index_node_t **prev_node) {
    _FIND_INDEX_NODE (string_index_node_t, _STR_CMP)
}

bool _find_int32_index_node (int32_t data, int32_index_node_t *chain_head, int32_index_node_t **prev_node) {
    _FIND_INDEX_NODE (int32_index_node_t, _NUM_CMP)
}

bool _find_int64_index_node (int64_t data, int64_index_node_t *chain_head, int64_index_node_t **prev_node) {
    _FIND_INDEX_NODE (int64_index_node_t, _NUM_CMP)
}

bool _find_date_time_index_node (int64_t data, date_time_index_node_t *chain_head, date_time_index_node_t **prev_node) {
    _FIND_INDEX_NODE (date_time_index_node_t, _NUM_CMP)
}

bool _find_float64_index_node (float64_t data, float64_index_node_t *chain_head, float64_index_node_t **prev_node) {
    _FIND_INDEX_NODE (float64_index_node_t, _NUM_CMP)
}

bool _find_bool_index_node (bool_t data, bool_index_node_t *chain_head, bool_index_node_t **prev_node) {
    _FIND_INDEX_NODE (bool_index_node_t, _NUM_CMP)
}

#define _INSERT_INDEX_NODE(INDEX_NODE_T, FIND_INDEX_NODE, CREATE_INDEX_NODE, STRING_LENGTH) \
    chain_head = (INDEX_NODE_T **) chain_head; \
    INDEX_NODE_T *prev_node, *next_node; \
    if (!FIND_INDEX_NODE (data, *chain_head, &prev_node)) { \
        if (prev_node == NULL) { \
            next_node = *chain_head; \
        } \
        else { \
            next_node = prev_node->next; \
        } \
        INDEX_NODE_T *p_new_node = CREATE_INDEX_NODE (data, next_node); \
        if (prev_node == NULL) { \
            *chain_head = p_new_node; \
        } \
        else { \
            prev_node->next = p_new_node; \
        }

int insert_index_node (bson_type_t type, void *p_data, void **chain_head, uint64_t string_length, uint64_t *uni_string_length) {
    if ( type == BSON_TYPE_UTF8 ) {
        char *data = (char *) p_data;
        _INSERT_INDEX_NODE (string_index_node_t, _find_string_index_node, _create_string_index_node, string_length)
            *uni_string_length = p_new_node->uni_string.length;
        }
    }
    else if ( type == BSON_TYPE_INT32 ) {
        int32_t *_p_data = (int32_t *) p_data;
        int32_t data = *_p_data;
        _INSERT_INDEX_NODE (int32_index_node_t, _find_int32_index_node, _create_int32_index_node, string_length)
        }
    }
    else if ( type == BSON_TYPE_INT64 ) {
        int64_t *_p_data = (int64_t *) p_data;
        int64_t data = *_p_data;
        _INSERT_INDEX_NODE (int64_index_node_t, _find_int64_index_node, _create_int64_index_node, string_length)
        }
    }
    else if ( type == BSON_TYPE_DATE_TIME ) {
        int64_t *_p_data = (int64_t *) p_data;
        int64_t data = *_p_data;
        _INSERT_INDEX_NODE (date_time_index_node_t, _find_date_time_index_node, _create_date_time_index_node, string_length)
        }
    }
    else if ( type == BSON_TYPE_DOUBLE ) {
        float64_t *_p_data = (float64_t *) p_data;
        float64_t data = *_p_data;
        _INSERT_INDEX_NODE (float64_index_node_t, _find_float64_index_node, _create_float64_index_node, string_length)
        }
    }
    else if ( type == BSON_TYPE_BOOL ) {
        bool_t *_p_data = (bool_t *) p_data;
        bool_t data = *_p_data;
        _INSERT_INDEX_NODE (bool_index_node_t, _find_bool_index_node, _create_bool_index_node, string_length)
        }
    }
    else {
        return UNSUPPORTED_TYPE_CODE;
    }
    return SUCCESS_CODE;
}

#define _SET_INDEX_NODE_INDEX(INDEX_NODE_T) \
    chain_head = (INDEX_NODE_T *) chain_head; \
    INDEX_NODE_T *node; \
    for (node = chain_head, node_cnt = 0; node != NULL; node = node->next, node_cnt++) { \
        node->index = node_cnt; \
    }

int set_index_node_index (bson_type_t type, void *chain_head) {
    uint64_t node_cnt;
    if (type == BSON_TYPE_UTF8) {
        _SET_INDEX_NODE_INDEX (string_index_node_t)
    }
    else if (type == BSON_TYPE_INT32) {
        _SET_INDEX_NODE_INDEX (int32_index_node_t)
    }
    else if (type == BSON_TYPE_INT64) {
        _SET_INDEX_NODE_INDEX (int64_index_node_t)
    }
    else if (type == BSON_TYPE_DATE_TIME) {
        _SET_INDEX_NODE_INDEX (date_time_index_node_t)
    }
    else if (type == BSON_TYPE_DOUBLE) {
        _SET_INDEX_NODE_INDEX (float64_index_node_t)
    }
    else if (type == BSON_TYPE_BOOL) {
        _SET_INDEX_NODE_INDEX (bool_index_node_t)
    }
    else {
        return UNSUPPORTED_TYPE_CODE;
    }
    return node_cnt;
}

#define _SET_INDEX_CHAIN_HEAD_TYPE(INDEX_NODE_T, CHAIN_HEAD) \
    if (is_first_doc) { \
        CHAIN_HEAD = (INDEX_NODE_T *) CHAIN_HEAD; \
    }

#define _PROCESS_INDEX_OR_COLUMN(TYPE, ITER, KEY, CHAIN_HEAD, MAX_STRING_LENGTH, UNI_STRING_LENGTH) \
    if (data_frame_info->TYPE == BSON_TYPE_UNKNOWN) { \
        data_frame_info->TYPE = bson_iter_type(&ITER); \
    } \
    if (data_frame_info->TYPE == BSON_TYPE_UTF8) { \
        string_data = bson_iter_utf8 (&ITER, &length); \
        if (debug) \
            printf ("%s : %s [BSON_TYPE_UTF8 %d]\n", data_frame_info->KEY, string_data, BSON_TYPE_UTF8); \
        _SET_INDEX_CHAIN_HEAD_TYPE (string_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_UTF8, string_data, &CHAIN_HEAD, length, &UNI_STRING_LENGTH); \
        if (UNI_STRING_LENGTH > MAX_STRING_LENGTH) { \
            MAX_STRING_LENGTH = UNI_STRING_LENGTH; \
        } \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_INT32) { \
        int32_data = bson_iter_int32 (&ITER); \
        if (debug) \
            printf ("%s : %d [BSON_TYPE_INT32 %d]\n", data_frame_info->KEY, int32_data, BSON_TYPE_INT32); \
        _SET_INDEX_CHAIN_HEAD_TYPE (int32_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_INT32, &int32_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_INT64) { \
        int64_data = bson_iter_int64 (&ITER); \
        if (debug) \
            printf ("%s : %ld [BSON_TYPE_INT64 %d]\n", data_frame_info->KEY, int64_data, BSON_TYPE_INT64); \
        _SET_INDEX_CHAIN_HEAD_TYPE (int64_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_INT64, &int64_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_DOUBLE) { \
        float64_data = bson_iter_double (&ITER); \
        if (debug) \
            printf ("%s : %lf [BSON_TYPE_DOUBLE %d]\n", data_frame_info->KEY, float64_data, BSON_TYPE_DOUBLE); \
        _SET_INDEX_CHAIN_HEAD_TYPE (float64_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_DOUBLE, &float64_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_BOOL) { \
        bool_data = bson_iter_bool (&ITER); \
        if (debug) \
            printf ("%s : %d [BSON_TYPE_BOOL %d]\n", data_frame_info->KEY, bool_data, BSON_TYPE_BOOL); \
        _SET_INDEX_CHAIN_HEAD_TYPE (bool_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_BOOL, &bool_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_DATE_TIME) { \
        int64_data = bson_iter_date_time (&ITER); \
        if (debug) \
            printf ("%s : %ld [BSON_TYPE_DATE_TIME %d]\n", data_frame_info->KEY, int64_data, BSON_TYPE_DATE_TIME); \
        _SET_INDEX_CHAIN_HEAD_TYPE (int64_index_node_t, CHAIN_HEAD) \
        insert_index_node (BSON_TYPE_DATE_TIME, &int64_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
    }

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
