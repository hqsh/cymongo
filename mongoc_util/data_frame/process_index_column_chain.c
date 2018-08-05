#include <stdio.h>

#define _CREATE_INDEX_NODE(INDEX_NODE_T, CREATE_DATA, DATA) \
    INDEX_NODE_T *p_node = (INDEX_NODE_T *) malloc (sizeof (INDEX_NODE_T) ); \
    CREATE_DATA (p_node, DATA) \
    p_node->next = next; \
    p_node->idx = 0;

string_index_node_t * _create_string_index_node (char *data, string_index_node_t *next) {
    _CREATE_INDEX_NODE (string_index_node_t, _CREATE_CHAR_STRING_DATA, data)
    return p_node;
}

int32_index_node_t * _create_int32_index_node (int32_t data, int32_index_node_t *next) {
    _CREATE_INDEX_NODE (int32_index_node_t, _CREATE_NUMBER_DATA, data)
    return p_node;
}

int64_index_node_t * _create_int64_index_node (int64_t data, int64_index_node_t *next) {
    _CREATE_INDEX_NODE (int64_index_node_t, _CREATE_NUMBER_DATA, data)
    return p_node;
}

date_time_index_node_t * _create_date_time_index_node (int64_t data, date_time_index_node_t *next) {
    _CREATE_INDEX_NODE (date_time_index_node_t, _CREATE_NUMBER_DATA, data)
    return p_node;
}

float64_index_node_t * _create_float64_index_node (float64_t data, float64_index_node_t *next) {
    _CREATE_INDEX_NODE (float64_index_node_t, _CREATE_NUMBER_DATA, data)
    return p_node;
}

bool_index_node_t * _create_bool_index_node (bool_t data, bool_index_node_t *next) {
    _CREATE_INDEX_NODE (bool_index_node_t, _CREATE_NUMBER_DATA, data)
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
            return (void *) this; \
        } \
        if (cmp_result > 0) { \
            *prev_node = prev; \
            return NULL; \
        } \
    } \
    *prev_node = prev; \
    return NULL;

void * _find_string_index_node (char *data, string_index_node_t *chain_head, string_index_node_t **prev_node) {
    _FIND_INDEX_NODE (string_index_node_t, _STR_CMP)
}

void *  _find_int32_index_node (int32_t data, int32_index_node_t *chain_head, int32_index_node_t **prev_node) {
    _FIND_INDEX_NODE (int32_index_node_t, _NUM_CMP)
}

void *  _find_int64_index_node (int64_t data, int64_index_node_t *chain_head, int64_index_node_t **prev_node) {
    _FIND_INDEX_NODE (int64_index_node_t, _NUM_CMP)
}

void *  _find_date_time_index_node (int64_t data, date_time_index_node_t *chain_head, date_time_index_node_t **prev_node) {
    _FIND_INDEX_NODE (date_time_index_node_t, _NUM_CMP)
}

void *  _find_float64_index_node (float64_t data, float64_index_node_t *chain_head, float64_index_node_t **prev_node) {
    _FIND_INDEX_NODE (float64_index_node_t, _NUM_CMP)
}

void *  _find_bool_index_node (bool_t data, bool_index_node_t *chain_head, bool_index_node_t **prev_node) {
    _FIND_INDEX_NODE (bool_index_node_t, _NUM_CMP)
}

#define _INSERT_INDEX_NODE(INDEX_NODE_T, FIND_INDEX_NODE, CREATE_INDEX_NODE, STRING_LENGTH, P_THIS_NODE) \
    chain_head = (INDEX_NODE_T **) chain_head; \
    INDEX_NODE_T *prev_node, *next_node; \
    P_THIS_NODE = FIND_INDEX_NODE (data, *chain_head, &prev_node); \
    if (P_THIS_NODE == NULL) { \
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
        } \
        P_THIS_NODE = (INDEX_NODE_T *) p_new_node;

void * insert_index_node (bson_type_t type, void *p_data, void **chain_head, uint64_t string_length, uint64_t *uni_string_length) {
    void * p_this_node;
    if ( type == BSON_TYPE_UTF8 ) {
        char *data = (char *) p_data;
        _INSERT_INDEX_NODE (string_index_node_t, _find_string_index_node, _create_string_index_node, string_length, p_this_node)
            *uni_string_length = ((string_index_node_t *)p_this_node)->uni_string.length;
        }
    }
    else if ( type == BSON_TYPE_INT32 ) {
        int32_t *_p_data = (int32_t *) p_data;
        int32_t data = *_p_data;
        _INSERT_INDEX_NODE (int32_index_node_t, _find_int32_index_node, _create_int32_index_node, string_length, p_this_node)
        }
    }
    else if ( type == BSON_TYPE_INT64 ) {
        int64_t *_p_data = (int64_t *) p_data;
        int64_t data = *_p_data;
        _INSERT_INDEX_NODE (int64_index_node_t, _find_int64_index_node, _create_int64_index_node, string_length, p_this_node)
        }
    }
    else if ( type == BSON_TYPE_DATE_TIME ) {
        int64_t *_p_data = (int64_t *) p_data;
        int64_t data = *_p_data;
        _INSERT_INDEX_NODE (date_time_index_node_t, _find_date_time_index_node, _create_date_time_index_node, string_length, p_this_node)
        }
    }
    else if ( type == BSON_TYPE_DOUBLE ) {
        float64_t *_p_data = (float64_t *) p_data;
        float64_t data = *_p_data;
        _INSERT_INDEX_NODE (float64_index_node_t, _find_float64_index_node, _create_float64_index_node, string_length, p_this_node)
        }
    }
    else if ( type == BSON_TYPE_BOOL ) {
        bool_t *_p_data = (bool_t *) p_data;
        bool_t data = *_p_data;
        _INSERT_INDEX_NODE (bool_index_node_t, _find_bool_index_node, _create_bool_index_node, string_length, p_this_node)
        }
    }
    else {
        return NULL;
    }
    return p_this_node;
}

#define _SET_INDEX_NODE_INDEX(INDEX_NODE_T) \
    chain_head = (INDEX_NODE_T *) chain_head; \
    INDEX_NODE_T *node; \
    for (node = chain_head, node_cnt = 0; node != NULL; node = node->next, node_cnt++) { \
        node->idx = node_cnt; \
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

#define _PROCESS_INDEX_OR_COLUMN(TYPE, ITER, KEY, CHAIN_HEAD, MAX_STRING_LENGTH, UNI_STRING_LENGTH, P_IDX) \
    if (data_frame_info->TYPE == BSON_TYPE_UNKNOWN) { \
        data_frame_info->TYPE = bson_iter_type(&ITER); \
    } \
    if (data_frame_info->TYPE == BSON_TYPE_UTF8) { \
        string_data = bson_iter_utf8 (&ITER, &length); \
        if (debug) \
            printf ("%s : %s [BSON_TYPE_UTF8 %d]\n", data_frame_info->KEY, string_data, BSON_TYPE_UTF8); \
        _SET_INDEX_CHAIN_HEAD_TYPE (string_index_node_t, CHAIN_HEAD) \
        string_index_node_t * __p_this_node = (string_index_node_t *) insert_index_node (BSON_TYPE_UTF8, string_data, &CHAIN_HEAD, length, &UNI_STRING_LENGTH); \
        P_IDX = &(__p_this_node->idx); \
        if (UNI_STRING_LENGTH > MAX_STRING_LENGTH) { \
            MAX_STRING_LENGTH = UNI_STRING_LENGTH; \
        } \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_INT32) { \
        int32_data = bson_iter_int32 (&ITER); \
        if (debug) \
            printf ("%s : %d [BSON_TYPE_INT32 %d]\n", data_frame_info->KEY, int32_data, BSON_TYPE_INT32); \
        _SET_INDEX_CHAIN_HEAD_TYPE (int32_index_node_t, CHAIN_HEAD) \
        int32_index_node_t * __p_this_node = (int32_index_node_t *) insert_index_node (BSON_TYPE_INT32, &int32_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
        P_IDX = &(__p_this_node->idx); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_INT64) { \
        int64_data = bson_iter_int64 (&ITER); \
        if (debug) \
            printf ("%s : %ld [BSON_TYPE_INT64 %d]\n", data_frame_info->KEY, int64_data, BSON_TYPE_INT64); \
        _SET_INDEX_CHAIN_HEAD_TYPE (int64_index_node_t, CHAIN_HEAD) \
        int64_index_node_t * __p_this_node = (int64_index_node_t *) insert_index_node (BSON_TYPE_INT64, &int64_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
        P_IDX = &(__p_this_node->idx); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_DATE_TIME) { \
        int64_data = bson_iter_date_time (&ITER); \
        if (debug) \
            printf ("%s : %ld [BSON_TYPE_DATE_TIME %d]\n", data_frame_info->KEY, int64_data, BSON_TYPE_DATE_TIME); \
        _SET_INDEX_CHAIN_HEAD_TYPE (int64_index_node_t, CHAIN_HEAD) \
        date_time_index_node_t * __p_this_node = (date_time_index_node_t *) insert_index_node (BSON_TYPE_DATE_TIME, &int64_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
        P_IDX = &(__p_this_node->idx); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_DOUBLE) { \
        float64_data = bson_iter_double (&ITER); \
        if (debug) \
            printf ("%s : %lf [BSON_TYPE_DOUBLE %d]\n", data_frame_info->KEY, float64_data, BSON_TYPE_DOUBLE); \
        _SET_INDEX_CHAIN_HEAD_TYPE (float64_index_node_t, CHAIN_HEAD) \
        float64_index_node_t * __p_this_node = (float64_index_node_t *) insert_index_node (BSON_TYPE_DOUBLE, &float64_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
        P_IDX = &(__p_this_node->idx); \
    } \
    else if (data_frame_info->TYPE == BSON_TYPE_BOOL) { \
        bool_data = bson_iter_bool (&ITER); \
        if (debug) \
            printf ("%s : %d [BSON_TYPE_BOOL %d]\n", data_frame_info->KEY, bool_data, BSON_TYPE_BOOL); \
        _SET_INDEX_CHAIN_HEAD_TYPE (bool_index_node_t, CHAIN_HEAD) \
        bool_index_node_t * __p_this_node = (bool_index_node_t *) insert_index_node (BSON_TYPE_BOOL, &bool_data, &CHAIN_HEAD, 0, &UNI_STRING_LENGTH); \
        P_IDX = &(__p_this_node->idx); \
    } \
    else { \
        P_IDX = NULL; \
    }
