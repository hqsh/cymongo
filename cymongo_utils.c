#include "cymongo_types.h"
#include "error_code.h"
#include <string.h>

#define _CREATE_STR_DATA(P_NODE) \
    P_NODE->data = (char *) malloc (sizeof (char) * (strlen (data) + 1)); \
    strcpy (P_NODE->data, data);

#define _CREATE_NUM_DATA(P_NODE) \
    P_NODE->data = data;

#define _CREATE_INDEX_NODE(INDEX_NODE_T, CREATE_DATA) \
    INDEX_NODE_T *p_node = (INDEX_NODE_T *) malloc (sizeof (INDEX_NODE_T) ); \
    CREATE_DATA(p_node) \
    p_node->next = next; \
    return p_node;

string_index_node_t * _create_string_index_node (string_t data, string_index_node_t *next) {
    _CREATE_INDEX_NODE (string_index_node_t, _CREATE_STR_DATA)
}

int32_index_node_t * _create_int32_index_node (int32_t data, int32_index_node_t *next) {
    _CREATE_INDEX_NODE (int32_index_node_t, _CREATE_NUM_DATA)
}

int64_index_node_t * _create_int64_index_node (int64_t data, int64_index_node_t *next) {
    _CREATE_INDEX_NODE (int64_index_node_t, _CREATE_NUM_DATA)
}

date_time_index_node_t * _create_date_time_index_node (int64_t data, date_time_index_node_t *next) {
    _CREATE_INDEX_NODE (date_time_index_node_t, _CREATE_NUM_DATA)
}

float64_index_node_t * _create_float64_index_node (float64_t data, float64_index_node_t *next) {
    _CREATE_INDEX_NODE (float64_index_node_t, _CREATE_NUM_DATA)
}

bool_index_node_t * _create_bool_index_node (bool_t data, bool_index_node_t *next) {
    _CREATE_INDEX_NODE (bool_index_node_t, _CREATE_NUM_DATA)
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

bool _find_string_index_node (string_t data, string_index_node_t *chain_head, string_index_node_t **prev_node) {
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

#define _INSERT_INDEX_NODE(INDEX_NODE_T, FIND_INDEX_NODE, CREATE_INDEX_NODE) \
    chain_head = (INDEX_NODE_T **) chain_head; \
    INDEX_NODE_T *prev_node, *next_node; \
    if (! FIND_INDEX_NODE (data, *chain_head, &prev_node)) { \
        if (prev_node == NULL) { \
            next_node = *chain_head; \
        } \
        else { \
            next_node = prev_node->next; \
        } \
        INDEX_NODE_T *node = CREATE_INDEX_NODE (data, next_node); \
        if (prev_node == NULL) { \
            *chain_head = node; \
        } \
        else { \
            prev_node->next = node; \
        } \
    }

int insert_index_node (bson_type_t type, void *p_data, void **chain_head) {
    if ( type == BSON_TYPE_UTF8 ) {
        char *data = (char *) p_data;
        _INSERT_INDEX_NODE (string_index_node_t, _find_string_index_node, _create_string_index_node)
    }
    else if ( type == BSON_TYPE_INT32 ) {
        int32_t *_p_data = (int32_t *) p_data;
        int32_t data = *_p_data;
        _INSERT_INDEX_NODE (int32_index_node_t, _find_int32_index_node, _create_int32_index_node)
    }
    else if ( type == BSON_TYPE_INT64 ) {
        int64_t *_p_data = (int64_t *) p_data;
        int64_t data = *_p_data;
        _INSERT_INDEX_NODE (int64_index_node_t, _find_int64_index_node, _create_int64_index_node)
    }
    else if ( type == BSON_TYPE_DATE_TIME ) {
        int64_t *_p_data = (int64_t *) p_data;
        int64_t data = *_p_data;
        _INSERT_INDEX_NODE (date_time_index_node_t, _find_date_time_index_node, _create_date_time_index_node)
    }
    else if ( type == BSON_TYPE_DOUBLE ) {
        float64_t *_p_data = (float64_t *) p_data;
        float64_t data = *_p_data;
        _INSERT_INDEX_NODE (float64_index_node_t, _find_float64_index_node, _create_float64_index_node)
    }
    else if ( type == BSON_TYPE_BOOL ) {
        bool_t *_p_data = (bool_t *) p_data;
        bool_t data = *_p_data;
        _INSERT_INDEX_NODE (bool_index_node_t, _find_bool_index_node, _create_bool_index_node)
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
