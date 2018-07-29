#include <mongoc.h>

#define BSON_TYPE_UNKNOWN -1

typedef char * string_t;
typedef double float64_t;
typedef bool bool_t;

// the data and information of index or columns of DataFrame
typedef struct _string_index_node_t {
    char *data;
    struct _string_index_node_t *next;
    uint64_t index;
} string_index_node_t;

typedef struct _int32_index_node_t {
    int32_t data;
    struct _int32_index_node_t *next;
    uint64_t index;
} int32_index_node_t;

typedef struct _int64_index_node_t {
    int64_t data;
    struct _int64_index_node_t *next;
    uint64_t index;
} int64_index_node_t;

typedef struct _date_time_index_node_t {
    int64_t data;
    struct _date_time_index_node_t *next;
    uint64_t index;
} date_time_index_node_t;

typedef struct _float64_index_node_t {
    float64_t data;
    struct _float64_index_node_t *next;
    uint64_t index;
} float64_index_node_t;

typedef struct _bool_index_node_t {
    bool_t data;
    struct _bool_index_node_t *next;
    uint64_t index;
} bool_index_node_t;

// the data and information of values of DataFrame
typedef struct _string_value_node_t {
    char *data;
    struct _string_value_node_t *next;
    uint64_t *p_index;
    uint64_t *p_column;
} string_value_node_t;

typedef struct _int32_value_node_t {
    int32_t data;
    struct _int32_value_node_t *next;
    uint64_t *p_index;
    uint64_t *p_column;
} int32_value_node_t;

typedef struct _int64_value_node_t {
    int64_t data;
    struct _int64_value_node_t *next;
    uint64_t *p_index;
    uint64_t *p_column;
} int64_value_node_t;

typedef struct _date_time_value_node_t {
    int64_t data;
    struct _date_time_value_node_t *next;
    uint64_t *p_index;
    uint64_t *p_column;
} date_time_value_node_t;

typedef struct _float64_value_node_t {
    float64_t data;
    struct _float64_value_node_t *next;
    uint64_t *p_index;
    uint64_t *p_column;
} float64_value_node_t;

typedef struct _bool_value_node_t {
    bool_t data;
    struct _bool_value_node_t *next;
    uint64_t *p_index;
    uint64_t *p_column;
} bool_value_node_t;

typedef struct {
    char *index_key;
    int index_type;
    char *column_key;
    int column_type;
    char **value_keys;
    int *value_types;
    unsigned int value_cnt;
} data_frame_info_t;

typedef struct {
    void *index_chain_head;
    void *column_chain_head;
    char **string_value_keys;
    char **int32_value_keys;
    char **int64_value_keys;
    char **date_time_value_keys;
    char **float64_value_keys;
    char **bool_value_keys;
    int *string_value_types;
    int *int32_value_types;
    int *int64_value_types;
    int *date_time_value_types;
    int *float64_value_types;
    int *bool_value_types;
    unsigned int string_value_cnt;
    unsigned int int32_value_cnt;
    unsigned int int64_value_cnt;
    unsigned int date_time_value_cnt;
    unsigned int float64_value_cnt;
    unsigned int bool_value_cnt;
} mongo_data_t;

typedef struct {
    uint64_t row_cnt;
    uint64_t col_cnt;
    string_t *string_index_array;
    int32_t *int32_index_array;
    int64_t *int64_index_array;
    int64_t *date_time_index_array;
    float64_t *float64_index_array;
    bool_t *bool_index_array;
    string_t *string_column_array;
    int32_t *int32_column_array;
    int64_t *int64_column_array;
    int64_t *date_time_column_array;
    float64_t *float64_column_array;
    bool_t *bool_column_array;
} data_frame_data_t;
