#include <mongoc.h>

#define BSON_TYPE_UNKNOWN -1

// basic data types
typedef double float64_t;
typedef bool bool_t;
typedef struct {
    bson_unichar_t *string;
    uint64_t length;
} string_t;

// ------------------------------ [start] data structure from mongo [start] ------------------------------
// the data and information of index or columns of DataFrame
typedef struct _string_index_node_t {
    char *data;            // char string, for faster strcpy when find node in chain
    string_t uni_string;   // uni string, for DataFrame string index
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
    string_t data;
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

// the data from mongo
typedef struct {
    void *index_chain_head;
    void *column_chain_head;
    uint64_t string_index_max_length;     // max length of index strings if index is string type
    uint64_t string_column_max_length;    // max length of column strings if column is string type
    char **string_value_keys;
    char **int32_value_keys;
    char **int64_value_keys;
    char **date_time_value_keys;
    char **float64_value_keys;
    char **bool_value_keys;
    unsigned int *string_value_key_index;      // the index of the data_frame_info_t.value_keys ( 0, 1, 2, ... , length(value_cnt)-1 )
    unsigned int *int32_value_key_index;
    unsigned int *int64_value_key_index;
    unsigned int *date_time_value_key_index;
    unsigned int *float64_value_key_index;
    unsigned int *bool_value_key_index;
    unsigned int string_value_cnt;
    unsigned int int32_value_cnt;
    unsigned int int64_value_cnt;
    unsigned int date_time_value_cnt;
    unsigned int float64_value_cnt;
    unsigned int bool_value_cnt;
    string_value_node_t *string_value_chain_head;
    int32_value_node_t *int32_value_chain_head;
    int64_value_node_t *int64_value_chain_head;
    date_time_value_node_t *date_time_value_chain_head;
    float64_value_node_t *float64_value_chain_head;
    bool_value_node_t *bool_value_chain_head;
} mongo_data_t;
// -------------------------------- [end] data structure from mongo [end] -------------------------------

// ------------------------------ [start] data structure to python [start] ------------------------------
typedef struct {
    char *index_key;
    int index_type;
    char *column_key;
    int column_type;
    char **value_keys;
    int *value_types;
    unsigned int value_cnt;
} data_frame_info_t;

// the data and information of values of DataFrame
typedef struct _data_frame_string_value_t {
    unsigned int value_key_index;    // the index of the data_frame_info_t.value_keys ( 0, 1, 2, ... , length(value_keys)-1 )
    string_t *string_value_array;
    struct _data_frame_string_value_t *next;
} data_frame_string_value_t;

typedef struct {
    // shape
    uint64_t row_cnt;
    uint64_t col_cnt;
    // index
    uint64_t string_index_max_length;
    string_t *string_index_array;
    int32_t *int32_index_array;
    int64_t *int64_index_array;
    int64_t *date_time_index_array;
    float64_t *float64_index_array;
    bool_t *bool_index_array;
    // column
    uint64_t string_column_max_length;
    string_t *string_column_array;
    int32_t *int32_column_array;
    int64_t *int64_column_array;
    int64_t *date_time_column_array;
    float64_t *float64_column_array;
    bool_t *bool_column_array;
//    value
//    unsigned int string_value_cnt;
//    uint64_t *string_value_max_length;
//    string_t **string_value_array;
//    int32_t **int32_value_array;
//    int64_t *int64_column_array;
//    int64_t *date_time_column_array;
//    float64_t *float64_column_array;
//    bool_t *bool_column_array;
} data_frame_data_t;
// -------------------------------- [end] data structure to python [end] --------------------------------
