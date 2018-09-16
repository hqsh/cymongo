#ifndef CYMONGO_TYPE_H
#define CYMONGO_TYPE_H

#include <mongoc.h>
#include "../uthash/uthash.h"

#define BSON_TYPE_UNKNOWN -1

// basic data types
typedef int64_t date_time_t;
typedef double float64_t;
typedef bool bool_t;
typedef struct {
    bson_unichar_t *string;
    uint64_t length;
} string_t;

// ------------------------------ [start] data structure from mongo [start] ------------------------------
// the data and information of index or columns of DataFrame
typedef struct {
    const char *key;       // char string, hash key, for faster search in hash table
    UT_hash_handle hh;
    string_t data;         // uni string, for DataFrame string index
    uint64_t idx;
} string_index_t;

typedef struct {
    int32_t data;          // hash key
    UT_hash_handle hh;
    uint64_t idx;
} int32_index_t;

typedef struct {
    int64_t data;          // hash key
    UT_hash_handle hh;
    uint64_t idx;
} int64_index_t;

typedef struct {
    date_time_t data;      // hash key
    UT_hash_handle hh;
    uint64_t idx;
} date_time_index_t;

typedef struct {
    float64_t data;        // hash key
    UT_hash_handle hh;
    uint64_t idx;
} float64_index_t;

typedef struct {
    bool_t data;           // hash key
    UT_hash_handle hh;
    uint64_t idx;
} bool_index_t;

// the data and information of values of DataFrame
typedef struct _string_value_node_t {
    string_t data;
    struct _string_value_node_t *next;
    uint64_t *p_index_idx;
    uint64_t *p_column_idx;
} string_value_node_t;

typedef struct _int32_value_node_t {
    int32_t data;
    struct _int32_value_node_t *next;
    uint64_t *p_index_idx;
    uint64_t *p_column_idx;
} int32_value_node_t;

typedef struct _int64_value_node_t {
    int64_t data;
    struct _int64_value_node_t *next;
    uint64_t *p_index_idx;
    uint64_t *p_column_idx;
} int64_value_node_t;

typedef struct _date_time_value_node_t {
    date_time_t data;
    struct _date_time_value_node_t *next;
    uint64_t *p_index_idx;
    uint64_t *p_column_idx;
} date_time_value_node_t;

typedef struct _float64_value_node_t {
    float64_t data;
    struct _float64_value_node_t *next;
    uint64_t *p_index_idx;
    uint64_t *p_column_idx;
} float64_value_node_t;

typedef struct _bool_value_node_t {
    bool_t data;
    struct _bool_value_node_t *next;
    uint64_t *p_index_idx;
    uint64_t *p_column_idx;
} bool_value_node_t;

// the data from mongo
typedef struct {
    // index
    uint64_t string_index_max_length;     // max length of index strings if index is string type
    string_index_t *string_index_head;
    int32_index_t *int32_index_head;
    int64_index_t *int64_index_head;
    date_time_index_t *date_time_index_head;
    float64_index_t *float64_index_head;
    bool_index_t *bool_index_head;
    // column
    uint64_t string_column_max_length;    // max length of column strings if column is string type
    string_index_t *string_column_head;
    int32_index_t *int32_column_head;
    int64_index_t *int64_column_head;
    date_time_index_t *date_time_column_head;
    float64_index_t *float64_column_head;
    bool_index_t *bool_column_head;
    // values
    string_value_node_t **string_value_chain_heads;
    int32_value_node_t **int32_value_chain_heads;
    int64_value_node_t **int64_value_chain_heads;
    date_time_value_node_t **date_time_value_chain_heads;
    float64_value_node_t **float64_value_chain_heads;
    bool_value_node_t **bool_value_chain_heads;
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
    bson_unichar_t *string_index_array;
    int32_t *int32_index_array;
    int64_t *int64_index_array;
    int64_t *date_time_index_array;
    float64_t *float64_index_array;
    bool_t *bool_index_array;
    // column
    uint64_t string_column_max_length;
    bson_unichar_t *string_column_array;
    int32_t *int32_column_array;
    int64_t *int64_column_array;
    int64_t *date_time_column_array;
    float64_t *float64_column_array;
    bool_t *bool_column_array;
    // value
    uint64_t *string_value_max_lengths;
    bson_unichar_t **string_value_arrays;
    int32_t **int32_value_arrays;
    int64_t **int64_value_arrays;
    int64_t **date_time_value_arrays;
    float64_t **float64_value_arrays;
    bool_t **bool_value_arrays;
} data_frame_data_t;
// -------------------------------- [end] data structure to python [end] --------------------------------


// ------------------------------- [end] data structure for table [start] -------------------------------
typedef struct {
    char **column_names;
    int *column_types;
    unsigned int column_cnt;
} table_info_t;

typedef struct _string_node_t {
    uint64_t idx;
    string_t data;
    struct _string_node_t *next;
} string_node_t;

typedef struct _int32_node_t {
    uint64_t idx;
    int32_t data;
    struct _int32_node_t *next;
} int32_node_t;

typedef struct _int64_node_t {
    uint64_t idx;
    int64_t data;
    struct _int64_node_t *next;
} int64_node_t;

typedef struct _date_time_node_t {
    uint64_t idx;
    int64_t data;
    struct _date_time_node_t *next;
} date_time_node_t;

typedef struct _float64_node_t {
    uint64_t idx;
    float64_t data;
    struct _float64_node_t *next;
} float64_node_t;

typedef struct _bool_node_t {
    uint64_t idx;
    bool_t data;
    struct _bool_node_t *next;
} bool_node_t;

typedef struct {
    string_node_t **string_node_chain_heads;
    int32_node_t **int32_node_chain_heads;
    int64_node_t **int64_node_chain_heads;
    date_time_node_t **date_time_node_chain_heads;
    float64_node_t **float64_node_chain_heads;
    bool_node_t **bool_node_chain_heads;
} node_chain_heads_t;

typedef struct {
    uint64_t row_cnt;
    uint64_t *string_column_max_lengths;
    bson_unichar_t **string_columns;
    int32_t **int32_columns;
    int64_t **int64_columns;
    int64_t **date_time_columns;
    float64_t **float64_columns;
    bool_t **bool_columns;
} table_t;
// -------------------------------- [end] data structure for table [end] --------------------------------

#endif
