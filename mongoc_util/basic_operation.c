#ifndef BASIC_OPERATION_C
#define BASIC_OPERATION_C
#include "cymongo_type.h"
#include "error_code.h"
#include <bcon.h>
#include <string.h>


#define _CHAR_STRING_TO_UNI_CHAR_STRING(CHAR_STRING, UNI_CHAR_STRING, UNI_STRING_LENGTH) \
    if (CHAR_STRING == NULL) { \
        UNI_CHAR_STRING = (bson_unichar_t *) malloc (sizeof (bson_unichar_t)); \
        UNI_STRING_LENGTH = 0; \
    } \
    else { \
        bson_unichar_t __uni_char; \
        UNI_CHAR_STRING = (bson_unichar_t *) malloc (sizeof (bson_unichar_t) * strlen(CHAR_STRING)); \
        const char *__char_string_iter = CHAR_STRING; \
        uint64_t __uni_string_length; \
        for (__uni_string_length = 0; *__char_string_iter; __char_string_iter = bson_utf8_next_char (__char_string_iter), __uni_string_length++) { \
            __uni_char = bson_utf8_get_char (__char_string_iter); \
            UNI_CHAR_STRING[__uni_string_length] = __uni_char; \
        } \
        UNI_STRING_LENGTH = __uni_string_length; \
    }

#define _CREATE_CHAR_STRING_DATA(P_NODE, DATA) \
    P_NODE->data = (char *) malloc (sizeof (char) * (strlen(DATA) + 1)); \
    strcpy (P_NODE->data, DATA); \
    _CHAR_STRING_TO_UNI_CHAR_STRING (DATA, P_NODE->uni_string.string, P_NODE->uni_string.length)

#define _CREATE_UNI_CHAR_STRING_DATA(P_NODE, CHAR_STRING) \
    _CHAR_STRING_TO_UNI_CHAR_STRING (CHAR_STRING, P_NODE->data.string, P_NODE->data.length)

#define _CREATE_NUMBER_DATA(P_NODE, DATA) \
    P_NODE->data = DATA;

#define _TRANSFER_DATE_TIME_FORMAT(DATE_TIME, NEW_DATE_TIME) \
    millisecond = DATE_TIME % 1000; \
    time = DATE_TIME / 1000; \
    time += tz_offset_second; \
    struct tm *p_date_time = gmtime(&time); \
    year = p_date_time->tm_year + 1900; \
    month = p_date_time->tm_mon + 1; \
    day = p_date_time->tm_mday; \
    hour = p_date_time->tm_hour; \
    minute = p_date_time->tm_min; \
    second = p_date_time->tm_sec; \
    NEW_DATE_TIME = year * 10000000000000 + month * 100000000000 + day * 1000000000 + hour * 10000000 + minute * 100000 + second * 1000 + millisecond;

#define _DEFINE_TEMPORARY_VARIABLE \
    const char *string_data; \
    int32_t int32_data; \
    int64_t int64_data; \
    bool_t is_int64; \
    date_time_t date_time_data; \
    date_time_t year, month, day, hour, minute, second, millisecond, formatted_date_time; \
    time_t time; \
    struct tm *p_date_time; \
    float64_t float64_data; \
    bool_t bool_data; \
    uint32_t length; \
    uint64_t uni_string_length; \
    uint64_t *p_index_idx, *p_column_idx;

int string_index_sort (string_index_t *a, string_index_t *b) {
    return strcmp(a->key, b->key);
}

int int32_index_sort (int32_index_t *a, int32_index_t *b) {
    if (a->data < b->data) return (int) -1;
    if (a->data == b->data) return (int) 0;
    return (int) 1;
}

int int64_index_sort (int64_index_t *a, int64_index_t *b) {
    if (a->data < b->data) return (int) -1;
    if (a->data == b->data) return (int) 0;
    return (int) 1;
}

int date_time_index_sort (date_time_index_t *a, date_time_index_t *b) {
    if (a->data < b->data) return (int) -1;
    if (a->data == b->data) return (int) 0;
    return (int) 1;
}

int float64_index_sort (float64_index_t *a, float64_index_t *b) {
    if (a->data < b->data) return (int) -1;
    if (a->data == b->data) return (int) 0;
    return (int) 1;
}

int bool_index_sort (bool_index_t *a, bool_index_t *b) {
    if (a->data < b->data) return (int) -1;
    if (a->data == b->data) return (int) 0;
    return (int) 1;
}

#endif
