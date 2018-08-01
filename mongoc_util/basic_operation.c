#ifndef BASIC_OPERATION_C
#define BASIC_OPERATION_C

#include "cymongo_type.h"
#include "error_code.h"
#include <string.h>

#define _CHAR_STRING_TO_UNI_CHAR_STRING(CHAR_STRING, UNI_CHAR_STRING, STRING_LENGTH, UNI_STRING_LENGTH) \
    bson_unichar_t __uni_char; \
    UNI_CHAR_STRING = (bson_unichar_t *) malloc (sizeof (bson_unichar_t) * STRING_LENGTH); \
    char *__char_string_iter = CHAR_STRING; \
    uint64_t __uni_string_length; \
    for (__uni_string_length = 0; *__char_string_iter; __char_string_iter = bson_utf8_next_char (__char_string_iter), __uni_string_length++) { \
        __uni_char = bson_utf8_get_char (__char_string_iter); \
        UNI_CHAR_STRING[__uni_string_length] = __uni_char; \
    } \
    UNI_STRING_LENGTH = __uni_string_length;

#define _CREATE_CHAR_STRING_DATA(P_NODE, DATA) \
    P_NODE->data = (char *) malloc (sizeof (char) * (strlen(DATA) + 1)); \
    strcpy (P_NODE->data, DATA); \
    _CHAR_STRING_TO_UNI_CHAR_STRING(DATA, P_NODE->uni_string.string, strlen(DATA), P_NODE->uni_string.length)

#define _CREATE_UNI_CHAR_STRING_DATA(P_NODE, CHAR_STRING) \
    _CHAR_STRING_TO_UNI_CHAR_STRING(CHAR_STRING, P_NODE->data.string, strlen(CHAR_STRING), P_NODE->data.length)

#define _CREATE_NUMBER_DATA(P_NODE, DATA) \
    P_NODE->data = DATA;

#endif
