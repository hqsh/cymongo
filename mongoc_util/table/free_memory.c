void free_table_memory (table_t * p_table, unsigned int column_cnt)
{
    if (column_cnt > 0) {
        unsigned int column_idx;
        p_table->string_column_max_lengths = NULL;
        for (column_idx = 0; column_idx < column_cnt; column_idx++) {
            if (p_table->string_columns[column_idx] != NULL) {
                free (p_table->string_columns[column_idx]);
            }
            else if (p_table->int32_columns[column_idx] != NULL) {
                free (p_table->int32_columns[column_idx]);
            }
            else if (p_table->int64_columns[column_idx] != NULL) {
                free (p_table->int64_columns[column_idx]);
            }
            else if (p_table->date_time_columns[column_idx] != NULL) {
                free (p_table->date_time_columns[column_idx]);
            }
            else if (p_table->float64_columns[column_idx] != NULL) {
                free (p_table->float64_columns[column_idx]);
            }
            else if (p_table->bool_columns[column_idx] != NULL) {
                free (p_table->bool_columns[column_idx]);
            }
        }
        free (p_table->string_columns);
        free (p_table->int32_columns);
        free (p_table->int64_columns);
        free (p_table->date_time_columns);
        free (p_table->float64_columns);
        free (p_table->bool_columns);
        free (p_table);
    }
}
